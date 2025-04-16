import json
import os
import time
import concurrent.futures
from typing import Dict, List
from tqdm import tqdm
from app import db
from utilities.config import PATH_CONFIG
from utilities.logging_utils import setup_logger
from utilities.utility_functions import (
    format_sql_response,
)
from utilities.constants.LLM_enums import LLMType, ModelType
from utilities.constants.prompts_enums import FormatType, PromptType, RefinerPromptType
from utilities.constants.script_constants import (
    GOOGLE_RESOURCE_EXHAUSTED_EXCEPTION_STR,
)
from utilities.prompts.prompt_factory import PromptFactory
from services.client_factory import ClientFactory
from utilities.sql_improvement import improve_sql_query
from utilities.candidate_selection import xiyan_basic_llm_selector
from utilities.vectorize import make_samples_collection
from utilities.selection_metadata_collection import SelectionMetadata

logger = setup_logger(__name__)

# Number of Workers to work on seperate candidates
MAX_CANDIDATE_WORKERS = 6

# Number of Workers to work on seperate databases
MAX_DATABASE_WORKERS = 2

def load_json_file(file_path: str):
    with open(file_path, "r") as file:
        file_data = json.load(file)
        file.close()
    return file_data

def generate_sql(candidate: Dict, item: Dict, database: str) -> List:
    """
    Prompts the LLM to generate an SQL query and optionally improves it.
    """

    try:
        # Get the client for the candidate model
        client = ClientFactory.get_client(*candidate['model'], candidate['temperature'], candidate['max_tokens'])

        # Create the prompt for the candidate
        prompt = PromptFactory.get_prompt_class(
            prompt_type=candidate['prompt_config']['type'],
            target_question=item['question'],
            shots=candidate['prompt_config']['shots'],
            schema_format=candidate['prompt_config']['format_type'],
            schema=item['runtime_schema_used'] if candidate['prune_schema'] else None,
            evidence=item['evidence'] if candidate['add_evidence'] else None
        )

        sql = ""
        while not sql:
            try:
                # Execute the prompt and format the SQL response
                sql = format_sql_response(client.execute_prompt(prompt=prompt))
            except Exception as e:
                if GOOGLE_RESOURCE_EXHAUSTED_EXCEPTION_STR in str(e):
                    logger.warning("Quota exhausted. Retrying in 5 seconds...")
                    time.sleep(5)
                else:
                    logger.error(f"Unhandled exception: {str(e)}")
                    raise

        # Improve the SQL query if improvement configuration is provided
        if candidate.get("improve_config"):
            try:
                improve_config = candidate["improve_config"]
                improv_client = ClientFactory.get_client(*improve_config['model'], improve_config['temperature'], improve_config['max_tokens'])

                sql = improve_sql_query(
                    sql=sql,
                    max_improve_sql_attempts=improve_config['max_attempts'],
                    database_name=database,
                    client=improv_client,
                    target_question=item['question'],
                    shots=improve_config['prompt_config']['shots'],
                    schema_used=item['runtime_schema_used'] if improve_config['prune_schema'] else None,
                    evidence=item['evidence'] if improve_config['add_evidence'] else None,
                    refiner_prompt_type=improve_config['prompt_config']['type'],
                    chat_mode=improve_config['prompt_config']['chat_mode']
                )
            except Exception as e:
                logger.error(f"Error improving SQL query: {str(e)}")
                raise

        return [sql, candidate['candidate_id']]
    except Exception as e:
        logger.error(f"Error processing candidate {candidate['candidate_id']}: {str(e)}")
        raise

def process_database(
    database: str,
    run_config: List,
    selector_model = None,
    collect_data = False,
    selection_metadata: SelectionMetadata | None = None
) -> None:
    """Main processing function for a single database."""

    db.set_database(database)

    formatted_pred_path = PATH_CONFIG.formatted_predictions_path(database_name=database)
    gold_sql_path = PATH_CONFIG.test_gold_path(database_name=database)

    predicted_scripts = {}
    gold_items = []
    
    # Load intermediary results if they exist
    if os.path.exists(formatted_pred_path):
        with open(formatted_pred_path, "r") as pred_file:
            if os.path.getsize(formatted_pred_path) > 0:
                predicted_scripts = json.load(pred_file)
                if os.path.exists(gold_sql_path):
                    with open(gold_sql_path, "r") as gold_file:
                        gold_items = [line.strip() for line in gold_file.readlines()]

    # Identify already processed question IDs
    processed_ids = set(predicted_scripts.keys())

    test_data = load_json_file(PATH_CONFIG.processed_test_path(database_name=database))[:5]

    if len(run_config)>1:    
        selector_client = ClientFactory.get_client(selector_model['model'][0], selector_model['model'][1], selector_model['temperature'], selector_model['max_tokens'])

    for item in tqdm(test_data, desc=f"Processing {database}"):
        try:
            if str(item["question_id"]) in processed_ids:
                logger.info(f"Skipping already processed query {item['question_id']}")
                continue

            all_results = []
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CANDIDATE_WORKERS) as executor:
                future_to_config = {executor.submit(generate_sql, config, item, database): config for config in run_config}
                for future in concurrent.futures.as_completed(future_to_config):
                    all_results.append(future.result())

            if len(all_results) > 1:
                sql, config_id = xiyan_basic_llm_selector(all_results, item['question'], selector_client, database, item['runtime_schema_used'], item['evidence'])
            else:
                sql,config_id = all_results[0][0], all_results[0][1]
            
            if collect_data:
                selection_metadata.update_selection_metadata(
                    candidates=all_results,
                    gold_sql = item['SQL'],
                    database=database,
                    selected_config=config_id
                )

            predicted_scripts[int(item["question_id"])] = (
                f"{sql}\t----- bird -----\t{database}"
            )
            gold_items.append(f"{item['SQL']}\t{database}")

            os.makedirs(os.path.dirname(formatted_pred_path), exist_ok=True)
            with open(formatted_pred_path, "w") as file:
                json.dump(predicted_scripts, file)

            with open(gold_sql_path, "w") as file:
                for line in gold_items:
                    file.write(f"{line}\n")

            if collect_data:
                selection_metadata.save_metadata()

        except Exception as e:
            logger.error(f"Exception in {e}", exc_info=True)
            exit()

    logger.info(f"Processed {database}")

def process_all_databases(
  dataset_dir: str,
  run_config: List,
  selector_model: Dict = None,
  collect_data: bool = False,
  save_global_files: bool = True,
) -> None:
    """Process all databases in the specified directory."""

    if any(config['prompt_config']['shots'] > 0 for config in run_config):
        make_samples_collection()

    databases = [d for d in os.listdir(dataset_dir) if os.path.isdir(os.path.join(dataset_dir, d))]

    if collect_data:
        selection_metadata = SelectionMetadata(
            run_config=run_config, 
            database=databases[0],
        )
    else:
        selection_metadata = None

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_DATABASE_WORKERS) as executor:
        futures = {
            executor.submit(
                process_database,
                database=database,
                run_config=run_config,
                selector_model=selector_model,
                collect_data=collect_data,
                selection_metadata=selection_metadata
            ): database
            for database in databases
        }
    for future in concurrent.futures.as_completed(futures):
        try:
            future.result() 
        except Exception as e:
            db_name = futures[future]
            print(f"Error processing {db_name}: {e}") 

    if save_global_files:

        #Saving Global File
        global_predictions = {}   
        for database in databases:
            database_predictions =  load_json_file(PATH_CONFIG.formatted_predictions_path(database_name=database))
                
            for prediction in database_predictions:
                global_predictions[prediction] = database_predictions[prediction]
        
        global_predictions = dict(sorted(global_predictions.items(), key=lambda item: int(item[0])))
        with open(PATH_CONFIG.formatted_predictions_path(global_file=True), 'w') as file:
            json.dump(global_predictions, file)
            file.close()

def validate_config(config: List, required_keys: List) -> bool:
    """
    Check if all dictionaries in the list contain the required keys.

    :param lst: List of dictionaries to validate
    :param required_keys: Set of required keys
    :return: True if all dictionaries have the required keys, False otherwise
    """
    required_keys_set = set(required_keys)
    return all(required_keys_set.issubset(d.keys()) for d in config)

if __name__ == "__main__":
    """
    To run this script:

    1. Ensure you have set the correct `PATH_CONFIG.dataset_type` and `PATH_CONFIG.sample_dataset_type` in `utilities.config`:
       - Set `PATH_CONFIG.dataset_type` to DatasetType.BIRD_TRAIN for training data.
       - Set `PATH_CONFIG.dataset_type` to DatasetType.BIRD_DEV for development data.
       - Set `PATH_CONFIG.sample_dataset_type` to DatasetType.BIRD_DEV or DatasetType.BIRD_TRAIN.

    2. Metadata File Configuration:
        - If continuing previous processing, provide the timestamp of the existing metadata file.
        - If starting a new process, set the `metadata_file_path` to `None`.

    3. Adjust Input Variables:
        - Ensure all input variables, such as file paths, LLM configurations, and prompt configurations, are correctly defined.
        - To enable improvement update each configs 'improve' key with the improver model, prompt_type (xiyan or basic), shots and max attempts to improve
        - To disable improvement set 'improve' to False or None. 
        - To test a number of variations simply add different configs in each list
        - To use pruned schema set 'prune_schema' to True
        - To use evidence in the prompts set 'add_evidence' to True
        - set collect_selection_data to true to log data about candidate selection and refiner module
        - set save_global_predictions to true to save a global file in the dataset root directory

    4. Run the Script:
        - Execute the following command in the terminal `python3 -m scripts.process_dataset_sequentially`

    5. Expected Outputs:
        - Formatted Predictions: Predictions for each processed database are saved.
        - Gold SQL Scripts: Gold standard SQL scripts are saved alongside predictions.

    6. Additional Notes:
        - Processing includes formatting predictions, executing LLM prompts, and saving results. The script pauses for a short delay between processing to manage API rate limits.
    """

    keys = [
    "model",
    "temperature",
    "max_tokens",
    "prompt_config",
    "prune_schema",
    "add_evidence",
    "improve",
    'candidate_id'
    ]

    # Initial variables
    selector_model = {
        "model":[LLMType.GOOGLE_AI, ModelType.GOOGLEAI_GEMINI_2_0_FLASH],
        "temperature": 0.2,
        "max_tokens": 8192,
    }

    config_options = [
        {
            "candidate_id":1,
            "model": [LLMType.GOOGLE_AI, ModelType.GOOGLEAI_GEMINI_2_0_FLASH],
            "temperature": 0.7,
            "max_tokens": 8192,
            "prompt_config": {
                "type": PromptType.SEMANTIC_FULL_INFORMATION,
                "shots": 5,
                "format_type": FormatType.M_SCHEMA,
            },
            "improve": {  
                "client": [LLMType.GOOGLE_AI, ModelType.GOOGLEAI_GEMINI_2_0_FLASH],
                "prompt": RefinerPromptType.XIYAN,
                "max_attempts": 5,
                'shots': 5
            },
            "prune_schema": True,
            "add_evidence": True,
        },
        {
            "candidate_id":2,
            "model": [LLMType.GOOGLE_AI, ModelType.GOOGLEAI_GEMINI_2_0_FLASH],
            "temperature": 0.7,
            "max_tokens": 8192,
            "prompt_config": {
                "type": PromptType.SEMANTIC_FULL_INFORMATION,
                "shots": 5,
                "format_type": FormatType.M_SCHEMA,
            },
            "improve": None,
            "prune_schema": True,
            "add_evidence": True,
        }
    ]

    if not validate_config(config_options, keys):
        logger.error("Config Not Correctly Set")
        exit()

    collect_selection_data = True
    save_global_predictions = True
   
    process_all_databases(
        dataset_dir=PATH_CONFIG.dataset_dir(),
        run_config=config_options,
        selector_model = selector_model,
        collect_data = (collect_selection_data and (len(config_options) > 1)),
        save_global_files = save_global_predictions
    )