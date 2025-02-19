import json
import os
import time
import concurrent.futures
from datetime import datetime
from typing import Dict
from alive_progress import alive_bar
from tqdm import tqdm
from app import db
from utilities.config import PATH_CONFIG
from utilities.logging_utils import setup_logger
from utilities.utility_functions import (
    format_sql_response,
    convert_enums_to_string,
)
from utilities.constants.LLM_enums import LLMType, ModelType
from utilities.constants.prompts_enums import FormatType, PromptType
from utilities.constants.script_constants import (
    DatasetEvalStatus,
    GOOGLE_RESOURCE_EXHAUSTED_EXCEPTION_STR,
)
from utilities.prompts.prompt_factory import PromptFactory
from services.client_factory import ClientFactory
from utilities.sql_improvement import improve_sql_query_chat, improve_sql_query
from utilities.candidate_selection import xiyan_basic_llm_selector
from utilities.vectorize import make_samples_collection

logger = setup_logger(__name__)
    
def initialize_metadata(
    metadata_file_path: str,
    config
) -> Dict:
    """Initializes or loads metadata from the specified file."""

    if not metadata_file_path:
        time_stamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
        metadata_file_path = f"{PATH_CONFIG.batch_job_metadata_dir()}/{time_stamp}.json"

        os.makedirs(os.path.dirname(metadata_file_path), exist_ok=True)
        metadata = {
            "batch_info": {
                "overall_status": DatasetEvalStatus.IN_PROGRESS.value,
            },
            "databases": {},
        }

        for idx, cfg in enumerate(config):
            metadata['batch_info'][f'config_{idx+1}']=cfg
        with open(metadata_file_path, "w") as file:
            json.dump(metadata, file, indent=4)
    else:
        with open(metadata_file_path, "r") as file:
            metadata = json.load(file)

    return metadata, metadata_file_path

def prompt_llm(prompt, improve, client, improv_client, max_improve_sql_attempts, database, question, shots):
    sql = ""
    while sql == "":
        try:
            sql = format_sql_response(client.execute_prompt(prompt=prompt))
        except Exception as e:
            if GOOGLE_RESOURCE_EXHAUSTED_EXCEPTION_STR in str(e):
                logger.warning("Quota exhausted. Retrying in 5 seconds...")
                time.sleep(5)
            else:
                logger.error(f"Unhandled exception: {e}")

    if improve:
        sql = improve_sql_query_chat(
            sql,
            max_improve_sql_attempts,
            database,
            improv_client,
            question,
            shots,
        )
    return sql

def process_config(config, item, database):

    client = ClientFactory.get_client(config['model'][0], config['model'][1], config['temperature'], config['max_tokens'])
    if config['improve_sql']:
        if config['model'] ==  config['improve_client']:
            improv_client = client
        else:
            improv_client = ClientFactory.get_client(config['improve_client'][0], config['improve_client'][1], config['temperature'], config['max_tokens'])
    else:
        improv_client = None
    
    prompt = PromptFactory.get_prompt_class(
                prompt_type=config['prompt_config']['type'],
                target_question=item["question"],
                shots=config['prompt_config']['shots'],
                schema_format=config['prompt_config']['format_type'],
                schema = item['runtime_schema_used'] if config['prune_schema'] else None,
                evidence = item['evidence'] if config['add_evidence'] else None,
            )

    sql = prompt_llm(prompt, config['improve_sql'], client, improv_client, config['max_improve_sql_attempts'], database, item['question'], config['prompt_config']['shots'])

    return sql

def process_database(
    database: str,
    run_config,
    metadata,
    metadata_file_path,
    selector_model = None
):
    """Main processing function for a single database."""

    if database not in metadata["databases"]:
        metadata["databases"][database] = {
            "state": DatasetEvalStatus.IN_PROGRESS.value,
        }
    elif metadata["databases"][database]["state"] == DatasetEvalStatus.COMPLETED.value:
        logger.info(f"Database {database} has already been processed. Skipping...")
        return

    db.set_database(database)

    if PATH_CONFIG.sample_dataset_type == PATH_CONFIG.dataset_type and any(config['prompt_config']['shots'] > 0 for config in run_config):
        make_samples_collection()

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

    with open(PATH_CONFIG.processed_test_path(database_name=database), "r") as f:
        test_data = json.load(f)

    if len(run_config)>1:    
        selector_client = ClientFactory.get_client(selector_model['model'][0], selector_model['model'][1], selector_model['temperature'], selector_model['max_tokens'])

    with alive_bar(len(test_data), bar = 'fish', spinner = 'fish2', title=f'Processing Questions for {database}') as bar: 
        for item in test_data:

            if str(item["question_id"]) in processed_ids:
                logger.info(f"Skipping already processed query {item['question_id']}")
                bar()
                continue

            MAX_THREADS = 6
            all_results = []

            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
                future_to_config = {executor.submit(process_config, config, item, database): config for config in run_config}
                for future in concurrent.futures.as_completed(future_to_config):
                    all_results.append(future.result())
            
            if len(all_results) > 1:
                sql = xiyan_basic_llm_selector(all_results, item['question'], selector_client, database, item['schema_used'], item['evidence'])
            else:
                sql = all_results[0]
            

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
            
            bar()

    # Update the status if all test data has been processed
    if len(predicted_scripts) == len(test_data):
        metadata["databases"][database] = {
            "state": DatasetEvalStatus.COMPLETED.value,
        }

    with open(metadata_file_path, "w") as file:
        json.dump(metadata, file, indent=4)


def process_all_databases(
  dataset_dir,
  metadata_file_path,
  run_config,
  selector_model = None
):
    """Process all databases in the specified directory."""

    if PATH_CONFIG.sample_dataset_type != PATH_CONFIG.dataset_type and any(config['prompt_config']['shots'] > 0 for config in run_config):
        make_samples_collection()

    metadata, metadata_file_path = initialize_metadata(
        metadata_file_path, convert_enums_to_string(run_config)
    )

    databases = [d for d in os.listdir(dataset_dir) if os.path.isdir(os.path.join(dataset_dir, d))]
    
    for database in tqdm(databases, desc="Processing all databases"):
        process_database(
            database,
            run_config,
            metadata,
            metadata_file_path,
            selector_model = selector_model,
        )

    if all(
        db["state"] == DatasetEvalStatus.COMPLETED.value
        for db in metadata["databases"].values()
    ):
        metadata["batch_info"]["overall_status"] = DatasetEvalStatus.COMPLETED.value
    with open(metadata_file_path, "w") as file:
        json.dump(metadata, file, indent=4)

def validate_config(config, required_keys):
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
        - To add an option to improve the prompt, set `improve_sql` to `True`.
        - To limit the number of attempts to improve the prompt, set `max_improve_sql_attempts` accordingly.
        - To test a number of variations simply add different configs in each list
        - To use pruned schema set 'prune_schema' to True
        - To use evidence in the prompts set 'add_evidence' to True

    4. Run the Script:
        - Execute the following command in the terminal `python3 -m scripts.process_dataset_sequentially`

    5. Expected Outputs:
        - Metadata File: A JSON metadata file is created or updated at the specified `metadata_file_path`.
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
    "improve_sql",
    "max_improve_sql_attempts",
    "improve_client",
    "prune_schema",
    "add_evidence"
    ]

    # Initial variables
    selector_model = {
        "model":[LLMType.GOOGLE_AI, ModelType.GOOGLEAI_GEMINI_2_0_PRO_EXP],
        "temperature": 0.2,
        "max_tokens": 8192,
    }

    config_options = [
        {
            "model": [LLMType.GOOGLE_AI, ModelType.GOOGLEAI_GEMINI_2_0_PRO_EXP],
            "temperature": 0.7,
            "max_tokens": 8192,
            "prompt_config": {"type":PromptType.SEMANTIC_FULL_INFORMATION, "shots": 5, "format_type": FormatType.M_SCHEMA},
            "improve_sql": True,
            "max_improve_sql_attempts": 5,
            "improve_client": [LLMType.GOOGLE_AI, ModelType.GOOGLEAI_GEMINI_2_0_PRO_EXP],
            "prune_schema": True,
            "add_evidence": True,
        }
    ]

    if not validate_config(config_options, keys):
        logger.error("Config Not Correctly Set")
        exit()
    
    # File Configurations
    file_name = "2024-12-24_18:10:36.json"
    metadata_file_path = None  # BATCH_JOB_METADATA_DIR + file_name

    process_all_databases(
        dataset_dir=PATH_CONFIG.dataset_dir(),
        metadata_file_path=metadata_file_path,
        run_config=config_options,
        selector_model = selector_model
    )
