from collections import defaultdict
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
    execute_sql_timeout
)
from utilities.constants.LLM_enums import LLMType, ModelType
from utilities.constants.prompts_enums import FormatType, PromptType, RefinerPromptType
from utilities.constants.script_constants import (
    DatasetEvalStatus,
    GOOGLE_RESOURCE_EXHAUSTED_EXCEPTION_STR,
)
from utilities.prompts.prompt_factory import PromptFactory
from services.client_factory import ClientFactory
from utilities.sql_improvement import improve_sql_query
from utilities.candidate_selection import xiyan_basic_llm_selector
from utilities.vectorize import make_samples_collection
import pandas as pd

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

def prompt_llm(prompt, client, database, question, schema_used = None, evidence = '', improve_config = None):
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

    if improve_config:
        if improve_config['client']:
            improv_client = ClientFactory.get_client(improve_config['client'][0], improve_config['client'][1], client.temperature, client.max_tokens)
        else:
            improv_client = client
            
        
        sql = improve_sql_query(
            sql=sql,
            max_improve_sql_attempts=improve_config['max_attempts'],
            database_name=database,
            client=improv_client,
            target_question=question,
            shots=improve_config['shots'],
            schema_used=schema_used,
            evidence=evidence,
            refiner_prompt_type=improve_config['prompt'],
            chat_mode=True
        )
    return sql 

def process_config(config, item, database, refiner_file=None):

    client = ClientFactory.get_client(config['model'][0], config['model'][1], config['temperature'], config['max_tokens'])

    prompt = PromptFactory.get_prompt_class(
                prompt_type=config['prompt_config']['type'],
                target_question=item["question"],
                shots=config['prompt_config']['shots'],
                schema_format=config['prompt_config']['format_type'],
                schema = item['runtime_schema_used'] if config['prune_schema'] else None,
                evidence = item['evidence'] if config['add_evidence'] else None,
            )

    sql = prompt_llm(
        prompt=prompt,
        client=client,
        database=database,
        question=item["question"],
        schema_used=item["schema_used"],
        evidence=item["evidence"],
        improve_config=config["improve"],
    )

    return [sql, config['config_id']]

def get_dict(database, file_path, columns):
    if os.path.exists(file_path):
        df = pd.read_csv(file_path, sep='\t', index_col=False)
        df_dict = df.set_index("database").to_dict(orient='index')
        if database not in list(df_dict.keys()):
            df_dict[database] = {str(i): 0 for i in columns}
    else:
        df_dict={database : {str(i): 0 for i in columns}}
    return df_dict

def save_df(data_dict, file_path):
    df = pd.DataFrame.from_dict(data_dict, orient='index')

    # Reset index and rename columns
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'database'}, inplace=True)

    # Save to CSV
    df.to_csv(file_path, sep='\t', index=False)

def process_database(
    database: str,
    run_config,
    metadata,
    metadata_file_path,
    selector_model = None,
    collect_data = False,
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

    if any(config['prompt_config']['shots'] > 0 for config in run_config):
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
    
    refiner_dict = None
    if collect_data:
        correct_gen_file = PATH_CONFIG.correct_generated_file()
        config_sel_file = PATH_CONFIG.config_selected_file()
        correct_sel_file = PATH_CONFIG.correct_selected_file()
        refiner_data_file = PATH_CONFIG.refiner_data_file()
        
        correct_gen_dict = get_dict(database, correct_gen_file, [i+1 for i in range(len(run_config))])
        config_sel_dict = get_dict(database, config_sel_file, [i+1 for i in range(len(run_config))])
        correct_sel_dict = get_dict(database, correct_sel_file, ['correct_selected', 'correct_generated'])
            
    with alive_bar(len(test_data), bar = 'fish', spinner = 'fish2', title=f'Processing Questions for {database}') as bar: 
        for item in test_data:

            if str(item["question_id"]) in processed_ids:
                logger.info(f"Skipping already processed query {item['question_id']}")
                bar()
                continue

            MAX_THREADS = 3
            all_results = []
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
                future_to_config = {executor.submit(process_config, config, item, database): config for config in run_config}
                for future in concurrent.futures.as_completed(future_to_config):
                    all_results.append(future.result())
            
            if collect_data:
                gold = item['SQL']
                try:
                    gold_res = execute_sql_timeout(database, sql_query=gold)
                except Exception as e:
                    logger.critical(f"ERROR IN GOLD SQL: {e}")

                correct_gen = []
                for  sql, id in all_results:
                    try:
                        res = execute_sql_timeout(database, sql)
                        if set(res) == set(gold_res):
                            correct_gen_dict[database][str(id)]+=1
                            correct_gen.append(sql)
                    except Exception as e:
                        logger.error(f"Error in Candidate SQL {e}")

                if len(correct_gen) > 0:
                    correct_sel_dict[database]['correct_generated']+=1

            if len(all_results) > 1:
                sql, config_id = xiyan_basic_llm_selector(all_results, item['question'], selector_client, database, item['schema_used'], item['evidence'])
            else:
                sql = all_results[0]
            
            if collect_data:
                config_sel_dict[database][str(config_id)]+=1

                if sql in correct_gen:
                    correct_sel_dict[database]['correct_selected']+=1

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
                save_df(correct_sel_dict, correct_sel_file)
                save_df(config_sel_dict, config_sel_file)
                save_df(correct_gen_dict, correct_gen_file)
             
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
  selector_model = None,
  collect_data = False,
):
    """Process all databases in the specified directory."""

    if any(config['prompt_config']['shots'] > 0 for config in run_config):
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
            collect_data = collect_data,
        )
    if all(
        db["state"] == DatasetEvalStatus.COMPLETED.value
        for db in metadata["databases"].values()
    ):
        metadata["batch_info"]["overall_status"] = DatasetEvalStatus.COMPLETED.value
    with open(metadata_file_path, "w") as file:
        json.dump(metadata, file, indent=4)

def process_test_file(
    run_config,
    selector_model = None,
    save_db_files = False,
    collect_data = False,
):
    """
    Process a single test file containing test questions and generate SQL queries.

    Args:
        run_config (list): A list of configuration dictionaries for processing questions.
        selector_model (dict, optional): A dictionary specifying the selector client's parameters. 
                                          Required if multiple run configurations are used.
        save_db_files (bool, optional): If True, saves predictions and gold standards in separate
                                        files for each database. Default is False.

    Returns:
        None
    """
    test_file = PATH_CONFIG.bird_file_path()
    processed_test_file = PATH_CONFIG.processed_test_path(global_file=True)
    pred_path = PATH_CONFIG.formatted_predictions_path(global_file=True)
    gold_path = PATH_CONFIG.test_gold_path(global_file=True)

    with open(test_file, "r") as file:
        test_data = json.load(file)

    with open(processed_test_file, 'r') as file:
        processed_test_data = json.load(file)
        
    # Check if files have the same number of items
    if len(test_data) != len(processed_test_data):
        logger.error(f"Test data ({len(test_data)} items) and processed data ({len(processed_test_data)} items) have different lengths")
        return
        
    if os.path.exists(gold_path):
        with open(gold_path, "r") as file:
            gold_items = [line.strip() for line in file.readlines()]
            if len(gold_items) > 0 and len(gold_items) != len(test_data):
                logger.error(f"Gold data ({len(gold_items)} items) and test data ({len(test_data)} items) have different lengths")
                return

    # Load predicted queries if available
    predicted_queries = {}
    if os.path.exists(pred_path):
        with open(pred_path, 'r') as file:
            if os.path.getsize(pred_path) > 0:
                predicted_queries = json.load(file)

    predicted_ids = set(predicted_queries.keys())

    # Set current database
    current_database = test_data[0]['db_id']
    db.set_database(current_database)

    # Create samples collection for few shot prompts
    if any(config['prompt_config']['shots'] > 0 for config in run_config):
        make_samples_collection()

    # Initialize selector client if multiple run configs are used
    if len(run_config) > 1:    
        selector_client = ClientFactory.get_client(selector_model['model'][0], selector_model['model'][1], selector_model['temperature'], selector_model['max_tokens'])
    
    correct_gen_file = PATH_CONFIG.correct_generated_file()
    config_sel_file = PATH_CONFIG.config_selected_file()
    correct_sel_file = PATH_CONFIG.correct_selected_file()

    with alive_bar(len(test_data), bar='fish', spinner='fish2', title=f'Processing Questions', length=30) as bar:
        for test_item, processed_test_item in zip(test_data, processed_test_data):
            if str(test_item['question_id']) in predicted_ids:
                logger.info(f"Skipping already processed query {test_item['question_id']}")
                bar()
                continue
            
         
            if current_database != test_item['db_id']:
                current_database = test_item['db_id']
                db.set_database(current_database)

            if collect_data:
    
                correct_gen_dict = get_dict(current_database, correct_gen_file, [i+1 for i in range(len(run_config))])
                config_sel_dict = get_dict(current_database, config_sel_file, [i+1 for i in range(len(run_config))])
                correct_sel_dict = get_dict(current_database, correct_sel_file, ['correct_selected', 'correct_generated'])
            
            if test_item['question_id'] == processed_test_item["question_id"]:
                item = processed_test_item
            else:
                item = next((q for q in processed_test_data if q["question_id"] == test_item["question_id"]), None)

            MAX_THREADS = 6
            all_results = []

            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
                future_to_config = {executor.submit(process_config, config, item, current_database): config for config in run_config}
                for future in concurrent.futures.as_completed(future_to_config):
                    all_results.append(future.result())
            
            if collect_data:
                gold = item['SQL']
                try:
                    gold_res = execute_sql_timeout(current_database, sql_query=gold)
                except Exception as e:
                    logger.critical(f"ERROR IN GOLD SQL: {e}")

                correct_gen = []
                for  sql, id in all_results:
                    try:
                        res = execute_sql_timeout(current_database, sql)
                        if set(res) == set(gold_res):
                            correct_gen_dict[current_database][str(id)]+=1
                            correct_gen.append(sql)
                    except Exception as e:
                        logger.error(f"Error in Candidate SQL {e}")

                if len(correct_gen) > 0:
                    correct_sel_dict[current_database]['correct_generated']+=1
            
            if len(all_results) > 1:
                sql, config_id = xiyan_basic_llm_selector(all_results, item['question'], selector_client, current_database, item['schema_used'], item['evidence'])
            else:
                sql,config_id = all_results[0][0], all_results[0][1]
            
            if collect_data:
                config_sel_dict[current_database][str(config_id)]+=1

                if sql in correct_gen:
                    correct_sel_dict[current_database]['correct_selected']+=1

            predicted_queries[int(item["question_id"])] = (
                f"{sql}\t----- bird -----\t{current_database}"
            )

            with open(pred_path, "w") as file:
                json.dump(predicted_queries, file)
            
            if collect_data:
                save_df(correct_sel_dict, correct_sel_file)
                save_df(config_sel_dict, config_sel_file)
                save_df(correct_gen_dict, correct_gen_file)
            
            bar()

    if not save_db_files:
        return

    # Split predictions for each databases and save them to the corresponding files
    database_queries = defaultdict(dict) 
    database_gold = defaultdict(list)

    for test_item in test_data:
        db_name = test_item["db_id"]
        question_id = str(test_item["question_id"])

        if question_id in predicted_queries:
            database_queries[db_name][question_id] = predicted_queries[question_id]
            database_gold[db_name].append(f"{test_item['SQL']}\t{db_name}")

    for db_name in database_queries:
        formatted_pred_path = PATH_CONFIG.formatted_predictions_path(database_name=db_name)
        gold_sql_path = PATH_CONFIG.test_gold_path(database_name=db_name)

        os.makedirs(os.path.dirname(formatted_pred_path), exist_ok=True)

        # Save predicted queries 
        with open(formatted_pred_path, "w") as file:
            json.dump(database_queries[db_name], file, indent=4)

        # Save gold SQL queries
        with open(gold_sql_path, "w") as file:
            for line in database_gold[db_name]:
                file.write(f"{line}\n")


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
        - To enable improvement update each configs 'improve' key with the improver model, prompt_type (xiyan or basic), shots and max attempts to improve
        - To disable improvement set 'improve' to False or None. 
        - To test a number of variations simply add different configs in each list
        - To use pruned schema set 'prune_schema' to True
        - To use evidence in the prompts set 'add_evidence' to True
        - set collect_data to true to log data about candidate selection and refiner module

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
    "prune_schema",
    "add_evidence",
    "improve",
    'config_id'
    ]

    # Initial variables
    selector_model = {
        "model":[LLMType.DASHSCOPE, ModelType.DASHSCOPE_QWEN_MAX],
        "temperature": 0.2,
        "max_tokens": 8192,
    }

    config_options = [
        {
            "config_id":1,
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
            "config_id":2,
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
            "config_id":3,
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
        }
    ]

    if not validate_config(config_options, keys):
        logger.error("Config Not Correctly Set")
        exit()

    collect_data = True
    if PATH_CONFIG.dataset_type != PATH_CONFIG.sample_dataset_type:
        save_db_files = True
        process_test_file(
            run_config=config_options,
            selector_model=selector_model,
            save_db_files=save_db_files,
            collect_data=collect_data,
        )

    elif PATH_CONFIG.dataset_type == PATH_CONFIG.sample_dataset_type:
        file_name = "2024-12-24_18:10:36.json"
        metadata_file_path = None  # BATCH_JOB_METADATA_DIR + file_name
        process_all_databases(
            dataset_dir=PATH_CONFIG.dataset_dir(),
            metadata_file_path=metadata_file_path,
            run_config=config_options,
            selector_model = selector_model,
            collect_data = collect_data
        )
