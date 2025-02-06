import json
import os
import re
import sqlite3
import time
import concurrent.futures
from datetime import datetime
from typing import Dict
from alive_progress import alive_bar
from tqdm import tqdm
from itertools import product
from app import db
from utilities.prompts.prompt_templates import IMPROVEMENT_PROMPT_TEMPLATE
from utilities.config import (
    DATABASE_SQLITE_PATH,
    TEST_DATA_FILE_PATH,
    UNMASKED_SAMPLE_DATA_FILE_PATH,
    DATASET_DIR,
    ChromadbClient,
    DatabaseConfig,
)
from utilities.logging_utils import setup_logger
from utilities.utility_functions import (
    format_sql_response,
    convert_enums_to_string,
)
from utilities.constants.LLM_enums import LLMType, ModelType
from utilities.constants.prompts_enums import FormatType, PromptType
from utilities.constants.script_constants import (
    FORMATTED_PRED_FILE,
    GENERATE_BATCH_SCRIPT_PATH,
    BATCH_JOB_METADATA_DIR,
    DatasetEvalStatus,
    GOOGLE_RESOURCE_EXHAUSTED_EXCEPTION_STR,
)
from utilities.prompts.prompt_factory import PromptFactory
from utilities.vectorize import vectorize_data_samples
from services.client_factory import ClientFactory
from services.base_client import Client
from utilities.constants.response_messages import ERROR_SHOTS_REQUIRED
from utilities.sql_improvement import improve_sql_query

logger = setup_logger(__name__)
    
def initialize_metadata(
    metadata_file_path: str,
    config
) -> Dict:
    """Initializes or loads metadata from the specified file."""

    if not metadata_file_path:
        time_stamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
        metadata_file_path = f"{BATCH_JOB_METADATA_DIR}{time_stamp}.json"

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
        sql = improve_sql_query(
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
                matches = None
            )
    
    sql = prompt_llm(prompt, config['improve_sql'], client, improv_client, config['max_improve_sql_attempts'], database, item['question'], config['prompt_config']['shots'])

    return sql

def selector(sqls):
    return sqls[0]

def process_database(
    database: str,
    run_config,
    metadata,
    metadata_file_path
):
    """Main processing function for a single database."""

    if database not in metadata["databases"]:
        metadata["databases"][database] = {
            "state": DatasetEvalStatus.IN_PROGRESS.value,
        }
    elif metadata["databases"][database]["state"] == DatasetEvalStatus.COMPLETED.value:
        logger.info(f"Database {database} has already been processed. Skipping...")
        return

    # Set database and reset vector database
    db.set_database(database)
    ChromadbClient.reset_chroma(
        UNMASKED_SAMPLE_DATA_FILE_PATH.format(database_name=database)
    )
    vectorize_data_samples()

    formatted_pred_path = (
        f"{GENERATE_BATCH_SCRIPT_PATH}{database}/{FORMATTED_PRED_FILE}_{database}.json"
    )
    gold_sql_path = f"{GENERATE_BATCH_SCRIPT_PATH}{database}/gold_{database}.sql"

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

    with open(TEST_DATA_FILE_PATH.format(database_name=database), "r") as f:
        test_data = json.load(f)

    with alive_bar(len(test_data), bar = 'fish', spinner = 'fish2', title=f'Processing Questions for {database}') as bar: 
        for item in test_data:

            if str(item["question_id"]) in processed_ids:
                logger.info(f"Skipping already processed query {item['question_id']}")
                bar()
                continue

            MAX_THREADS = 2
            all_results = []

            # Use ThreadPoolExecutor to manage threads
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
                future_to_config = {executor.submit(process_config, config, item, database): config for config in run_config}
                for future in concurrent.futures.as_completed(future_to_config):
                    all_results.append(future.result())
            
            sql = selector(all_results)

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
  run_config
):
    """Process all databases in the specified directory."""

    metadata, metadata_file_path = initialize_metadata(
        metadata_file_path, convert_enums_to_string(run_config)
    )

    databases = [d for d in os.listdir(dataset_dir) if os.path.isdir(os.path.join(dataset_dir, d))]
    
    for database in tqdm(databases, desc="Processing all databases"):
        process_database(
            database,
            run_config,
            metadata,
            metadata_file_path
        )

    if all(
        db["state"] == DatasetEvalStatus.COMPLETED.value
        for db in metadata["databases"].values()
    ):
        metadata["batch_info"]["overall_status"] = DatasetEvalStatus.COMPLETED.value
    with open(metadata_file_path, "w") as file:
        json.dump(metadata, file, indent=4)


if __name__ == "__main__":
    """
    To run this script:

    1. Ensure you have set the correct `DATASET_TYPE` in `utilities.config`:
       - Set `DATASET_TYPE` to DatasetType.BIRD_TRAIN for training data.
       - Set `DATASET_TYPE` to DatasetType.BIRD_DEV for development data.

    2. Metadata File Configuration:
        - If continuing previous processing, provide the timestamp of the existing metadata file.
        - If starting a new process, set the `metadata_file_path` to `None`.

    3. Adjust Input Variables:
        - Ensure all input variables, such as file paths, LLM configurations, and prompt configurations, are correctly defined.
        - To add an option to improve the prompt, set `improve_sql` to `True`.
        - To limit the number of attempts to improve the prompt, set `max_improve_sql_attempts` accordingly.
        - To test a number of variations simply add different configs in each list

    4. Run the Script:
        - Execute the following command in the terminal `python3 -m scripts.process_dataset_sequentially`

    5. Expected Outputs:
        - Metadata File: A JSON metadata file is created or updated at the specified `metadata_file_path`.
        - Formatted Predictions: Predictions for each processed database are saved.
        - Gold SQL Scripts: Gold standard SQL scripts are saved alongside predictions.

    6. Additional Notes:
        - Processing includes formatting predictions, executing LLM prompts, and saving results. The script pauses for a short delay between processing to manage API rate limits.
    """

    # Initial variables

    config_options = [
        {
            "model": [LLMType.GOOGLE_AI, ModelType.GOOGLEAI_GEMINI_2_0_FLASH_EXP],
            "temperature": 0.2,
            "max_tokens": 8192,
            "prompt_config": {"type":PromptType.SEMANTIC_FULL_INFORMATION, "shots": 5, "format_type": FormatType.M_SCHEMA},
            "improve_sql": False,
            "max_improve_sql_attempts": 5,
            "improve_client": [LLMType.GOOGLE_AI, ModelType.GOOGLEAI_GEMINI_2_0_FLASH_EXP],
        },
        {
            "model": [LLMType.GOOGLE_AI, ModelType.GOOGLEAI_GEMINI_2_0_FLASH_EXP],
            "temperature": 0.5,
            "max_tokens": 8192,
            "prompt_config": {"type":PromptType.SEMANTIC_FULL_INFORMATION, "shots": 5, "format_type": FormatType.M_SCHEMA},
            "improve_sql": False,
            "max_improve_sql_attempts": 5,
            "improve_client": [LLMType.GOOGLE_AI, ModelType.GOOGLEAI_GEMINI_2_0_FLASH_EXP],
        }
    ]

    # File Configurations
    file_name = "2024-12-24_18:10:36.json"
    metadata_file_path = None  # BATCH_JOB_METADATA_DIR + file_name

    process_all_databases(
        dataset_dir=DATASET_DIR,
        metadata_file_path=metadata_file_path,
        run_config=config_options,
    )
