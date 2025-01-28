import json
import os
import re
import sqlite3
import time
from datetime import datetime
from typing import Dict
from tqdm import tqdm

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
    execute_sql_query,
    format_schema,
    format_sql_response,
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
from utilities.vectorize import fetch_few_shots, vectorize_data_samples
from services.client_factory import ClientFactory
from services.base_client import Client

logger = setup_logger(__name__)


def initialize_metadata(
    metadata_file_path: str,
    model: ModelType,
    prompt_types_with_shots,
    temperature: float,
    max_tokens: int,
) -> Dict:
    """Initializes or loads metadata from the specified file."""

    if not metadata_file_path:
        time_stamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
        metadata_file_path = f"{BATCH_JOB_METADATA_DIR}{time_stamp}.json"

        os.makedirs(os.path.dirname(metadata_file_path), exist_ok=True)
        metadata = {
            "batch_info": {
                "model": model.name,
                "candidates": {
                    str(key): {
                        'shots': prompt_types_with_shots[key]["shots"],
                        'format_type': prompt_types_with_shots[key]["format_type"].value
                    }for key in prompt_types_with_shots
                },
                "temperature": temperature,
                "max_tokens": max_tokens,
                "overall_status": DatasetEvalStatus.IN_PROGRESS.value,
            },
            "databases": {},
        }
        with open(metadata_file_path, "w") as file:
            json.dump(metadata, file, indent=4)
    else:
        with open(metadata_file_path, "r") as file:
            metadata = json.load(file)

    return metadata, metadata_file_path


def generate_improvement_prompt(pred_sql, results, target_question, shots):
    formatted_schema = format_schema(FormatType.CODE, DatabaseConfig.DATABASE_URL)
    examples = fetch_few_shots(shots, target_question)

    examples_text = "\n".join(
        f"/* Question: {example['question']} */\n{example['answer']}\n"
        for example in examples
    )

    return IMPROVEMENT_PROMPT_TEMPLATE.format(
        formatted_schema=formatted_schema,
        examples=examples_text,
        target_question=target_question,
        pred_sql=pred_sql,
        results=results,
    )


def improve_sql_query(
    sql,
    max_improve_sql_attempts,
    database_name,
    client,
    target_question,
    shots,
):
    """Attempts to improve the given SQL query by executing it and refining it using the improvement prompt."""

    connection = sqlite3.connect(
        DATABASE_SQLITE_PATH.format(database_name=database_name)
    )
    for idx in range(max_improve_sql_attempts):
        try:
            # Try executing the query
            try:
                res = execute_sql_query(connection, sql)
                if not isinstance(res, RuntimeError):
                    res = res[:5]
                    if idx > 0:
                        break  # Successfully executed the query

            except Exception as e:
                logger.error(f"Error executing SQL: {e}")
                res = str(e)

            # Generate and execute improvement prompt
            prompt = generate_improvement_prompt(sql, res, target_question, shots)
            improved_sql = client.execute_prompt(prompt=prompt)
            improved_sql = format_sql_response(improved_sql)

            # Update SQL for the next attempt
            sql = improved_sql if improved_sql else sql

        except RuntimeError as e:
            if GOOGLE_RESOURCE_EXHAUSTED_EXCEPTION_STR in str(e):
                logger.warning("Quota exhausted. Retrying in 5 seconds...")
                time.sleep(5)

        except Exception as e:
            logger.error(f"Unhandled exception: {e}")
            break

    return sql


def process_database(
    database: str,
    client: Client,
    prompt_types_with_shots,
    metadata: Dict,
    metadata_file_path: str,
    improve_sql: bool,
    max_improve_sql_attempts: int,
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

    for item in tqdm(test_data, desc=f"Processing {database}", unit="item"):
        if str(item["question_id"]) in processed_ids:
            logger.info(f"Skipping already processed query {item['question_id']}")
            continue

        for prompt_type in prompt_types_with_shots:
            shots = prompt_types_with_shots[prompt_type]["shots"]
            try:
                schema_format = prompt_types_with_shots[prompt_type]["format_type"]
            except KeyError:
                if prompt_type == PromptType.FULL_INFORMATION:
                    raise ValueError("Format type not provided for FULL_INFORMATION prompt")
                else:
                    schema_format = None
                
            prompt = PromptFactory.get_prompt_class(
                prompt_type=prompt_type,
                target_question=item["question"],
                shots=shots,
                schema_format=schema_format
            )

            sql = ""
            while sql == "":
                try:
                    sql = format_sql_response(client.execute_prompt(prompt=prompt))
                except Exception as e:
                    if GOOGLE_RESOURCE_EXHAUSTED_EXCEPTION_STR in str(e):
                        # Rate limit exceeded: Too many requests. Retrying in 5 seconds...
                        time.sleep(5)

            if improve_sql:
                sql = improve_sql_query(
                    sql,
                    max_improve_sql_attempts,
                    database,
                    client,
                    item["question"],
                    shots,
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

    # Update the status if all test data has been processed
    if len(predicted_scripts) == len(test_data):
        metadata["databases"][database] = {
            "state": DatasetEvalStatus.COMPLETED.value,
        }

    with open(metadata_file_path, "w") as file:
        json.dump(metadata, file, indent=4)


def process_all_databases(
    dataset_dir: str,
    metadata_file_path: str,
    llm_type: LLMType,
    model: ModelType,
    temperature: float,
    max_tokens: int,
    prompt_types_with_shots,
    improve_sql: bool,
    max_improve_sql_attempts: int,
):
    """Process all databases in the specified directory."""

    metadata, metadata_file_path = initialize_metadata(
        metadata_file_path, model, prompt_types_with_shots, temperature, max_tokens
    )
    client = ClientFactory.get_client(llm_type, model, temperature, max_tokens)
    databases = [d for d in os.listdir(dataset_dir) if d != ".DS_Store"]

    for database in tqdm(databases, desc="Processing all databases"):
        process_database(
            database,
            client,
            prompt_types_with_shots,
            metadata,
            metadata_file_path,
            improve_sql,
            max_improve_sql_attempts,
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

    # LLM Configurations
    llm_type = LLMType.GOOGLE_AI
    model = ModelType.GOOGLEAI_GEMINI_2_0_FLASH_EXP
    temperature = 0.2
    max_tokens = 8192

    # Prompt Configurations
    prompt_types_with_shots = {PromptType.FULL_INFORMATION: {"shots": 5, "format_type": FormatType.M_SCHEMA}}

    # File Configurations
    file_name = "2024-12-24_18:10:36.json"
    metadata_file_path = None  # BATCH_JOB_METADATA_DIR + file_name

    # Improve SQL Configurations
    improve_sql = False
    improve_sql = False
    max_improve_sql_attempts = 5

    process_all_databases(
        dataset_dir=DATASET_DIR,
        metadata_file_path=metadata_file_path,
        llm_type=llm_type,
        model=model,
        temperature=temperature,
        max_tokens=max_tokens,
        prompt_types_with_shots=prompt_types_with_shots,
        improve_sql=improve_sql,
        max_improve_sql_attempts=max_improve_sql_attempts,
    )
