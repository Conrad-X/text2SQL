import json
import os
import time
from datetime import datetime
from typing import Dict
from tqdm import tqdm

from app import db
from utilities.config import (
    TEST_DATA_FILE_PATH,
    DATASET_DIR,
)
from utilities.logging_utils import setup_logger
from utilities.utility_functions import format_sql_response
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
from utilities.sql_improvement import improve_sql_query
from services.client_factory import ClientFactory
from services.base_client import Client
from utilities.constants.response_messages import ERROR_SHOTS_REQUIRED

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
                    str(key.value): {
                        "shots": prompt_types_with_shots[key]["shots"],
                        "format_type": prompt_types_with_shots[key][
                            "format_type"
                        ].value,
                    }
                    for key in prompt_types_with_shots
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


def process_database(
    database: str,
    client: Client,
    prompt_types_with_shots,
    metadata: Dict,
    metadata_file_path: str,
    improve_sql: bool,
    max_improve_sql_attempts: int,
    improver_client: Client,
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

    for item in tqdm(test_data[:1], desc=f"Processing {database}", unit="item"):
        if str(item["question_id"]) in processed_ids:
            logger.info(f"Skipping already processed query {item['question_id']}")
            continue

        for prompt_type in prompt_types_with_shots:
            try:
                shots = prompt_types_with_shots[prompt_type]["shots"]
            except KeyError:
                if prompt_type in [
                    PromptType.FULL_INFORMATION,
                    PromptType.SEMANTIC_FULL_INFORMATION,
                    PromptType.SQL_ONLY,
                    PromptType.DAIL_SQL,
                ]:
                    raise ValueError(ERROR_SHOTS_REQUIRED)
                else:
                    shots = None

            try:
                schema_format = prompt_types_with_shots[prompt_type]["format_type"]
            except KeyError:
                if (
                    prompt_type == PromptType.FULL_INFORMATION
                    or prompt_type == PromptType.SEMANTIC_FULL_INFORMATION
                ):
                    raise ValueError(
                        f"Format type not provided for {prompt_type.value} prompt"
                    )
                else:
                    schema_format = None

            prompt = PromptFactory.get_prompt_class(
                prompt_type=prompt_type,
                target_question=item["question"],
                shots=shots,
                schema_format=schema_format,
                matches={
                    "matched_tables": item["matched_tables"],
                    "matched_columns": item["matched_columns"],
                },
            )

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
                        break

            if improve_sql:
                sql = improve_sql_query(
                    sql,
                    max_improve_sql_attempts,
                    database,
                    improver_client,
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
    sql_generator_client: Client,
    prompt_types_with_shots,
    improve_sql: bool,
    max_improve_sql_attempts: int,
    improver_client: Client,
):
    """Process all databases in the specified directory."""

    metadata, metadata_file_path = initialize_metadata(
        metadata_file_path, model, prompt_types_with_shots, temperature, max_tokens
    )
    databases = [
        d
        for d in os.listdir(dataset_dir)
        if os.path.isdir(os.path.join(dataset_dir, d))
    ]

    for database in tqdm(databases[:3], desc="Processing all databases"):
        process_database(
            database,
            sql_generator_client,
            prompt_types_with_shots,
            metadata,
            metadata_file_path,
            improve_sql,
            max_improve_sql_attempts,
            improver_client,
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

    try:
        # Initial variables

        # SQL Generator LLM Configurations
        llm_type = LLMType.GOOGLE_AI
        model = ModelType.GOOGLEAI_GEMINI_2_0_FLASH_EXP
        temperature = 0.2
        max_tokens = 8192

        # SQL Generator LLM Client
        sql_generator_client = ClientFactory.get_client(
            llm_type, model, temperature, max_tokens
        )

        # Prompt Configurations
        prompt_types_with_shots = {
            PromptType.DAIL_SQL: {
                "shots": 5,
                "format_type": FormatType.M_SCHEMA,
            }
        }

        # File Configurations
        file_name = "2024-12-24_18:10:36.json"
        metadata_file_path = None  # BATCH_JOB_METADATA_DIR + file_name

        # Improve SQL Configurations
        improve_sql = True
        max_improve_sql_attempts = None
        improver_client = None

        if improve_sql:
            # SQL Improver Configurations
            max_improve_sql_attempts = 5

            # SQL Improver LLM Configurations
            llm_type = LLMType.DEEPSEEK
            model = ModelType.DEEPSEEK_REASONER
            temperature = 0.2
            max_tokens = 8192

            # SQL Improver LLM Client
            improver_client = ClientFactory.get_client(
                llm_type, model, temperature, max_tokens
            )

        process_all_databases(
            dataset_dir=DATASET_DIR,
            metadata_file_path=metadata_file_path,
            sql_generator_client=sql_generator_client,
            prompt_types_with_shots=prompt_types_with_shots,
            improve_sql=improve_sql,
            max_improve_sql_attempts=max_improve_sql_attempts,
            improver_client=improver_client,
        )

    except Exception as e:
        logger.error(f"Error processing dataset: {e}")
        raise e
