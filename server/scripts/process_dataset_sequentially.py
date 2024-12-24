import json
import os
import re
import time
from datetime import datetime
from typing import Dict
import logging
from tqdm import tqdm

from app import db
from utilities.config import (
    TEST_DATA_FILE_PATH,
    UNMASKED_SAMPLE_DATA_FILE_PATH,
    DATASET_DIR,
    ChromadbClient,
)
from utilities.constants.LLM_enums import LLMType, ModelType
from utilities.constants.prompts_enums import PromptType
from utilities.constants.script_constants import (
    FORMATTED_PRED_FILE,
    GENERATE_BATCH_SCRIPT_PATH,
    BATCH_JOB_METADATA_DIR,
    DatasetEvalStatus,
)
from utilities.prompts.prompt_factory import PromptFactory
from utilities.vectorize import vectorize_data_samples
from services.client_factory import ClientFactory
from services.base_client import Client

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
logging.getLogger("tqdm").setLevel(logging.WARNING)


def format_sql_response(sql_response: str) -> str:
    """Format the SQL response."""

    sql = re.sub(r"^```sqlite\s*", "", sql_response)
    sql = re.sub(r"\s*```$", "", sql)
    sql = sql.replace("\n", " ").replace("\\n", " ")
    if sql.startswith("SELECT"):
        return sql
    return "SELECT " + sql


def initialize_metadata(
    metadata_file_path: str,
    model: ModelType,
    prompt_types_with_shots: Dict[PromptType, int],
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
                    str(key): value for key, value in prompt_types_with_shots.items()
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
    prompt_types_with_shots: Dict[PromptType, int],
    metadata: Dict,
    metadata_file_path: str,
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
                        gold_items = gold_file.readlines()

    # Identify already processed question IDs
    processed_ids = set(predicted_scripts.keys())

    with open(TEST_DATA_FILE_PATH.format(database_name=database), "r") as f:
        test_data = json.load(f)

    for item in tqdm(test_data, desc=f"Processing {database}", unit="item"):
        if str(item["question_id"]) in processed_ids:
            logger.info(f"Skipping already processed query {item['question_id']}")
            continue

        for prompt_type, shots in prompt_types_with_shots.items():
            prompt = PromptFactory.get_prompt_class(
                prompt_type=prompt_type,
                target_question=item["question"],
                shots=shots,
            )
            sql = format_sql_response(client.execute_prompt(prompt=prompt))
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

            time.sleep(5)

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
    prompt_types_with_shots: Dict[PromptType, int],
):
    """ Process all databases in the specified directory. """
    
    metadata, metadata_file_path = initialize_metadata(
        metadata_file_path, model, prompt_types_with_shots, temperature, max_tokens
    )
    client = ClientFactory.get_client(llm_type, model, temperature, max_tokens)
    databases = [d for d in os.listdir(dataset_dir) if d != ".DS_Store"]

    for database in tqdm(databases[:2], desc="Processing all databases"):
        process_database(
            database, client, prompt_types_with_shots, metadata, metadata_file_path
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
    llm_type = LLMType.OPENAI
    model = ModelType.OPENAI_GPT4_O_MINI
    temperature = 0.7
    max_tokens = 8192

    # Prompt Configurations
    prompt_types_with_shots = {PromptType.OPENAI_DEMO: 0}

    # File Configurations
    file_name = "2024-12-24_18:10:36.json" 
    metadata_file_path = None # Should be none if you want to create a new metadata file

    process_all_databases(
        dataset_dir=DATASET_DIR,
        metadata_file_path=metadata_file_path,
        llm_type=llm_type,
        model=model,
        temperature=temperature,
        max_tokens=max_tokens,
        prompt_types_with_shots=prompt_types_with_shots,
    )
