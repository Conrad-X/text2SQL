import json
import os
from datetime import datetime
from tqdm import tqdm

from utilities.cost_estimation import calculate_cost_and_tokens_for_file
from utilities.config import DATASET_DIR, BATCH_INPUT_FILE_PATH
from utilities.constants.LLM_enums import ModelType
from utilities.constants.prompts_enums import PromptType
from utilities.batch_job import create_batch_input_file, upload_and_run_batch_job

from utilities.constants.script_constants import BATCH_JOB_METADATA_DIR, BatchFileStatus


def generate_and_run_batch_input_files(
    prompt_types_with_shots: dict,
    model: ModelType,
    temperature: float,
    max_tokens: int,
    dataset_dir: str,
):
    databases = [d for d in os.listdir(dataset_dir)]

    uploaded_batch_input_files = []
    batch_jobs_dict = {}

    for database in tqdm(databases, desc="Creating and running batch input files"):
        db_name = os.path.splitext(database)[0]  # remove .db in case we are working in synthetic data dir

        try:
            create_batch_input_file(
                prompt_types_with_shots=prompt_types_with_shots,
                model=model,
                temperature=temperature,
                max_tokens=max_tokens,
                database_name=db_name,
            )

            _, estimated_cost, _ = calculate_cost_and_tokens_for_file(
                file_path=BATCH_INPUT_FILE_PATH.format(database_name=db_name),
                model=model,
                is_batched=True,
            )

            input_file_id, batch_job_id = upload_and_run_batch_job(
                database_name=db_name
            )

            uploaded_batch_input_files.append(input_file_id)

            batch_jobs_dict[batch_job_id] = {
                "dataset": DATASET_DIR,
                "database": database,
                # converting PromptType to str because PromptType is an enum which cannot be serialized by json.dump
                "candidates": {
                    str(key): value for key, value in prompt_types_with_shots.items()
                },
                "state": BatchFileStatus.UPLOADED.value,
                "eatimated_cost": estimated_cost,
            }

        except Exception as e:
            tqdm.write(str(e))

    # Storing batch job metadata with the corresponding DB directory
    now = datetime.now()
    time_stamp = now.strftime("%Y-%m-%d_%H:%M:%S")
    metadata_file_path = os.path.join(BATCH_JOB_METADATA_DIR, f"{time_stamp}.json")

    os.makedirs(BATCH_JOB_METADATA_DIR, exist_ok=True)

    with open(metadata_file_path, "w") as file:
        json.dump(batch_jobs_dict, file)

    return metadata_file_path


if __name__ == "__main__":
    """
    This script automates the process of creating batch input files for multiple databases, uploading the files to an API client, and running batch jobs for LLM evaluation.
    To run this script:

    1. Set Up Required Configurations:
        - Ensure `DATASET_TYPE` is configured in `utilities.config` based on your dataset:
            - `DatasetType.BIRD_TRAIN` for training data.
            - `DatasetType.BIRD_DEV` for development data.
            - `DatasetType.SYNTHETIC` for synthetic data.
        - Verify that the dataset exists in the `DATASET_DIR` specified in the configuration.

    2. Run the Script:
        - Navigate to the project directory.
        - Execute the script using the following command:
            python3 -m scripts.generate_batch_files

    3. Expected Functionality:
        - The script performs the following steps:
            - Creates batch input files for candidate prompt types and shots for each database.
            - Uploads the batch input files to the LLM client (e.g., OpenAI) and initiates batch jobs.
            - Stores batch job metadata, linking batch jobs to their respective databases.
        - Batch input files are saved to paths defined in `BATCH_INPUT_FILE_PATH`.
        - Batch job metadata is saved as a timestamped `.json` file in the `BATCH_JOB_METADATA_DIR`.

    Extra Note:
        - Make sure that the dataset is split in the correct ratio. e.g. 50% for test data and 50% for examples data.
        - If you want to generate a batch input file for only one candidate, just set the prompt_type_with_shots accordingly.
    """

    # Inputs, update these accordingly
    prompt_type_with_shots = {PromptType.CODE_REPRESENTATION: 0, PromptType.DAIL_SQL: 5,PromptType.OPENAI_DEMO: 0}
    temperature = 0.5
    model = ModelType.OPENAI_GPT4_O_MINI
    max_tokens = 1000

    metadata_file_path = generate_and_run_batch_input_files(
        prompt_types_with_shots=prompt_type_with_shots,
        model=model,
        temperature=temperature,
        max_tokens=max_tokens,
        dataset_dir=DATASET_DIR,
    )

    print("Batch Metadata File saved at: ", metadata_file_path)
