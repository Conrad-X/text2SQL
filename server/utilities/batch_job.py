import json
import os
from typing import Dict

from app import db
from services.client_factory import ClientFactory
from tqdm import tqdm
from utilities.config import PATH_CONFIG, ChromadbClient
from utilities.constants.LLM_enums import LLMType, ModelType
from utilities.constants.prompts_enums import PromptType
from utilities.constants.response_messages import (
    ERROR_BATCH_INPUT_FILE_CREATION, ERROR_BATCH_JOB_CREATION,
    ERROR_BATCH_JOB_STATUS_NOT_COMPLETED, ERROR_DOWNLOAD_BATCH_FILE,
    ERROR_SCHEMA_FILE_NOT_FOUND, SUCCESS_BATCH_INPUT_FILE_CREATED,
    SUCCESS_BATCH_OUTPUT_FILE_DOWNLOADED)
from utilities.constants.script_constants import BatchJobStatus
from utilities.prompts.prompt_factory import PromptFactory
from utilities.vectorize import vectorize_data_samples


def create_batch_input_file(
    prompt_types_with_shots: Dict[PromptType, int],
    model: ModelType,
    temperature: float,
    max_tokens: int,
    database_name: str,
):
    # Change Database
    db.set_database(database_name)
    ChromadbClient.reset_chroma(
        PATH_CONFIG.processed_train_path(database_name=database_name)
    )
    vectorize_data_samples()

    # Set file path
    batch_input_file_path = PATH_CONFIG.batch_input_path(database_name=database_name)

    # Load test data
    test_data = None
    try:
        with open(PATH_CONFIG.processed_test_path(database_name=database_name), "r") as f:
            test_data = json.load(f)
    except FileNotFoundError as e:
        raise RuntimeError(ERROR_SCHEMA_FILE_NOT_FOUND.format(error=(str(e))))

    try:
        # Create batch input items
        batch_input = []
        for item in tqdm(test_data, desc="Processing queries", unit="item"):
            for prompt_type, shots in prompt_types_with_shots.items():
                prompt = PromptFactory.get_prompt_class(
                    prompt_type=prompt_type,
                    target_question=item["question"],
                    shots=shots,
                )
                batch_input.append(
                    {
                        "custom_id": f"{prompt_type.value}-{item['question_id']}",
                        "method": "POST",
                        "url": "/v1/chat/completions",
                        "body": {
                            "model": model.value,
                            "messages": [
                                {"role": "system", "content": prompt},
                            ],
                            "max_tokens": max_tokens,
                            "temperature": temperature,
                        },
                    }
                )

        # Create and save batch input file
        batch_input_dir = os.path.dirname(batch_input_file_path)
        os.makedirs(batch_input_dir, exist_ok=True)

        with open(batch_input_file_path, "w") as f:
            for item in batch_input:
                json.dump(item, f)
                f.write("\n")

        return {"success": SUCCESS_BATCH_INPUT_FILE_CREATED}

    except Exception as e:
        raise RuntimeError(
            ERROR_BATCH_INPUT_FILE_CREATION.format(
                file_path=batch_input_file_path, error=str(e)
            )
        )


def upload_and_run_batch_job(database_name: str):
    try:
        client = ClientFactory.get_client(LLMType.OPENAI)

        batch_input_file = client.upload_batch_input_file(database_name)
        batch_job = client.create_batch_job(batch_input_file.id)

        return batch_input_file.id, batch_job.id

    except Exception as e:
        raise RuntimeError(ERROR_BATCH_JOB_CREATION.format(error=str(e)))


def download_batch_job_output_file(batch_job_id: str, download_file_path: str):
    try:
        client = ClientFactory.get_client(LLMType.OPENAI)

        batch_job = client.client.batches.retrieve(batch_job_id)

        if batch_job.status != BatchJobStatus.COMPLETED.value:
            raise RuntimeError(
                ERROR_BATCH_JOB_STATUS_NOT_COMPLETED.format(status=batch_job.status)
            )
        
        batch_output_file = client.download_file(
            batch_job.output_file_id, download_file_path
        )
        
        return {
            "success": SUCCESS_BATCH_OUTPUT_FILE_DOWNLOADED.format(
                output_file_path=batch_output_file
            )
        }

    except Exception as e:
        raise RuntimeError(ERROR_DOWNLOAD_BATCH_FILE.format(error=str(e)))
