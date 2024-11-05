import json
import os

from utilities.prompts.prompt_factory import PromptFactory
from utilities.constants.LLM_enums import LLMType, ModelType
from utilities.constants.prompts_enums import PromptType
from utilities.constants.database_enums import DatabaseType
from utilities.config import (
    BATCH_INPUT_FILE_PATH,
    TEST_DATA_FILE_PATH
)
from utilities.constants.response_messages import (
    ERROR_SCHEMA_FILE_NOT_FOUND,
    ERROR_BATCH_INPUT_FILE_CREATION,
    ERROR_BATCH_INPUT_FILE_NOT_UPLOADED,
    ERROR_BATCH_JOB_CREATION,
    ERROR_BATCH_JOB_NOT_FOUND,
    ERROR_DOWNLOAD_BATCH_FILE,
    ERROR_BATCH_JOB_STATUS_NOT_COMPLETED,
    SUCCESS_BATCH_INPUT_FILE_EXISTS,
    SUCCESS_BATCH_INPUT_FILE_CREATED,
    SUCCESS_BATCH_JOB_EXISTS,
    SUCCESS_BATCH_JOB_CREATED,
    SUCCESS_BATCH_OUTPUT_FILE_DOWNLOADED
)

from services.client_factory import ClientFactory

def create_batch_input_file(prompt_type: PromptType, shots: int, model: ModelType, temperature: float, max_tokens: int, database_name: str):
    input_file_path = BATCH_INPUT_FILE_PATH.format(database_name=database_name)
    if os.path.exists(input_file_path):
        return {'success': SUCCESS_BATCH_INPUT_FILE_EXISTS}
    
    schema_file_path = f"{TEST_DATA_FILE_PATH.format(database_name=database_name)}"
    schema_file_data = None

    try:
        with open(schema_file_path, "r") as f:
            schema_file_data = json.load(f)
    except FileNotFoundError as e:
        raise RuntimeError(ERROR_SCHEMA_FILE_NOT_FOUND.format(error=(str(e))))

    try:
        batch_input = []
        for item in schema_file_data:
            prompt = PromptFactory.get_prompt_class(prompt_type=prompt_type, target_question=item["question"], shots=shots)
            batch_input.append({
                "custom_id": f"request-{len(batch_input) + 1}",
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": model.value,
                    "messages": [
                        {"role": "system", "content": prompt},
                    ],
                    "max_tokens": max_tokens,
                    "temperature": temperature
                }
            })

        batch_input_dir = os.path.dirname(input_file_path)
        os.makedirs(batch_input_dir, exist_ok=True)

        with open(input_file_path, "w") as f:
            for item in batch_input:
                json.dump(item, f)
                f.write('\n')

        return {'success': SUCCESS_BATCH_INPUT_FILE_CREATED}
    
    except Exception as e:
        raise RuntimeError(ERROR_BATCH_INPUT_FILE_CREATION.format(output_file_path=input_file_path, error=str(e)))

def create_and_run_batch_job(database_name: str):
    try:
        client = ClientFactory.get_client(LLMType.OPENAI)
        all_files = client.get_all_uploaded_files()

        # Check if batch file for that database type is already uploaded
        batch_input_file_name = os.path.basename(BATCH_INPUT_FILE_PATH.format(database_name = database_name))
        batch_input_file = next((file for file in all_files.data if batch_input_file_name == file.filename), None)
        
        if not batch_input_file:
            batch_input_file = client.upload_batch_input_file(database_name)
        else:
            # Check if batch job for that file already exists
            all_batches = client.get_all_batches()
            if any(batch.input_file_id == batch_input_file.id for batch in all_batches.data):
                return {'success': SUCCESS_BATCH_JOB_EXISTS}
        
        batch_job = client.create_batch_job(batch_input_file.id)
        return {'success': SUCCESS_BATCH_JOB_CREATED.format(job_id=batch_job.id)}
    
    except Exception as e:
        raise RuntimeError(ERROR_BATCH_JOB_CREATION.format(error=str(e)))
    
def download_batch_job_output_file(database_name: str):
    try:
        client = ClientFactory.get_client(LLMType.OPENAI)
        all_files = client.get_all_uploaded_files()

        batch_input_file_name = os.path.basename(BATCH_INPUT_FILE_PATH.format(database_name = database_name))
        batch_input_file = next((file for file in all_files.data if batch_input_file_name == file.filename), None)
        
        if not batch_input_file:
            raise RuntimeError(ERROR_BATCH_INPUT_FILE_NOT_UPLOADED.format(database_type=database_name))
        
        all_batch_jobs = client.get_all_batches()
        batch_job = next((batch for batch in all_batch_jobs.data if batch.input_file_id == batch_input_file.id), None)

        if not batch_job:
            raise RuntimeError(ERROR_BATCH_JOB_NOT_FOUND.format(input_file_id=batch_input_file.id))
        
        batch_job_status = client.client.batches.retrieve(batch_job.id).status
        if batch_job_status != "completed":
            raise RuntimeError(ERROR_BATCH_JOB_STATUS_NOT_COMPLETED.format(status=batch_job_status))
        
        batch_output_file = client.download_file(batch_job.output_file_id, database_name)
        return {'success': SUCCESS_BATCH_OUTPUT_FILE_DOWNLOADED.format(output_file_path=batch_output_file)}

    except Exception as e:
        raise RuntimeError(ERROR_DOWNLOAD_BATCH_FILE.format(str(e)))
