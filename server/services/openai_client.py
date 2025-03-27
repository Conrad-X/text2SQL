
from openai import OpenAI
import os
from typing import Optional

from utilities.config import OPENAI_API_KEY, PATH_CONFIG

from utilities.constants.LLM_enums import LLMType, ModelType
from utilities.constants.response_messages import (
    ERROR_API_FAILURE, 
    ERROR_BATCH_JOB_CREATION, 
    ERROR_UPLOAD_BATCH_INPUT_FILE,
    ERROR_GET_ALL_BATCHES, 
    ERROR_GET_ALL_UPLOADED_FILES, 
    ERROR_DOWNLOAD_BATCH_FILE,
    ERROR_BATCH_INPUT_FILE_NOT_FOUND
)
from utilities.utility_functions import format_chat
from services.base_client import Client 

class OpenAIClient(Client):
    def __init__(self, model: Optional[ModelType], max_tokens: Optional[int], temperature: Optional[float]):
        client = OpenAI(api_key=OPENAI_API_KEY)
        super().__init__(model=model, temperature=temperature, max_tokens=max_tokens, client=client)

    def execute_prompt(self, prompt: str) -> str:
        try:
            response = ""
            if self.model in [ModelType.OPENAI_O1, ModelType.OPENAI_O3_MINI, ModelType.OPENAI_O1_MINI]:
                response = self.client.chat.completions.create(
                    model=self.model.value,
                    messages=[{"role": "user", "content": prompt}],
                    reasoning_effort="high"
                )
            
            else:
                response = self.client.chat.completions.create(
                    model=self.model.value, 
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=self.max_tokens,
                    temperature=self.temperature,
                )
            content = response.choices[0].message.content
            if content.startswith('```sql') and content.endswith('```'):
                return content.strip('```sql\n').strip('```')
            else:
                return content

        except Exception as e:
            raise RuntimeError(ERROR_API_FAILURE.format(llm_type=LLMType.OPENAI.value, error=str(e)))
        
    def upload_batch_input_file(self, database_name: str) -> str:
        file_path = PATH_CONFIG.batch_input_path(database_name=database_name)

        if not os.path.exists(file_path):
            raise FileNotFoundError(ERROR_BATCH_INPUT_FILE_NOT_FOUND.format(file_name=file_path))
        try:
            with open(file_path, "rb") as file:
                batch_input_file = self.client.files.create(file=file, purpose="batch")
            return batch_input_file
        except Exception as e:
            raise RuntimeError(ERROR_UPLOAD_BATCH_INPUT_FILE.format(error=str(e)))

    def create_batch_job(self, file_id: str) -> str:
        try:
            batch = self.client.batches.create(
                input_file_id=file_id,
                endpoint="/v1/chat/completions",
                completion_window="24h",
            )
            return batch
        except Exception as e:
            raise RuntimeError(ERROR_BATCH_JOB_CREATION.format(error=str(e)))
        
    def get_all_batches(self):
        try:
            return self.client.batches.list()
        except Exception as e:
            raise RuntimeError(ERROR_GET_ALL_BATCHES.format(error=str(e)))
        
    def get_all_uploaded_files(self):
        try:
            return self.client.files.list()
        except Exception as e:
            raise RuntimeError(ERROR_GET_ALL_UPLOADED_FILES.format(error=str(e)))

        
    def download_file(self, file_id: str, file_path: str):
        try:
            file_content = self.client.files.content(file_id)
            with open(file_path, "w") as f:
                f.write(file_content.text)
            
            return file_content.text
        except Exception as e:
            raise RuntimeError(ERROR_DOWNLOAD_BATCH_FILE.format(error=str(e)))
            
    def execute_chat(self, chat):
        
        chat=format_chat(chat, {'system':'system','user':'user', 'model':'assistant', 'content':'content'})
        try:
            response = ""
            if self.model in [ModelType.OPENAI_O1, ModelType.OPENAI_O3_MINI, ModelType.OPENAI_O1_MINI]:
                response = self.client.chat.completions.create(
                    model=self.model.value,
                    messages=chat,
                    reasoning_effort="high"
                )

            else:
                response = self.client.chat.completions.create(
                    model=self.model.value, 
                    messages=chat,
                    max_tokens=self.max_tokens,
                    temperature=self.temperature,
                )
            content = response.choices[0].message.content
            return content

        except Exception as e:
            raise RuntimeError(ERROR_API_FAILURE.format(llm_type=LLMType.OPENAI.value, error=str(e)))