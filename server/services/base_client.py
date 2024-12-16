from abc import ABC, abstractmethod
from typing import Optional

from utilities.constants.LLM_enums import ModelType

class Client(ABC):
    def __init__(self, model: Optional[ModelType], temperature: Optional[float] = 0.5, max_tokens: Optional[int] = 150, client=None):
        self.model = model
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.client = client

    @abstractmethod
    def execute_prompt(self, prompt: str) -> str:
        pass

    @abstractmethod
    def upload_batch_input_file(self, upload_file_path: str) -> str:
        pass

    @abstractmethod
    def create_batch_job(self, file_id: str) -> str:
        pass

    @abstractmethod
    def get_all_batches(self):
        pass

    @abstractmethod
    def get_all_uploaded_files(self):
        pass

    @abstractmethod
    def download_file(self, file_id: str, file_path: str):
        pass

