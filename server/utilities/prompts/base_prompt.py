from abc import ABC, abstractmethod
import json

from utilities.vectorize import fetch_few_shots
from utilities.constants.response_messages import ERROR_FETCHING_EXAMPLES, ERROR_SCHEMA_FILE_NOT_FOUND

class BasePrompt(ABC):
    def __init__(self, examples = None, target_question = None, shots = 0, schema_format = None):
        self.target_question = target_question
        self.shots = shots
        self.schema_format=schema_format
        
        if not self.shots or self.shots <= 0:
            self.examples = None
            return
            
        if examples and len(examples) > 0:
            self.examples = examples[:self.shots] if self.shots <= len(examples) else examples
            return
        
        self.examples = self.fetch_examples_based_on_query_similarity()
        
    def fetch_examples_based_on_query_similarity(self):
        try:
            return fetch_few_shots(self.shots, self.target_question)
        except FileNotFoundError as e:
            raise FileNotFoundError(ERROR_SCHEMA_FILE_NOT_FOUND.format(error=str(e)))
        except Exception as e:
            raise RuntimeError(ERROR_FETCHING_EXAMPLES.format(error=str(e)))

    @abstractmethod
    def get_prompt(self) -> str:
        pass