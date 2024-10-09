from abc import ABC, abstractmethod
import json
import os

from utilities.config import DatabaseConfig

class BasePrompt(ABC):
    def __init__(self, examples=None, target_question=None, shots=None):
        if shots and shots > 0:
            # finding similarity between example will be here
            file_path = os.path.join(
                os.path.abspath(os.path.dirname(__file__)), 
                f'../../data/sample_questions_and_queries/{DatabaseConfig.ACTIVE_DATABASE.value}_schema.json' 
            )

            with open(file_path, 'r') as file:
                all_samples = json.load(file)
                self.examples= all_samples[:shots]
        else:
            self.examples= examples

        self.shots = shots
        self.target_question = target_question

    @abstractmethod
    def get_prompt(self) -> str:
        pass