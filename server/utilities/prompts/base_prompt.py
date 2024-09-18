from abc import ABC, abstractmethod
import json
import os

class BasePrompt(ABC):
    def __init__(self, examples=None, target_question=None, shots=None):
        if shots and shots > 0:
            # finding similarity between examples will be here
            file_path = os.path.join(os.path.dirname(__file__), '../samples_questions_and_queries/hotel_schema.json')
            
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