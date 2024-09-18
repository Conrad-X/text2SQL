from abc import ABC, abstractmethod

class Client(ABC):
    def __init__(self, model: str, temperature: float = 0.5, max_tokens: int = 150, client=None):
        self.model = model
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.client = client

    @abstractmethod
    def execute_prompt(self, prompt: str) -> str:
        pass

