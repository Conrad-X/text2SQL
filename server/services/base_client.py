from abc import ABC, abstractmethod

class Client(ABC):
    def __init__(self, model: str, temperature: float = 0.5, max_tokens: int = 150, client=None):
        self.model = model
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.prompt = None
        self.client = client

    @abstractmethod
    def execute_prompt(self) -> str:
        pass

    def set_prompt(self, schema: str, question: str):
        self.prompt = (
            f"### Complete sqlite SQL query only and with no explanation\n"
            f"### Given the following database schema :\n {schema}\n"
            f"### Answer the following: {question}\nSELECT*/"
        )
