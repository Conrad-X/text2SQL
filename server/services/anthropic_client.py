from anthropic import Anthropic

from utilities.constants.LLM_config import LLMType, ModelType
from utilities.config import ANTHROPIC_API_KEY
from utilities.constants.message_templates import ERROR_API_FAILURE

from services.base_client import Client 

class AnthropicClient(Client):
    def __init__(self, model: ModelType, max_tokens: int, temperature: float):
        client = Anthropic(api_key=ANTHROPIC_API_KEY)
        super().__init__(model=model.value, temperature=temperature, max_tokens=max_tokens, client=client)

    def execute_prompt(self) -> str:
        try:
            response = self.client.messages.create(
                model=self.model,
                messages=[{"role": "user", "content": self.prompt}],
                max_tokens=self.max_tokens,
                temperature=self.temperature,
            )
            return response.content[0].text
        except Exception as e:
            raise RuntimeError(ERROR_API_FAILURE.format(llm_type=LLMType.ANTHROPIC.value, error=str(e)))
