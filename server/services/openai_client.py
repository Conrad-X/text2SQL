
from openai import OpenAI

from utilities.config import OPENAI_API_KEY
from utilities.constants.LLM_enums import LLMType, ModelType
from utilities.constants.response_messages import ERROR_API_FAILURE

from services.base_client import Client 

class OpenAIClient(Client):
    def __init__(self, model: ModelType, max_tokens: int, temperature: float):
        client = OpenAI(api_key=OPENAI_API_KEY)
        super().__init__(model=model.value, temperature=temperature, max_tokens=max_tokens, client=client)

    def execute_prompt(self, prompt: str) -> str:
        try:
            response = self.client.chat.completions.create(
                model=self.model, 
                messages=[{"role": "user", "content": prompt}],
                max_tokens=self.max_tokens,
                temperature=self.temperature,
            )
            return response.choices[0].message.content.strip('```sql\n')
        except Exception as e:
            raise RuntimeError(ERROR_API_FAILURE.format(llm_type=LLMType.OPENAI.value, error=str(e)))



