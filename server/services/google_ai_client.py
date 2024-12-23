from typing import Optional
import google.generativeai as genai
from utilities.constants.LLM_enums import LLMType, ModelType
from utilities.config import GOOGLE_AI_API_KEY
from utilities.constants.response_messages import ERROR_API_FAILURE
from services.base_client import Client

class GoogleAIClient(Client):
    def __init__(self, model: ModelType, max_tokens: Optional[int] = 150, temperature: Optional[float] = 0.5):
        self.client = genai.configure(api_key=GOOGLE_AI_API_KEY)
        super().__init__(model=model.value, temperature=temperature, max_tokens=max_tokens, client=self.client)

    def execute_prompt(self, prompt: str) -> str:
        try:
            model = genai.GenerativeModel(self.model)
            response = model.generate_content(
                contents=prompt,
                generation_config={
                    'temperature': self.temperature,
                    'max_output_tokens': self.max_tokens
                }
            )
            return response.text

        except Exception as e:
            raise RuntimeError(ERROR_API_FAILURE.format(llm_type=LLMType.GOOGLE_AI.value, error=str(e)))