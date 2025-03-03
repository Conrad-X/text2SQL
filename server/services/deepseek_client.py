from services.base_client import Client
from utilities.constants.LLM_enums import LLMType, ModelType
from openai import OpenAI
from typing import Optional
from utilities.config import DEEPSEEK_API_KEY
from utilities.utility_functions import format_chat
from utilities.constants.response_messages import (
    ERROR_API_FAILURE
)
import re

class DeepSeekClient(Client):
    def __init__(self, model: Optional[ModelType], max_tokens: Optional[int], temperature: Optional[float]):
        client = OpenAI(api_key=DEEPSEEK_API_KEY, base_url='https://api.deepseek.com')
        super().__init__(model=model, temperature=temperature, max_tokens=max_tokens, client=client)

    def execute_prompt(self, prompt: str) -> str:
        try:
            response = self.client.chat.completions.create(
                model=self.model.value, 
                messages=[{"role": "user", "content": prompt}],
                max_tokens=self.max_tokens,
                temperature=self.temperature,
                stream=False
            )
            content = response.choices[0].message.content
            pattern = r"```sql\n(.*?)```"
            match = re.search(pattern, content, re.DOTALL)
            return match.group(1).strip() if match else content
            
        except Exception as e:
            raise RuntimeError(ERROR_API_FAILURE.format(llm_type=LLMType.DEEPSEEK.value, error=str(e)))
        
    def execute_chat(self, chat):
        
        chat=format_chat(chat, {'system':'system', 'user':'user', 'model':'assistant', 'content':'content'})
        try:
            response = self.client.chat.completions.create(
                model=self.model.value, 
                messages=chat,
                max_tokens=self.max_tokens,
                temperature=self.temperature,
                stream = False
            )
            content = response.choices[0].message.content
            pattern = r"```sql\n(.*?)```"
            match = re.search(pattern, content, re.DOTALL)
            return match.group(1).strip() if match else content

        except Exception as e:
            raise RuntimeError(ERROR_API_FAILURE.format(llm_type=LLMType.DEEPSEEK.value, error=str(e)))