from typing import Optional
import google.generativeai as genai
from utilities.constants.LLM_enums import LLMType, ModelType
from utilities.constants.response_messages import ERROR_API_FAILURE
from utilities.utility_functions import format_chat
from utilities.config import ALL_GOOGLE_KEYS
from services.base_client import Client
from google.generativeai.types import HarmCategory, HarmBlockThreshold
import random

class GoogleAIClient(Client):
    def __init__(self, model: ModelType, max_tokens: Optional[int] = 150, temperature: Optional[float] = 0.5):
        self.current_key_idx = random.randint(0,len(ALL_GOOGLE_KEYS)-1)
        self.client = genai.configure(api_key=ALL_GOOGLE_KEYS[self.current_key_idx])
        self.call_num=0
        self.call_limit=5
        super().__init__(model=model.value, temperature=temperature, max_tokens=max_tokens, client=self.client)
    
    def change_client(self):
        self.current_key_idx = (self.current_key_idx + 1) % len(ALL_GOOGLE_KEYS)
        self.client = genai.configure(api_key=ALL_GOOGLE_KEYS[self.current_key_idx])
        self.call_num = 0

    def execute_prompt(self, prompt: str) -> str:
        if self.call_num >= self.call_limit:
            self.change_client()
        try:
            model = genai.GenerativeModel(self.model)
            response = model.generate_content(
                contents=prompt,
                generation_config={
                    'temperature': self.temperature,
                    'max_output_tokens': self.max_tokens,
                },
                safety_settings={
                    HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
                    HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
                    HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
                    HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE
                }
            )
            self.call_num+=1
            return response.text

        except Exception as e1:
            current_key = self.current_key_idx
            self.change_client()
            while current_key != self.current_key_idx:
                try:
                    model = genai.GenerativeModel(self.model)
                    response = model.generate_content(
                        contents=prompt,
                        generation_config={
                            'temperature': self.temperature,
                            'max_output_tokens': self.max_tokens
                        }
                    )
                    self.call_num+=1
                    return response.text
                except Exception as e:
                    self.change_client()

            raise RuntimeError(ERROR_API_FAILURE.format(llm_type=LLMType.GOOGLE_AI.value, error=str(e1)))
    
    def execute_chat(self, chat, prompt):
        
        chat=format_chat(chat, {'user':'user', 'model':'model', 'content':'parts'})
        changes=0
        if self.call_num >= self.call_limit:
            self.change_client()
        while changes<len(ALL_GOOGLE_KEYS):
            try:
                model = genai.GenerativeModel(self.model)
                chat_model = model.start_chat(
                    history = chat
                )
                response = chat_model.send_message(prompt)
                self.call_num+=1
                return response.text
            except Exception as e:
                changes+=1
                error = str(e)
                self.change_client()

        raise RuntimeError(ERROR_API_FAILURE.format(llm_type=LLMType.GOOGLE_AI.value, error=str(error)))
        