import time
import random
from typing import Optional

import google.generativeai as genai
from google.generativeai.types import HarmCategory, HarmBlockThreshold

from services.base_client import Client
from utilities.config import ALL_GOOGLE_KEYS
from utilities.logging_utils import setup_logger
from utilities.utility_functions import format_chat
from utilities.constants.LLM_enums import LLMType, ModelType
from utilities.constants.response_messages import (
    ERROR_API_FAILURE,
    WARNING_GOOGLE_API_KEY_ROTATION,
)

logger = setup_logger(__name__)

# Constants
QUOTA_EXCEEDED_ERROR_CODE = "429"
GENERATION_SAFETY_SETTINGS = [
    {"category": HarmCategory.HARM_CATEGORY_HATE_SPEECH, "threshold": HarmBlockThreshold.BLOCK_NONE},
    {"category": HarmCategory.HARM_CATEGORY_HARASSMENT, "threshold": HarmBlockThreshold.BLOCK_NONE},
    {"category": HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT, "threshold": HarmBlockThreshold.BLOCK_NONE},
    {"category": HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT, "threshold": HarmBlockThreshold.BLOCK_NONE},
]

class GoogleAIClient(Client):
    def __init__(self, model: ModelType, max_tokens: Optional[int] = 150, temperature: Optional[float] = 0.5):
        self.__current_key_index = random.randint(0, len(ALL_GOOGLE_KEYS) - 1) 
        self.__key_rotation_cycles = 0
        self.client = genai.configure(api_key=ALL_GOOGLE_KEYS[self.__current_key_index])
        super().__init__(model=model.value, temperature=temperature, max_tokens=max_tokens, client=self.client)

    def __rotate_api_key(self):
        self.__current_key_index = (self.__current_key_index + 1) % len(ALL_GOOGLE_KEYS)
        self.client = genai.configure(api_key=ALL_GOOGLE_KEYS[self.__current_key_index])

        if self.__current_key_index == 0:
            self.__key_rotation_cycles += 1
            logger.warning(WARNING_GOOGLE_API_KEY_ROTATION.format(key_rotation_cycles=self.__key_rotation_cycles))
            time.sleep(5)

    def __retry_on_quota_exceeded(self, call_fn):
        response = None
        while response is None:
            try:
                response = call_fn()
            except Exception as e:
                if QUOTA_EXCEEDED_ERROR_CODE in str(e):
                    self.__rotate_api_key()
                else:
                    raise RuntimeError(ERROR_API_FAILURE.format(
                        llm_type=LLMType.GOOGLE_AI.value,
                        error=str(e)
                    ))
        
        return response

    def execute_prompt(self, prompt: str) -> str:
        def generate_text_response():
            model = genai.GenerativeModel(self.model)
            response = model.generate_content(
                contents=prompt,
                generation_config={
                    "temperature": self.temperature,
                    "max_output_tokens": self.max_tokens,
                },
                safety_settings=GENERATION_SAFETY_SETTINGS,
            )
            return response.text

        return self.__retry_on_quota_exceeded(generate_text_response)


    def execute_chat(self, chat) -> str:
        chat = format_chat(chat, {"system": "system", "user": "user", "model": "model", "content": "parts"})

        def generate_chat_response():
            system_msg = chat[0]["parts"]
            user_msg = next((msg["parts"] for msg in reversed(chat) if msg["role"] == "user"), None)
            history = chat[1:-1] if len(chat) > 2 else []

            model = genai.GenerativeModel(
                model_name=self.model,
                system_instruction=system_msg,
            )
            chat_model = model.start_chat(history=history)
            response = chat_model.send_message(user_msg)
            return response.text

        return self.__retry_on_quota_exceeded(generate_chat_response)