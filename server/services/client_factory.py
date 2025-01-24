from typing import Optional

from services.base_client import Client 
from services.openai_client import OpenAIClient
from services.anthropic_client import AnthropicClient
from services.google_ai_client import GoogleAIClient
from services.deepseek_client import DeepSeekClient

from utilities.constants.LLM_enums import LLMType, ModelType
from utilities.constants.response_messages import ERROR_UNSUPPORTED_CLIENT_TYPE

class ClientFactory:
    @staticmethod
    def get_client(type: LLMType, model: Optional[ModelType] = None, temperature: Optional[float] = 0.7, max_tokens: Optional[int] = 1000) -> Client:
        if type == LLMType.OPENAI:
            return OpenAIClient(model=model, temperature=temperature, max_tokens=max_tokens)
        elif type == LLMType.ANTHROPIC:
            return AnthropicClient(model=model, temperature=temperature, max_tokens=max_tokens)
        elif type == LLMType.GOOGLE_AI:
            return GoogleAIClient(model=model, temperature=temperature, max_tokens=max_tokens)
        elif type == LLMType.DEEPSEEK:
            return DeepSeekClient(model=model, temperature=temperature, max_tokens=max_tokens)
        else:
            raise ValueError(ERROR_UNSUPPORTED_CLIENT_TYPE)