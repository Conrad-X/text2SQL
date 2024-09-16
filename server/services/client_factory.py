from services.base_client import Client 
from services.openai_client import OpenAIClient
from services.anthropic_client import AnthropicClient

from utilities.constants.LLM_config import LLMType, ModelType
from utilities.constants.message_templates import ERROR_UNSUPPORTED_CLIENT_TYPE

class ClientFactory:
    @staticmethod
    def get_client(type: LLMType, model: ModelType, temperature: float = 0.7, max_tokens: int = 1000) -> Client:
        if type == LLMType.OPENAI:
            return OpenAIClient(model=model, temperature=temperature, max_tokens=max_tokens)
        elif type == LLMType.ANTHROPIC:
            return AnthropicClient(model=model, temperature=temperature, max_tokens=max_tokens)
        else:
            raise ValueError(ERROR_UNSUPPORTED_CLIENT_TYPE)