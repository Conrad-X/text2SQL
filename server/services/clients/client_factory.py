"""
Module providing ClientFactory, which instantiates LLM clients based on configuration.

Supported LLM types:
  - OpenAI
  - Anthropic
  - Google AI
  - DeepSeek
  - DashScope

Validates the requested model type and constructs the appropriate client.
"""

from services.clients.anthropic_client import AnthropicClient
from services.clients.base_client import Client
from services.clients.dashscope_client import DashScopeClient
from services.clients.deepseek_client import DeepSeekClient
from services.clients.google_ai_client import GoogleAIClient
from services.clients.openai_client import OpenAIClient
from services.validators.model_validator import validate_llm_and_model
from utilities.constants.services.llm_enums import LLMConfig, LLMType
from utilities.constants.services.response_messages import \
    ERROR_UNSUPPORTED_CLIENT_TYPE


class ClientFactory:
    """
    Factory class to obtain LLM client instances from a given configuration.

    Methods:
        get_client: Return a Client subclass based on LLMConfig.
    """

    @staticmethod
    def get_client(llm_config: LLMConfig) -> Client:
        """
        Return a client instance based on the provided LLM configuration.

        Args:
            llm_config (LLMConfig): The configuration for the language model.

        Returns:
            Client: An instance of the client corresponding to the LLM type.

        Raises:
            ValueError: If the LLM type is not supported.
        """
        validate_llm_and_model(llm_config.llm_type, llm_config.model_type)

        client_map = {
            LLMType.OPENAI: OpenAIClient,
            LLMType.ANTHROPIC: AnthropicClient,
            LLMType.GOOGLE_AI: GoogleAIClient,
            LLMType.DEEPSEEK: DeepSeekClient,
            LLMType.DASHSCOPE: DashScopeClient,
        }

        client_class = client_map.get(llm_config.llm_type)
        if client_class is None:
            raise ValueError(ERROR_UNSUPPORTED_CLIENT_TYPE)

        return client_class(llm_config)
