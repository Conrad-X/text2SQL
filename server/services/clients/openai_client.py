"""
Module defining OpenAIClient, a wrapper around OpenAI's Chat API.

Supports both single-prompt and chat-based interactions.
"""

from typing import Any, Dict, List, Optional, Tuple

from openai import OpenAI
from services.chat_formatter.openai_chat_formatter import OpenAIChatFormatter
from services.clients.base_client import Client
from services.utils.api_key_manager import APIKeyManager
from services.utils.call_retry_handler import LLMCallRetryHandler
from utilities.config import OPENAI_API_KEYS
from utilities.constants.services.chat_format import ChatRole
from utilities.constants.services.indexing_constants import (
    MAX_TOKENS_KEY, MESSAGES_KEY, MODEL_KEY, REASONING_EFFORT_KEY,
    TEMPERATURE_KEY)
from utilities.constants.services.llm_enums import (LLMConfig, LLMType,
                                                    ModelType)
from utilities.constants.services.response_messages import (
    ERROR_EMPTY_CHAT_HISTORY, ERROR_EMPTY_PROMPT,
    ERROR_MODEL_DOES_NOT_SUPPORT_CHAT)

# Configuration constants
REASONING_EFFORT_CONFIG = "high"


class OpenAIClient(Client):
    """
    Client for OpenAI's Chat API with support for.

      - API key management and rotation
      - Automatic retry on transient failures
      - Formatting of prompts and chat messages.
    """

    def __init__(
        self,
        llm_config: LLMConfig,
        api_keys: Optional[list[str]] = None,
        base_url: Optional[str] = None,
    ):
        """
        Initialize the OpenAI client with rotation and retry handlers.

        Args:
            llm_config (LLMConfig): Desired LLM configuration.
            api_keys (Optional[List[str]]): List of API keys to rotate.
            base_url (Optional[str]): Custom base URL for the API.
        """
        super().__init__(llm_config)
        self.key_manager = APIKeyManager(api_keys or OPENAI_API_KEYS)
        self.retry_handler = LLMCallRetryHandler(
            key_manager=self.key_manager,
            llm_type=self.llm_type,
            on_api_key_rotation=self._configure_client,
        )
        self.formatter = OpenAIChatFormatter(LLMType.OPENAI)
        self.base_url = base_url
        self._configure_client()

    @property
    def is_o_series_model(self) -> bool:
        """
        Determine if the configured model is an O-series, which lacks chat support.

        Returns:
            True if the model type is one of the O-series variants.
        """
        return self.model_type in {
            ModelType.OPENAI_O1.value,
            ModelType.OPENAI_O3_MINI.value,
            ModelType.OPENAI_O1_MINI.value,
            ModelType.OPENAI_O3.value,
            ModelType.OPENAI_O4_MINI.value,
        }

    def execute_prompt(self, prompt: str) -> str:
        """
        Send a single-prompt completion request and return the generated text.

        Args:
            prompt: Non-empty user input string.

        Returns:
            The model-generated text response.

        Raises:
            ValueError: If `prompt` is empty or only whitespace.
        """
        if not isinstance(prompt, str) or not prompt.strip():
            raise ValueError(ERROR_EMPTY_PROMPT)

        messages = self.formatter.format([(ChatRole.USER, prompt)])
        return self.retry_handler.execute_with_retries(
            lambda: self.__create_completion(messages)
        )

    def execute_chat(self, chat: list[Tuple[ChatRole, str]]) -> str:
        """
        Send a sequence of chat messages and return the assistant's reply.

        Args:
            chat: List of tuples pairing ChatRole with message content.

        Returns:
            The reply text from the assistant.

        Raises:
            ValueError: If `chat` is empty or the model does not support chat.
        """
        if not chat:
            raise ValueError(ERROR_EMPTY_CHAT_HISTORY)

        if self.is_o_series_model:
            raise ValueError(ERROR_MODEL_DOES_NOT_SUPPORT_CHAT)

        formatted_chat = self.formatter.format(chat)
        return self.retry_handler.execute_with_retries(
            lambda: self.__create_completion(formatted_chat)
        )

    def _configure_client(self):
        """Configure the OpenAI SDK client using the current API key."""
        api_key = self.key_manager.get_current_key()
        self.client = OpenAI(api_key=api_key, base_url=self.base_url)

    def __create_completion(self, messages: list[dict]) -> str:
        """Perform a chat completion API call.

        Args:
            messages: List of dicts formatted for OpenAI ('role', 'content').

        Returns:
            The content string of the first choice in the response.
        """
        params = self.get_chat_completion_params(messages)
        response = self.client.chat.completions.create(**params)
        return response.choices[0].message.content

    def get_chat_completion_params(
        self, messages: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Build parameter dict for the OpenAI chat completion endpoint.

        Args:
            messages: List of role/content messages to send.

        Returns:
            Dictionary with model, messages, and tuning parameters.
        """
        params = {
            MODEL_KEY: self.model_type,
            MESSAGES_KEY: messages,
        }

        if self.is_o_series_model:
            params[REASONING_EFFORT_KEY] = REASONING_EFFORT_CONFIG
        else:
            params[MAX_TOKENS_KEY] = self.max_tokens
            params[TEMPERATURE_KEY] = self.temperature

        return params
