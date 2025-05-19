"""
Module defining AnthropicClient, a wrapper for Anthropic's chat API with API key management.

API key management, retry logic, and message formatting.
Inherits from Client and integrates with Anthropic's SDK.
"""

from typing import Any, Dict, List, Optional, Tuple

from anthropic import Anthropic
from services.chat_formatter.anthropic_chat_formatter import \
    AnthropicChatFormatter
from services.clients.base_client import Client
from services.utils.api_key_manager import APIKeyManager
from services.utils.call_retry_handler import LLMCallRetryHandler
from utilities.config import ANTHROPIC_API_KEYS
from utilities.constants.services.chat_format import ChatRole
from utilities.constants.services.indexing_constants import (MAX_TOKENS_KEY,
                                                             MESSAGES_KEY,
                                                             MODEL_KEY,
                                                             SYSTEM_KEY,
                                                             TEMPERATURE_KEY)
from utilities.constants.services.llm_enums import LLMConfig, LLMType
from utilities.constants.services.response_messages import (
    ERROR_EMPTY_CHAT_HISTORY, ERROR_EMPTY_PROMPT)


class AnthropicClient(Client):
    """
    Client for interacting with Anthropic's chat API.

    Manages API keys with rotation, applies retry logic to API calls,
    and formats chat messages according to Anthropic's requirements.
    """

    def __init__(self, llm_config: LLMConfig) -> None:
        """
        Initialize the AnthropicClient.

        Args:
            llm_config: Configuration object specifying model type, max tokens, and temperature.
        """
        super().__init__(llm_config)
        self.key_manager = APIKeyManager(all_keys=ANTHROPIC_API_KEYS)
        self.retry_handler = LLMCallRetryHandler(
            key_manager=self.key_manager,
            llm_type=self.llm_type,
            on_api_key_rotation=self._configure_client,
        )
        self._configure_client()
        self.formatter = AnthropicChatFormatter(LLMType.ANTHROPIC)

    def execute_prompt(self, prompt: str) -> str:
        """
        Send a single-text prompt to the Anthropic API and return the response.

        Args:
            prompt: Non-empty user input string.

        Returns:
            The generated text from Anthropic.

        Raises:
            ValueError: If `prompt` is empty or only whitespace.
            RuntimeError: If the API call fails after retrying.
        """
        if not isinstance(prompt, str) or not prompt.strip():
            raise ValueError(ERROR_EMPTY_PROMPT)

        _, messages = self.formatter.format([(ChatRole.USER, prompt)])
        return self.retry_handler.execute_with_retries(
            lambda: self._create_completion(messages)
        )

    def execute_chat(self, chat: List[Tuple[ChatRole, str]]) -> str:
        """
        Send a sequence of chat messages and return the assistant's reply.

        Args:
            chat_history: List of tuples pairing ChatRole with message content.

        Returns:
            The assistant's reply text.

        Raises:
            ValueError: If `chat_history` is empty.
            RuntimeError: If the API call fails after retrying.
        """
        if not chat:
            raise ValueError(ERROR_EMPTY_CHAT_HISTORY)

        system_msg, messages = self.formatter.format(chat)
        return self.retry_handler.execute_with_retries(
            lambda: self._create_completion(messages, system_msg)
        )

    def _configure_client(self) -> None:
        """Configure the Anthropic SDK client with the current API key."""
        api_key = self.key_manager.get_current_key()
        self.client = Anthropic(api_key=api_key, max_retries=0)  # Disable retries

    def _create_completion(
        self,
        messages: List[Dict[str, Any]],
        system_msg: Optional[str] = None,
    ) -> str:
        """
        Construct and send a completion request.

        Args:
            messages: List of message dicts formatted for Anthropic.
            system_msg: Optional system instruction to prepend.

        Returns:
            The generated text from the first response.
        """
        params: Dict[str, Any] = {
            MODEL_KEY: self.model_type,
            MESSAGES_KEY: messages,
            MAX_TOKENS_KEY: self.max_tokens,
            TEMPERATURE_KEY: self.temperature,
        }

        if system_msg is not None:
            params[SYSTEM_KEY] = system_msg

        response = self.client.messages.create(**params)
        return response.content[0].text
