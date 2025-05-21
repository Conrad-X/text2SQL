"""
This module defines the GoogleAIClient class, which serves as a client for interacting with Google's Generative AI API.

The client includes functionality for API key rotation, retry logic, and formatting chat histories.
It is designed to handle both single-text prompts and chat-based interactions with the model.
"""

from typing import Any, Dict, List, Optional, Tuple

import google.generativeai as genai
from google.generativeai.types import HarmBlockThreshold, HarmCategory
from services.chat_formatter.google_ai_chat_formatter import \
    GoogleAIChatFormatter
from services.clients.base_client import Client
from services.utils.api_key_manager import APIKeyManager
from services.utils.call_retry_handler import LLMCallRetryHandler
from utilities.config import GOOGLE_AI_API_KEYS
from utilities.constants.services.chat_format import ChatRole
from utilities.constants.services.indexing_constants import (
    MAX_OUTPUT_TOKENS_KEY, TEMPERATURE_KEY)
from utilities.constants.services.llm_enums import LLMConfig
from utilities.constants.services.response_messages import (
    ERROR_EMPTY_CHAT_HISTORY, ERROR_EMPTY_PROMPT)
from utilities.logging_utils import setup_logger

logger = setup_logger(__name__)


class GoogleAIClient(Client):
    """
    Client for Google's Generative AI API with key rotation and retry logic.

    Manages API keys, handles transient errors with retries, and formats chat
    histories for both prompt-based and chat-based calls.
    """

    _DEFAULT_SAFETY_SETTINGS = [
        {"category": category, "threshold": HarmBlockThreshold.BLOCK_NONE}
        for category in (
            HarmCategory.HARM_CATEGORY_HATE_SPEECH,
            HarmCategory.HARM_CATEGORY_HARASSMENT,
            HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
            HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
        )
    ]

    def __init__(self, llm_config: LLMConfig):
        """
        Initialize GoogleAIClient.

        Args:
            llm_config: Configuration specifying llm type, model type, temperature, and token limits.
        """
        super().__init__(llm_config)
        self.key_manager = APIKeyManager(GOOGLE_AI_API_KEYS)
        self.retry_handler = LLMCallRetryHandler(
            key_manager=self.key_manager,
            llm_type=self.llm_type,
            on_api_key_rotation=self._configure_genai,
        )
        self._configure_genai()

    def execute_prompt(self, prompt: str) -> str:
        """
        Send a single-text prompt to the model and return the generated text.

        Args:
            prompt: The input string to generate content from.

        Returns:
            The model's generated text.

        Raises:
            ValueError: If prompt is empty.
        """
        if not isinstance(prompt, str) or not prompt.strip():
            raise ValueError(ERROR_EMPTY_PROMPT)

        return self.retry_handler.execute_with_retries(
            lambda: self._send_prompt(prompt)
        )

    def execute_chat(self, chat=list[Tuple[ChatRole, str]]) -> str:
        """
        Format chat history, send it to the model, and return the generated reply.

        Args:
            chat_history: A sequence of message dicts (each with 'role' and 'content').

        Returns:
            The assistant's reply text.

        Raises:
            ValueError: If chat_history is empty or improperly structured.
        """
        if not chat:
            raise ValueError(ERROR_EMPTY_CHAT_HISTORY)

        chat_formatter = GoogleAIChatFormatter(self.llm_type)
        system_msg, last_user_msg, history = chat_formatter.format(chat)

        if not last_user_msg:
            raise ValueError(ERROR_EMPTY_PROMPT)

        return self.retry_handler.execute_with_retries(
            lambda: self._send_chat(system_msg, history, last_user_msg)
        )

    def _configure_genai(self) -> None:
        """Configure the Generative AI client."""
        api_key = self.key_manager.get_current_key()
        genai.configure(api_key=api_key)

    def _get_model(self, system_message: Optional[str] = None) -> genai.GenerativeModel:
        """
        Instantiate a GenerativeModel with the desired model_type and optional system instruction.

        Args:
            system_message: Optional instruction guiding model behavior.

        Returns:
            A configured GenerativeModel instance.
        """
        return genai.GenerativeModel(self.model_type, system_instruction=system_message)

    def _send_prompt(self, prompt: str) -> str:
        """
        Perform a single-content generation request.

        Args:
            prompt: The input text for generation.

        Returns:
            The generated text from the API.
        """
        model = self._get_model()
        response = model.generate_content(
            contents=prompt,
            generation_config={
                TEMPERATURE_KEY: self.temperature,
                MAX_OUTPUT_TOKENS_KEY: self.max_tokens,
            },
            safety_settings=self._DEFAULT_SAFETY_SETTINGS,
        )
        return response.text

    def _send_chat(
        self,
        system_message: Optional[str],
        history: List[Dict[str, Any]],
        user_message: Dict[str, Any],
    ) -> str:
        """
        Perform a chat-based model call.

        Args:
            system_message: Optional system instruction string.
            history: List of prior chat messages.
            user_message: The final user message dict to send.

        Returns:
            The generated reply text.
        """
        model = self._get_model(system_message=system_message)
        chat_session = model.start_chat(history=history)
        response = chat_session.send_message(user_message)
        return response.text
