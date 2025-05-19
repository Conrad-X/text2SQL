"""
This module provides a retry mechanism for handling API calls to Language Model (LLM) services.

It includes functionality to manage API keys, handle rate limit and quota exceeded errors,
and retry failed API calls with a backoff delay.
"""

import time
from typing import Any, Callable

from services.utils.api_key_manager import APIKeyManager
from utilities.constants.services.llm_enums import LLMType
from utilities.constants.services.response_messages import (
    ERROR_API_FAILURE, WARNING_ALL_API_KEYS_QUOTA_EXCEEDED)
from utilities.logging_utils import setup_logger

logger = setup_logger(__name__)


# Constants
BACKOFF_DELAY_SECONDS = 5
QUOTA_EXHAUSTED_KEYWORDS = [
    "rate limit",
    "quota",
    "429",
]


class LLMCallRetryHandler:
    """
    A class to handle retries for LLM API calls, manage API keys, and implement backoff delays.

    Attributes:
        key_manager (APIKeyManager): Manages the API keys for the LLM service.
        llm_type (LLMType): The type of LLM service (for logging).
        on_key_rotation (Callable[[str], None]): A callback function to reinitialize the client with a new API key.
        error_count (int): Counts the number of consecutive errors encountered.
    """

    def __init__(
        self,
        key_manager: APIKeyManager,
        llm_type: LLMType,
        on_api_key_rotation: Callable[[str], None],
    ):
        """
        Initialize the retry handler.

        Args:
            key_manager (APIKeyManager): Manages the API keys for the LLM service.
            llm_type (LLMType): The type of LLM service (for logging).
            on_key_rotation (Callable[[str], None], optional): A callback function to reinitialize the client with a new API key.
        Raises:
            ValueError: If on_key_rotation is None or not callable.
        """
        self.key_manager = key_manager
        self.error_count = 0
        self.on_api_key_rotation = on_api_key_rotation
        self.llm_type = llm_type

    def execute_with_retries(self, llm_call: Callable[[], Any]) -> str:
        """
        Execute an LLM API call with retries.

        Args:
            llm_call (Callable[[], Any]): The LLM API call to execute.

        Returns:
            str: The response from the LLM API call.
        """
        response = None
        while response is None:
            try:
                response = llm_call()
            except Exception as e:
                self.__handle_llm_call_exception(e)

        return response

    def __handle_llm_call_exception(self, e: Exception):
        """
        Handle exceptions raised during LLM API calls.

        Args:
            e (Exception): The raised exception.
        """
        if not self.is_quota_exceeded_error(e):
            raise RuntimeError(
                ERROR_API_FAILURE.format(llm_type=self.llm_type.value, error=str(e))
            )

        self.__handle_quota_exceeded

    def __handle_quota_exceeded(self):
        """Handle the scenario where the quota is exceeded."""
        self.error_count += 1
        self.key_manager.rotate_api_key()
        # Call the parent function's callback to manage new client creation with new api key or anything else
        self.on_api_key_rotation(self.key_manager.get_current_key())

        if self.error_count >= self.key_manager.get_num_of_keys():
            logger.warning(
                WARNING_ALL_API_KEYS_QUOTA_EXCEEDED.format(
                    llm_type=self.llm_type.value,
                )
            )
            self.__backoff_delay(BACKOFF_DELAY_SECONDS)
            self.error_count = 0

    def __backoff_delay(self, seconds: int) -> None:
        """
        Implement a backoff delay.

        Args:
            seconds (int): The number of seconds to delay.
        """
        time.sleep(seconds)

    def is_quota_exceeded_error(self, e: Exception) -> bool:
        """
        Determine whether the given exception indicates a quota/rate limit exhaustion.

        Specific to the configured LLM provider.

        Args:
            e (Exception): The raised exception.

        Returns:
            bool: True if the exception is related to quota/rate limiting.
        """
        message = str(e).lower()
        return any(keyword in message for keyword in QUOTA_EXHAUSTED_KEYWORDS)
