"""
Module defining DashScopeClient, a subclass of OpenAIClient tailored for DashScope's API.

This client inherits key rotation, retry logic, and message formatting, overriding model
capabilities and parameter construction for DashScope.
"""

from typing import Any, Dict, List

from services.clients.openai_client import OpenAIClient
from utilities.config import DASHSCOPE_API_KEYS
from utilities.constants.services.indexing_constants import (MAX_TOKENS_KEY,
                                                             MESSAGES_KEY,
                                                             MODEL_KEY,
                                                             TEMPERATURE_KEY)
from utilities.constants.services.llm_enums import LLMConfig

# DashScope API endpoint override
BASE_URL: str = "https://dashscope-intl.aliyuncs.com/compatible-mode/v1"


class DashScopeClient(OpenAIClient):
    """
    Client for DashScope’s chat API, extending OpenAIClient.

    Overrides OpenAIClient to disable O-series checks and adjust parameter
    construction for DashScope’s endpoint. Inherits key rotation, retry logic,
    and message formatting from the parent class.
    """

    def __init__(self, llm_config: LLMConfig) -> None:
        """
        Initialize DashScopeClient with DashScope-specific API keys and base URL.

        Args:
            llm_config: Configuration object specifying model, tokens, and temperature.
        """
        super().__init__(llm_config, api_keys=DASHSCOPE_API_KEYS, base_url=BASE_URL)

    @property
    def is_o_series_model(self) -> bool:
        """
        Dashscope models all support chat, so never treated as O-series.

        Returns:
            Always False for DashScopeClient.
        """
        return False

    def get_chat_completion_params(
        self, messages: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Build DashScope-specific parameters for chat completions.

        Args:
            messages: List of dicts with 'role' and 'content'.

        Returns:
            Dictionary including model, messages, max tokens, and temperature.
        """
        return {
            MODEL_KEY: self.model_type,
            MESSAGES_KEY: messages,
            MAX_TOKENS_KEY: self.max_tokens,
            TEMPERATURE_KEY: self.temperature,
        }
