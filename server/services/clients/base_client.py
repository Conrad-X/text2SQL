"""
Abstract base client for interacting with Language Model services.

This module provides a common interface for various Language Model service implementations,
defining methods for executing prompts, managing batch jobs, file operations, and chat functionality.
Concrete implementations should override these methods with service-specific logic.
"""

from abc import ABC
from typing import Tuple

from utilities.constants.services.chat_format import ChatRole
from utilities.constants.services.llm_enums import LLMConfig


class Client(ABC):
    """
    Abstract base client for Language Model service interactions.

    This class defines the interface for interacting with various Language Model services,
    providing methods for prompt execution, batch processing, file management, and chat functionality.
    Concrete implementations should inherit from this class and implement the abstract methods.
    """

    def __init__(self, llm_config: LLMConfig):
        """
        Initialize the Language Model client with configuration parameters.

        Args:
            llm_config (LLMConfig): Configuration for the language model.
        """
        self.llm_type = llm_config.llm_type
        self.model_type = llm_config.model_type.value
        self.temperature = llm_config.temperature
        self.max_tokens = llm_config.max_tokens

    def execute_prompt(self, prompt: str) -> str:
        """
        Execute a single prompt and return the generated response.

        Args:
            prompt: The text prompt to send to the language model

        Returns:
            The generated text response from the language model
        """
        pass

    def excecute_chat(self, chat: list[Tuple[ChatRole, str]]) -> str:
        """
        Execute a chat-based interaction with the language model.

        Args:
            chat: Chat context or history
            prompt: The new prompt to send within the chat context

        Returns:
            The generated response within the chat context
        """
        pass
