"""This module defines the base class for formatting chat messages for different LLM types."""

from abc import ABC, abstractmethod
from typing import Any, List, Tuple

from utilities.constants.services.chat_format import (CHAT_FORMATS_BY_LLM_TYPE,
                                                      ChatRole)
from utilities.constants.services.llm_enums import LLMType


class LLMChatFormatter(ABC):
    """An abstract base class for formatting chat messages for different LLM types."""

    def __init__(self, llm_type: LLMType):
        """
        Initialize the LLMChatFormatter with the given LLM type.

        Args:
            llm_type (LLMType): The type of LLM.
        """
        self.translation_map = CHAT_FORMATS_BY_LLM_TYPE[llm_type]

    @abstractmethod
    def format(self, chat: List[Tuple[ChatRole, str]]) -> Any:
        """
        Format the chat messages.

        Args:
            chat (List[Tuple[ChatRole, str]]): A list of tuples containing chat roles and messages.s
        """
        pass
