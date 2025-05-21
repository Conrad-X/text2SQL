"""
This module provides a formatter class for converting global chat data into a format compatible with OpenAI's API.

The OpenAIChatFormatter transforms chat messages to work with OpenAI's API structure.
"""

from typing import Dict, List, Tuple

from services.chat_formatter.base_chat_formatter import LLMChatFormatter
from utilities.constants.services.chat_format import ChatDictKey, ChatRole


class OpenAIChatFormatter(LLMChatFormatter):
    """A formatter class for converting chat data into a format compatible with OpenAI's API."""

    def format(self, chat: List[Tuple[ChatRole, str]]) -> List[Dict[str, str]]:
        """
        Format the given chat data into a list of dictionaries.

        Args:
            chat (List[Tuple[ChatRole, str]]): A list of tuples where each tuple contains a ChatRole and a message string.

        Returns:
            List[Dict[str, str]]: A list of dictionaries where each dictionary contains a role and a message.
        """
        role_key = self.translation_map[ChatDictKey.ROLE]
        content_key = self.translation_map[ChatDictKey.CONTENT]
        roles_map = self.translation_map[ChatDictKey.ROLES_MAP]

        return [
            {role_key: roles_map[role], content_key: message}
            for role, message in chat
            if message
        ]
