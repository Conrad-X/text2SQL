"""
This module contains the AnthropicChatFormatter class, which is responsible for formatting chat messages.

The AnthropicChatFormatter properly formats messages for compatibility with the Anthropic API.
"""

from typing import Dict, List, Optional, Tuple

from services.chat_formatter.base_chat_formatter import LLMChatFormatter
from utilities.constants.services.chat_format import ChatDictKey, ChatRole


class AnthropicChatFormatter(LLMChatFormatter):
    """A formatter for chat messages that is compatible with the Anthropic API."""

    def format(
        self, chat: List[Tuple[ChatRole, str]]
    ) -> Tuple[Optional[str], List[Dict[str, str]]]:
        """
        Format a list of chat messages into a tuple containing an optional system message.

        Creates a tuple with system message and formatted messages for Anthropic API.

        Args:
            chat (List[Tuple[ChatRole, str]]): The list of chat messages to format.

        Returns:
            Tuple[Optional[str], List[Dict[str, str]]]: A tuple containing the system message
            and the formatted messages.
        """
        system_msg = self._extract_system_message(chat)
        formatted_messages = self._format_messages(chat)

        return system_msg, formatted_messages

    def _format_messages(
        self, chat: List[Tuple[ChatRole, str]]
    ) -> List[Dict[str, str]]:
        """
        Format the chat messages into a list of dictionaries.

        Args:
            chat (List[Tuple[ChatRole, str]]): The list of chat messages to format.

        Returns:
            List[Dict[str, str]]: The formatted messages.
        """
        role_key = self.translation_map[ChatDictKey.ROLE]
        content_key = self.translation_map[ChatDictKey.CONTENT]
        roles_map = self.translation_map[ChatDictKey.ROLES_MAP]

        formatted_messages = []

        for role, message in chat:
            if role == ChatRole.SYSTEM:
                continue

            mapped_role = roles_map[role]
            formatted_message = {role_key: mapped_role, content_key: message}
            formatted_messages.append(formatted_message)

        return formatted_messages

    def _extract_system_message(
        self, chat: List[Tuple[ChatRole, str]]
    ) -> Optional[str]:
        """
        Extract the system message from the chat messages.

        Args:
            chat (List[Tuple[ChatRole, str]]): The list of chat messages to extract the system message from.

        Returns:
            Optional[str]: The system message, if found.
        """
        for role, message in chat:
            if role == ChatRole.SYSTEM and message:
                return message
        return None
