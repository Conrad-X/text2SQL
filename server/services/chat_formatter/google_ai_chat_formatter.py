"""
This module contains the GoogleAIChatFormatter class, which is responsible for formatting chat messages.

The GoogleAIChatFormatter properly formats messages for compatibility with Google's AI services.
"""

from typing import Any, Dict, List, Optional, Tuple

from services.chat_formatter.base_chat_formatter import LLMChatFormatter
from utilities.constants.services.chat_format import ChatDictKey, ChatRole


class GoogleAIChatFormatter(LLMChatFormatter):
    """
    A formatter class for Google AI chat messages.

    This class inherits from LLMChatFormatter and provides methods to format chat messages
    for use with Google's AI services.
    """

    def format(
        self, chat: List[Tuple[ChatRole, str]]
    ) -> Tuple[str, str, List[Dict[str, Any]]]:
        """
        Format the chat messages into a tuple containing the system message.

        Creates a tuple with system message, latest user message, and formatted chat history.

        Args:
            chat (List[Tuple[ChatRole, str]]): A list of tuples where each tuple
                contains a ChatRole and a message string.

        Returns:
            Tuple[str, str, List[Dict[str, Any]]]: A tuple containing the system
                message, the latest user message, and the formatted chat history.
        """
        system_msg = self._extract_system_message(chat)
        last_user_msg = self._extract_latest_user_message(chat)
        formatted_history = self._format_history(chat, last_user_msg)

        return system_msg, last_user_msg, formatted_history

    def _format_history(
        self, chat: List[Tuple[ChatRole, str]], last_user_msg: Optional[str]
    ) -> List[Dict[str, Any]]:
        """
        Format the chat history by mapping roles and filtering out unnecessary messages.

        Args:
            chat (List[Tuple[ChatRole, str]]): A list of tuples where each tuple
                contains a ChatRole and a message string.
            last_user_msg (Optional[str]): The latest user message in the chat.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries representing the formatted chat history.
        """
        role_key = self.translation_map[ChatDictKey.ROLE]
        content_key = self.translation_map[ChatDictKey.CONTENT]
        roles_map = self.translation_map[ChatDictKey.ROLES_MAP]

        formatted_history = []

        for role, message in chat:
            if not self._should_keep_chat_message(role, message, last_user_msg):
                mapped_role = roles_map[role]
                formatted_chat_item = {role_key: mapped_role, content_key: [message]}
                formatted_history.append(formatted_chat_item)

        return formatted_history

    def _extract_system_message(
        self, chat: List[Tuple[ChatRole, str]]
    ) -> Optional[str]:
        """
        Extract the system message from the chat history.

        Args:
            chat (List[Tuple[ChatRole, str]]): A list of tuples where each tuple
                contains a ChatRole and a message string.

        Returns:
            Optional[str]: The system message if found, otherwise None.
        """
        for role, message in chat:
            if role == ChatRole.SYSTEM and message:
                return message
        return None

    def _extract_latest_user_message(
        self, chat: List[Tuple[ChatRole, str]]
    ) -> Optional[str]:
        """
        Extract the latest user message from the chat history.

        Args:
            chat (List[Tuple[ChatRole, str]]): A list of tuples where each tuple
                contains a ChatRole and a message string.

        Returns:
            Optional[str]: The latest user message if found, otherwise None.
        """
        latest_role, latest_message = chat[-1]
        if latest_role == ChatRole.USER and latest_message:
            return latest_message
        return None

    def _should_keep_chat_message(
        self, role: ChatRole, message: str, last_user_msg: Optional[str]
    ) -> bool:
        """
        Determine whether a chat message should be included in the formatted history.

        Args:
            role (ChatRole): The role of the message sender.
            message (str): The message content.
            last_user_msg (Optional[str]): The latest user message in the chat.

        Returns:
            bool: True if the message should be included, False otherwise.
        """
        return (
            not message
            or (role == ChatRole.USER and message == last_user_msg)
            or role == ChatRole.SYSTEM
        )
