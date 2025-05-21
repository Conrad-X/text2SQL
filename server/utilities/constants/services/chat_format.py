"""This module defines the chat format for different LLM types."""

from enum import Enum

from utilities.constants.services.llm_enums import LLMType


class ChatDictKey(Enum):
    """Enum for chat dictionary keys."""

    ROLE = "role"
    CONTENT = "content"
    ROLES_MAP = "roles_map"


class ChatRole(Enum):
    """Enum for chat roles."""

    SYSTEM = "system"
    USER = "user"
    MODEL = "model"


"""Dictionary mapping LLM types to their respective chat formats."""
CHAT_FORMATS_BY_LLM_TYPE = {
    LLMType.OPENAI: {
        ChatDictKey.ROLE: ChatDictKey.ROLE.value,
        ChatDictKey.CONTENT: "content",
        ChatDictKey.ROLES_MAP: {
            ChatRole.SYSTEM: "system",
            ChatRole.USER: "user",
            ChatRole.MODEL: "assistant",
        },
    },
    LLMType.ANTHROPIC: {
        ChatDictKey.ROLE: ChatDictKey.ROLE.value,
        ChatDictKey.CONTENT: "content",
        ChatDictKey.ROLES_MAP: {
            ChatRole.SYSTEM: "system",
            ChatRole.USER: "user",
            ChatRole.MODEL: "assistant",
        },
    },
    LLMType.GOOGLE_AI: {
        ChatDictKey.ROLE: ChatDictKey.ROLE.value,
        ChatDictKey.CONTENT: "parts",
        ChatDictKey.ROLES_MAP: {
            ChatRole.SYSTEM: "system",
            ChatRole.USER: "user",
            ChatRole.MODEL: "model",
        },
    },
}
