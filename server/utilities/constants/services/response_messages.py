"""
This module defines various response messages used throughout the application.

These messages are categorized into errors and warnings, each serving a specific
purpose in handling different scenarios.
"""

# Errors
ERROR_API_FAILURE = "{llm_type} API error: {error}"
ERROR_UNSUPPORTED_CLIENT_TYPE = "Unsupported client type."
ERROR_EMPTY_PROMPT = "`prompt` must be a non-empty string"
ERROR_EMPTY_CHAT_HISTORY = "`chat_history` must contain at least one message"
ERROR_MODEL_DOES_NOT_SUPPORT_CHAT = "This model does not support chat completion."
ERROR_INVALID_MODEL_FOR_TYPE = "Model {model_type} is not valid for {llm_type}."
ERROR_UNSUPPORTED_CLIENT_TYPE = "Unsupported client type."

# Warnings
WARNING_ALL_API_KEYS_QUOTA_EXCEEDED = "All {llm_type} API keys quota-exhausted. Sleeping for 5s"