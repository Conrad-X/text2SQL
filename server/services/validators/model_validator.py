"""This module provides validation functions for LLM (Language Learning Model) types and model types."""

from utilities.constants.services.llm_enums import (VALID_LLM_MODELS, LLMType,
                                                    ModelType)
from utilities.constants.services.response_messages import (
    ERROR_INVALID_MODEL_FOR_TYPE, ERROR_UNSUPPORTED_CLIENT_TYPE)


def validate_llm_type(llm_type: LLMType):
    """
    Validate that the LLM type is supported.

    Args:
        llm_type (LLMType): The type of the LLM.

    Raises:
        ValueError: If the LLM type is not supported.
    """
    if llm_type not in VALID_LLM_MODELS:
        raise ValueError(ERROR_UNSUPPORTED_CLIENT_TYPE)


def validate_model_for_llm(llm_type: LLMType, model_type: ModelType):
    """
    Validate that the model type is valid for the given LLM type.

    Args:
        llm_type (LLMType): The type of the LLM.
        model_type (ModelType): The type of the model.

    Raises:
        ValueError: If the model type is not valid for the given LLM type.
    """
    if model_type not in VALID_LLM_MODELS[llm_type]:
        raise ValueError(
            ERROR_INVALID_MODEL_FOR_TYPE.format(
                model_type=model_type.value, llm_type=llm_type.value
            )
        )


def validate_llm_and_model(llm_type: LLMType, model_type: ModelType):
    """Validate LLM type and model type against supported combinations."""
    validate_llm_type(llm_type)
    validate_model_for_llm(llm_type, model_type)
