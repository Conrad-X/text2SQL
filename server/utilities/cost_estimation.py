import tiktoken
import os
import json

from utilities.config import DATASET_DIR, UNMASKED_SAMPLE_DATA_FILE_PATH
from utilities.constants.LLM_enums import (
    ModelType,
    LLMType,
    VALID_LLM_MODELS,
    MODEL_COST,
)
from utilities.constants.response_messages import (
    ERROR_INVALID_MODEL_FOR_TOKEN_ESTIMATION,
    ERROR_TOKEN_ESTIMATION_NOT_IMPLEMENTED,
    ERROR_PROCESSING_ITEM,
    ERROR_FILE_NOT_FOUND,
    WARNING_MODEL_NOT_FOUND_FOR_ENCODING,
    WARNING_MODEL_MAY_UPDATE,
)

AVG_OUTPUT_TOKENS = 65


def validate_and_calculate_token_count(model: ModelType, messages: list):
    """Count tokens for OpenAI models based on example messages."""

    warnings = []

    model, model_warnings = validate_and_resolve_model(model)
    if model_warnings:
        warnings.append(model_warnings)

    token_count, token_count_warnings = calculate_token_count(model, messages)
    if token_count_warnings:
        warnings.append(token_count_warnings)

    return token_count, warnings


def calculate_token_count(model: ModelType, messages: list):
    """This function assumes that the model type is already validated and supported in the validate_and_resolve_model function."""

    encoding, encoding_warnings = get_encoding(model)

    tokens_per_message = 3
    tokens_per_name = 1

    total_token_count = sum(
        tokens_per_message
        + len(encoding.encode(value))
        + (tokens_per_name if key == "name" else 0)
        for message in messages
        for key, value in message.items()
    )
    total_token_count += 3  # every reply is primed with <|start|>assistant<|message|>

    return total_token_count, encoding_warnings


def validate_and_resolve_model(model: ModelType):
    """Check if the model is supported and change the model type to the most similar supported model."""

    # Only OpenAI models are supported for this token counter
    if model not in VALID_LLM_MODELS[LLMType.OPENAI]:
        raise ValueError(ERROR_INVALID_MODEL_FOR_TOKEN_ESTIMATION.format(model=model))

    supported_models = {
        ModelType.OPENAI_GPT3_5_TURBO_0125,
        ModelType.OPENAI_GPT4_0314,
        ModelType.OPENAI_GPT4_32K_0314,
        ModelType.OPENAI_GPT4_0613,
        ModelType.OPENAI_GPT4_32K_0613,
        ModelType.OPENAI_GPT4_O_MINI,
        ModelType.OPENAI_GPT4_O,
    }

    if model in supported_models:
        return model, None

    warnings = {
        "gpt-3.5-turbo": ModelType.OPENAI_GPT3_5_TURBO_0125,
        "gpt-4o-mini": ModelType.OPENAI_GPT4_O_MINI,
        "gpt-4o": ModelType.OPENAI_GPT4_O,
        "gpt-4": ModelType.OPENAI_GPT4_0613,
    }

    for key, resolved_model in warnings.items():
        if key in model.value:
            warning = WARNING_MODEL_MAY_UPDATE.format(
                model=key, resolved_model=resolved_model.value
            )
            return resolved_model, warning

    raise NotImplementedError(
        ERROR_TOKEN_ESTIMATION_NOT_IMPLEMENTED.format(model=model)
    )


def get_encoding(model: ModelType):
    """Get the tiktoken encoding used for token calculation for the specified mode"""

    try:
        return tiktoken.encoding_for_model(model.value), None
    except KeyError:
        return tiktoken.get_encoding(
            "o200k_base"
        ), WARNING_MODEL_NOT_FOUND_FOR_ENCODING.format(model=model)


def calculate_cost_and_tokens_for_file(file_path: str, model: ModelType, is_batched: bool):
    """Function to calculate the cost and tokens for a file that has JSON items formatted as:
        ..."body": {... "messages": [{ "role": "...", "content": "..."}],
        and each item starts at new line
    """

    if not os.path.exists(file_path):
        raise FileNotFoundError(ERROR_FILE_NOT_FOUND.format(file_path=file_path))

    input_tokens = 0
    output_tokens = 0
    total_items = 0

    warnings = []

    model, model_warnings = validate_and_resolve_model(model)
    if model_warnings:
        warnings.append(model_warnings)

    with open(file_path, "r") as file:
        for line in file:
            try:
                request_data = json.loads(line)
                messages = request_data["body"]["messages"]

                # Get token count for this prompt
                token_count, encoding_warnings = calculate_token_count(model, messages)
                if encoding_warnings:
                    warnings.append(encoding_warnings)

                input_tokens += token_count
                output_tokens += AVG_OUTPUT_TOKENS
                total_items += 1

            except Exception as e:
                raise RuntimeError(
                    ERROR_PROCESSING_ITEM.format(item=line, error=str(e))
                )
            
    aggregate_token_count = input_tokens + output_tokens
    estimated_total_cost = estimate_cost_for_tokens(model, input_tokens, output_tokens, is_batched)

    # print(f"Total Tokens in {file_path}: {input_tokens}")
    # print(f"Total Cost for model {model} for {file_path}: ${estimated_total_cost:.4f}")

    return aggregate_token_count, round(estimated_total_cost, 4), warnings


def estimate_cost_for_tokens(model: ModelType, input_tokens: int, output_tokens: int, is_batched: bool):
    """This function assumes that the model type is already validated and supported in the validate_and_resolve_model function."""

    # Validate that the model's cost is known
    if model not in MODEL_COST:
        raise ValueError(f"Model {model} does not have a cost estimation.")

    model_cost = MODEL_COST[model]
    if is_batched:
        input_rate = model_cost["batch_input"]
        output_rate = model_cost["batch_output"]
    else:
        input_rate = model_cost["input"]
        output_rate = model_cost["output"]

    # Calculate total cost
    input_cost = (input_tokens / 1000) * input_rate
    output_cost = (output_tokens / 1000) * output_rate
    total_cost = round(input_cost + output_cost, 4)

    return total_cost


def calculate_average_output_tokens_for_all_samples(model: ModelType):
    """Function to calculate the AVG_OUTPUT_TOKENS constant"""
    total_tokens = 0
    total_answers = 0

    for db_name in os.listdir(DATASET_DIR):
        db_name = os.path.splitext(db_name)[0] # remove .db in case we are working in synthetic data dir
        file_path = UNMASKED_SAMPLE_DATA_FILE_PATH.format(database_name = db_name)

        if not os.path.exists(file_path):
            continue

        data = []
        with open(file_path, "r") as f:
            data = json.load(f)

        messages = []
        for item in data:
            messages.append({"role": "assistant", "content": item["answer"]})
            total_answers += 1

        try:
            file_token_count, _ = calculate_token_count(model, messages)
        except Exception as e:
            print(f"Error processing file {file_path}: {e}")
            continue

        total_tokens += file_token_count

        average_for_file = file_token_count / len(data)
        print(f"Average for {file_path} is {average_for_file:.2f} tokens")

    average_tokens = total_tokens / total_answers if total_answers > 0 else 0
    print("Average of all samples is ", average_tokens)
    return average_tokens
