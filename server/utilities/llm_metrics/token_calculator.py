from pathlib import Path
from typing import Dict, List, Union

import google.generativeai as genai
import tiktoken
import transformers
from anthropic import Anthropic
from dashscope import get_tokenizer
from utilities.config import GOOGLE_AI_API_KEY
from utilities.constants.services.llm_enums import LLMType, ModelType
from utilities.constants.response_messages import (
    ERROR_PRICING_INFORMATION_NOT_FOUND,
    ERROR_TOKEN_ESTIMATION_NOT_IMPLEMENTED_LLMTYPE)
from utilities.llm_metrics.pricing import PRICING
from utilities.utility_functions import format_chat, validate_llm_and_model


# TO DO: This file will not work until it is updated according to the new refactored services folder.
class TokenCalculator:
    def __init__(self, model: ModelType, llm_type: LLMType):
        self.llm_type = llm_type
        self.model = model
        validate_llm_and_model(llm_type, model)

    def calculate_tokens_for_prompt(self, prompt: str) -> int:
        if self.llm_type == LLMType.OPENAI:
            messages = [{"role": "user", "content": prompt}]
            return self.__calculate_token_count_openai(messages)
        elif self.llm_type == LLMType.ANTHROPIC:
            messages = [{"role": "user", "content": prompt}]
            return self.__calculate_token_count_anthropic(messages)
        elif self.llm_type == LLMType.GOOGLE_AI:
            return self.__calculate_token_count_googleai(prompt)
        elif self.llm_type == LLMType.DEEPSEEK:
            return self.__calculate_token_count_deepseek(prompt)
        elif self.llm_type == LLMType.DASHSCOPE:
            return self.__calculate_token_count_dashscope(prompt)
        else:
            raise ValueError(f"Unsupported LLM type: {self.llm_type}")

    def calculate_tokens_for_chat(self, chat: List[Dict]) -> int:
        if self.llm_type == LLMType.OPENAI:
            chat = format_chat(chat, {'system':'system','user':'user', 'model':'assistant', 'content':'content'})
            return self.__calculate_token_count_openai(chat)
        elif self.llm_type == LLMType.ANTHROPIC:
            chat = format_chat(chat, {'system':'system','user':'user', 'model':'assistant', 'content':'content'})
            return self.__calculate_token_count_anthropic(chat)
        elif self.llm_type == LLMType.DEEPSEEK:
            chat = format_chat(chat, {'system':'system', 'user':'user', 'model':'assistant', 'content':'content'})
            return self.__calculate_token_count_deepseek(chat)
        elif self.llm_type == LLMType.GOOGLE_AI:
            chat = format_chat(chat, {'system': 'system', 'user':'user', 'model':'model', 'content':'parts'})
            return self.__calculate_token_count_googleai(chat)
        elif self.llm_type == LLMType.DASHSCOPE:
            chat = format_chat(chat, {'system': 'system', 'user': 'user', 'model': 'assistant', 'content': 'content'})
            return self.__calculate_token_count_dashscope(chat)
        else:
            raise ValueError(ERROR_TOKEN_ESTIMATION_NOT_IMPLEMENTED_LLMTYPE.format(llm_type=self.llm_type.value))  
        
    def calculate_cost(self, input_tokens, output_tokens):
        try:
            pricing_info = PRICING[self.llm_type][self.model]
        except KeyError:
            raise ValueError(ERROR_PRICING_INFORMATION_NOT_FOUND.format(llm_type=self.llm_type.value, model=self.model.value))

        input_cost = pricing_info.get("input", 0)
        output_cost = pricing_info.get("output", 0)

        total_cost = ((input_cost * input_tokens) + (output_cost * output_tokens)) / 1000

        return total_cost

    def __calculate_token_count_openai(self, messages: List[dict]) -> int:
        # get encoding, if not found, use "o200k_base"
        encoding = tiktoken.Encoding
        try:
            encoding = tiktoken.encoding_for_model(self.model.value)
        except KeyError:
            encoding =  tiktoken.get_encoding("o200k_base")

        # calculate token count
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

        return total_token_count
    
    def __calculate_token_count_anthropic(self, messages: List[dict]) -> int:
        client = Anthropic()
        response = client.messages.count_tokens(
            model=self.model.value,
            messages=messages,
        )
        token_count = response.input_tokens
        return token_count
    
    def __calculate_token_count_deepseek(self, messages: Union[List[dict], str]) -> int:
        base_path = Path(__file__).parent.resolve()
        chat_tokenizer_dir = base_path / "deepseek_v3_tokenizer"
        tokenizer = transformers.AutoTokenizer.from_pretrained(chat_tokenizer_dir, trust_remote_code=True)
        
        if isinstance(messages, str):
            token_count = len(tokenizer.encode(messages))
            return token_count
        
        elif isinstance(messages, list):
            total_token_count = 0
            for message in messages:
                if isinstance(message, dict):
                    for value in message.values():
                        if isinstance(value, str):
                            total_token_count += len(tokenizer.encode(value))
            return total_token_count
    
    def __calculate_token_count_googleai(self, messages: Union[List[dict], str]) -> int:
        genai.configure(api_key=GOOGLE_AI_API_KEY)
        model = genai.GenerativeModel(self.model.value)   
        response = model.count_tokens(messages)
        return response.total_tokens

    def __calculate_token_count_dashscope(self, messages: Union[List[dict], str]) -> int:
        tokenizer = get_tokenizer(self.model.value)

        if isinstance(messages, str):
            token_count = len(tokenizer.encode(messages))
            return token_count
        
        elif isinstance(messages, list):
            total_token_count = 0
            for message in messages:
                if isinstance(message, dict):
                    for value in message.values():
                        if isinstance(value, str):
                            total_token_count += len(tokenizer.encode(value))
            return total_token_count
    