from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any


class LLMType(Enum):
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    GOOGLE_AI = "google_ai"  
    DEEPSEEK = "deepseek"
    DASHSCOPE = "dashscope"

class ModelType(Enum):
    # OpenAI Models
    OPENAI_GPT3_5_TURBO_0125 = "gpt-3.5-turbo-0125"
    OPENAI_GPT4_0314 = "gpt-4-0314"
    OPENAI_GPT4_32K_0314 = "gpt-4-32k-0314"
    OPENAI_GPT4_0613 = "gpt-4-0613"
    OPENAI_GPT4_32K_0613 = "gpt-4-32k-0613"
    OPENAI_GPT4_O = "gpt-4o-2024-08-06"
    OPENAI_GPT4_O_MINI = "gpt-4o-mini-2024-07-18"
    OPENAI_O1 = "o1"
    OPENAI_O3 = "o3"
    OPENAI_O4_MINI = "o4-mini"
    OPENAI_O3_MINI = "o3-mini"
    OPENAI_O1_MINI = "o1-mini"


    # Anthropic Models
    ANTHROPIC_CLAUDE_3_5_SONNET = "claude-3-5-sonnet-20241022" 
    ANTHROPIC_CLAUDE_3_7_SONNET = "claude-3-7-sonnet-20250219" 
    ANTHROPIC_CLAUDE_3_SONNET = "claude-3-sonnet-20240229"
    ANTHROPIC_CLAUDE_3_5_HAIKU = "claude-3-5-haiku-20241022"
    ANTHROPIC_CLAUDE_3_HAIKU = "claude-3-haiku-20240307"
    ANTHROPIC_CLAUDE_3_OPUS = "claude-3-opus-20240229"
    
    # Google AI Models
    GOOGLEAI_GEMINI_1_5_FLASH = "gemini-1.5-flash"
    GOOGLEAI_GEMINI_1_5_PRO = "gemini-1.5-pro"
    GOOGLEAI_GEMINI_1_5_FLASH_8B = "gemini-1.5-flash-8b"
    GOOGLEAI_GEMINI_2_0_FLASH = "gemini-2.0-flash"
    GOOGLEAI_GEMINI_2_0_FLASH_THINKING_EXP_0121 = "gemini-2.0-flash-thinking-exp-01-21"
    GOOGLEAI_GEMINI_2_0_FLASH_LITE_PREVIEW_0205 = "gemini-2.0-flash-lite-preview-02-05"
    GOOGLEAI_GEMINI_2_0_PRO_EXP = "gemini-2.0-pro-exp-02-05"
    GOOGLEAI_GEMINI_2_5_PRO_EXP = "gemini-2.5-pro-exp-03-25"
    GOOGLEAI_GEMINI_2_5_PRO_PREVIEW = "gemini-2.5-pro-preview-03-25"
    GOOGLEAI_GEMINI_2_5_FLASH_PREVIEW = "gemini-2.5-flash-preview-04-17"
    # Fine Tuned Models
    GOOGLEAI_GEMINI_1_5_FLASH_SCHEMA_PRUNING_FT = "tunedModels/pruneschema3305samples-34bzckir3jfw"

    #DeepSeek Models
    DEEPSEEK_CHAT='deepseek-chat'
    DEEPSEEK_REASONER='deepseek-reasoner'

    #DashScope / Alibaba Models
    DASHSCOPE_QWEN_MAX ='qwen-max-latest'
    DASHSCOPE_QWEN_PLUS ='qwen-plus-latest'
    DASHSCOPE_QWEN_TURBO ='qwen-turbo-latest'

    DASHSCOPE_QWEN2_5_14B_INSTRUCT_1M = 'qwen2.5-14b-instruct-1m'
    DASHSCOPE_QWEN2_5_7B_INSTRUCT_1M = 'qwen2.5-7b-instruct-1m'
    DASHSCOPE_QWEN2_5_72B_INSTRUCT = 'qwen2.5-72b-instruct'
    DASHSCOPE_QWEN2_5_32B_INSTRUCT = 'qwen2.5-32b-instruct'
    DASHSCOPE_QWEN2_5_14B_INSTRUCT = 'qwen2.5-14b-instruct'
    DASHSCOPE_QWEN2_5_7B_INSTRUCT = 'qwen2.5-7b-instruct'

    DASHSCOPE_QWEN1_5_110B_CHAT = 'qwen1.5-110b-chat'
    DASHSCOPE_QWEN1_5_72B_CHAT = 'qwen1.5-72b-chat'
    DASHSCOPE_QWEN1_5_32B_CHAT = 'qwen1.5-32b-chat'
    DASHSCOPE_QWEN1_5_14B_CHAT = 'qwen1.5-14b-chat'
    DASHSCOPE_QWEN1_5_7B_CHAT = 'qwen1.5-7b-chat'


VALID_LLM_MODELS = {
    LLMType.OPENAI: [
        ModelType.OPENAI_GPT3_5_TURBO_0125,
        ModelType.OPENAI_GPT4_0314,
        ModelType.OPENAI_GPT4_32K_0314,
        ModelType.OPENAI_GPT4_0613,
        ModelType.OPENAI_GPT4_32K_0613,
        ModelType.OPENAI_GPT4_O,
        ModelType.OPENAI_GPT4_O_MINI,
        ModelType.OPENAI_O1,
        ModelType.OPENAI_O3,
        ModelType.OPENAI_O4_MINI,
        ModelType.OPENAI_O3_MINI,
        ModelType.OPENAI_O1_MINI
    ],
    LLMType.ANTHROPIC: [
        ModelType.ANTHROPIC_CLAUDE_3_5_SONNET,
        ModelType.ANTHROPIC_CLAUDE_3_7_SONNET,
        ModelType.ANTHROPIC_CLAUDE_3_SONNET,
        ModelType.ANTHROPIC_CLAUDE_3_5_HAIKU,
        ModelType.ANTHROPIC_CLAUDE_3_HAIKU,
        ModelType.ANTHROPIC_CLAUDE_3_OPUS
    ],
    LLMType.GOOGLE_AI: [
        ModelType.GOOGLEAI_GEMINI_1_5_FLASH,
        ModelType.GOOGLEAI_GEMINI_1_5_PRO,
        ModelType.GOOGLEAI_GEMINI_1_5_FLASH_8B,
        ModelType.GOOGLEAI_GEMINI_2_0_FLASH,
        ModelType.GOOGLEAI_GEMINI_2_0_FLASH_THINKING_EXP_0121,
        ModelType.GOOGLEAI_GEMINI_2_0_FLASH_LITE_PREVIEW_0205,
        ModelType.GOOGLEAI_GEMINI_2_0_PRO_EXP,
        ModelType.GOOGLEAI_GEMINI_2_5_PRO_EXP,
        ModelType.GOOGLEAI_GEMINI_2_5_PRO_PREVIEW,
        ModelType.GOOGLEAI_GEMINI_2_5_FLASH_PREVIEW,
        ModelType.GOOGLEAI_GEMINI_1_5_FLASH_SCHEMA_PRUNING_FT
    ],
    LLMType.DEEPSEEK: [
        ModelType.DEEPSEEK_CHAT,
        ModelType.DEEPSEEK_REASONER
    ],
    LLMType.DASHSCOPE: [
        ModelType.DASHSCOPE_QWEN_MAX,
        ModelType.DASHSCOPE_QWEN_PLUS,
        ModelType.DASHSCOPE_QWEN_TURBO,
        ModelType.DASHSCOPE_QWEN2_5_14B_INSTRUCT_1M,
        ModelType.DASHSCOPE_QWEN2_5_7B_INSTRUCT_1M,
        ModelType.DASHSCOPE_QWEN2_5_72B_INSTRUCT,
        ModelType.DASHSCOPE_QWEN2_5_32B_INSTRUCT,
        ModelType.DASHSCOPE_QWEN2_5_14B_INSTRUCT,
        ModelType.DASHSCOPE_QWEN2_5_7B_INSTRUCT,
        ModelType.DASHSCOPE_QWEN1_5_110B_CHAT,
        ModelType.DASHSCOPE_QWEN1_5_72B_CHAT,
        ModelType.DASHSCOPE_QWEN1_5_32B_CHAT,
        ModelType.DASHSCOPE_QWEN1_5_14B_CHAT,
        ModelType.DASHSCOPE_QWEN1_5_7B_CHAT
    ]
}


@dataclass
class LLMConfig:
    type: LLMType
    model: ModelType
    temperature: float = 0.2
    max_tokens: int = 8192

    def to_client_args(self) -> dict[str, Any]:
        """Convert this config to kwargs for the ClientFactory."""
        return asdict(self)


MODEL_COST = {
    # Pricing per 1K tokens
    ModelType.OPENAI_GPT3_5_TURBO_0125: { 
        "input": 0.0005,
        "output": 0.0015,
        "batch_input": 0.00025,
        "batch_output": 0.00075,
    },
    ModelType.OPENAI_GPT4_0314: {  
        "input": 0.03, 
        "output": 0.06,
        "batch_input": 0.015,
        "batch_output": 0.03,
    },
    ModelType.OPENAI_GPT4_32K_0314: { 
        "input": 0.06, 
        "output": 0.12,
        "batch_input": 0.03,
        "batch_output": 0.06,
    },
    ModelType.OPENAI_GPT4_0613: { 
        "input": 0.03, 
        "output": 0.06,
        "batch_input": 0.015,
        "batch_output": 0.03,
    },
    ModelType.OPENAI_GPT4_32K_0613: {
        "input": 0.06, 
        "output": 0.12,
        "batch_input": 0.03,
        "batch_output": 0.06,
    },
    ModelType.OPENAI_GPT4_O: { 
        "input": 0.0025,
        "output": 0.01,
        "cached_input": 0.00125,
        "batch_input": 0.00125,
        "batch_output": 0.005,
    },
    ModelType.OPENAI_GPT4_O_MINI: {
        "input": 0.00015,
        "output": 0.0006,
        "cached_input": 0.000075,
        "batch_input": 0.000075,
        "batch_output": 0.0003,
    },
}