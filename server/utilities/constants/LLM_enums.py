from enum import Enum

class LLMType(Enum):
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    GOOGLE_AI = "google_ai"  
    DEEPSEEK = "deepseek"

class ModelType(Enum):
    # OpenAI Models
    OPENAI_GPT3_5_TURBO_0125 = "gpt-3.5-turbo-0125"
    OPENAI_GPT4_0314 = "gpt-4-0314"
    OPENAI_GPT4_32K_0314 = "gpt-4-32k-0314"
    OPENAI_GPT4_0613 = "gpt-4-0613"
    OPENAI_GPT4_32K_0613 = "gpt-4-32k-0613"
    OPENAI_GPT4_O = "gpt-4o-2024-08-06"
    OPENAI_GPT4_O_MINI = "gpt-4o-mini-2024-07-18"

    # Anthropic Models
    ANTHROPIC_CLAUDE_3_5_SONNET = "claude-3-5-sonnet-20240620" 
    ANTHROPIC_CLAUDE_3_SONNET = "claude-3-sonnet-20240229"
    ANTHROPIC_CLAUDE_3_HAIKU = "claude-3-haiku-20240307"
    
    # Google AI Models
    GOOGLEAI_GEMINI_1_5_FLASH = "gemini-1.5-flash"
    GOOGLEAI_GEMINI_1_5_PRO = "gemini-1.5-pro"
    GOOGLEAI_GEMINI_1_5_FLASH_8B = "gemini-1.5-flash-8b"
    GOOGLEAI_GEMINI_2_0_FLASH_EXP = "gemini-2.0-flash-exp"
    GOOGLEAI_GEMINI_2_0_FLASH_THINKING_EXP_1219 = "gemini-2.0-flash-thinking-exp-1219"

    #DeepSeek Models
    DEEPSEEK_CHAT='deepseek-chat'
    DEEPSEEK_REASONER='deepseek-reasoner'


VALID_LLM_MODELS = {
    LLMType.OPENAI: [
        ModelType.OPENAI_GPT3_5_TURBO_0125,
        ModelType.OPENAI_GPT4_0314,
        ModelType.OPENAI_GPT4_32K_0314,
        ModelType.OPENAI_GPT4_0613,
        ModelType.OPENAI_GPT4_32K_0613,
        ModelType.OPENAI_GPT4_O,
        ModelType.OPENAI_GPT4_O_MINI
    ],
    LLMType.ANTHROPIC: [
        ModelType.ANTHROPIC_CLAUDE_3_5_SONNET,
        ModelType.ANTHROPIC_CLAUDE_3_SONNET,
        ModelType.ANTHROPIC_CLAUDE_3_HAIKU
    ],
    LLMType.GOOGLE_AI: [
        ModelType.GOOGLEAI_GEMINI_1_5_FLASH,
        ModelType.GOOGLEAI_GEMINI_1_5_PRO,
        ModelType.GOOGLEAI_GEMINI_1_5_FLASH_8B,
        ModelType.GOOGLEAI_GEMINI_2_0_FLASH_EXP,
        ModelType.GOOGLEAI_GEMINI_2_0_FLASH_THINKING_EXP_1219
    ],
    LLMType.DEEPSEEK: [
        ModelType.DEEPSEEK_CHAT,
        ModelType.DEEPSEEK_REASONER
    ]
}

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