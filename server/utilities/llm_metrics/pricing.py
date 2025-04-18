from utilities.constants.LLM_enums import LLMType, ModelType

# Pricing per 1K tokens
PRICING = {
    LLMType.OPENAI: {
        ModelType.OPENAI_GPT3_5_TURBO_0125: {
            "input": 0.0005,
            "output": 0.0015,
        },
        ModelType.OPENAI_GPT4_0314: {
            "input": 0.03,
            "output": 0.06,
        },
        ModelType.OPENAI_GPT4_32K_0314: {
            "input": 0.06,
            "output": 0.12,
        },
        ModelType.OPENAI_GPT4_0613: {
            "input": 0.03,
            "output": 0.06,
        },
        ModelType.OPENAI_GPT4_32K_0613: {
            "input": 0.06,
            "output": 0.12,
        },
        ModelType.OPENAI_GPT4_O: {
            "input": 0.0025,
            "output": 0.01,
        },
        ModelType.OPENAI_GPT4_O_MINI: {
            "input": 0.00015,
            "output": 0.0006,
        },
        ModelType.OPENAI_O1: {
            "input": 0.015,
            "output": 0.06,
        },
        ModelType.OPENAI_O3_MINI: {
            "input": 0.0011,
            "output": 0.0044,
        },
        ModelType.OPENAI_O1_MINI: {
            "input": 0.0011,
            "output": 0.0044,
        },
    },
    LLMType.GOOGLE_AI: {
        ModelType.GOOGLEAI_GEMINI_1_5_FLASH: {
            "input": 0.000075,
            "output": 0.0003,
        },
        ModelType.GOOGLEAI_GEMINI_1_5_PRO: {
            "input": 0.00125,
            "output": 0.005,
        },
        ModelType.GOOGLEAI_GEMINI_1_5_FLASH_8B: {
            "input": 0.000035,
            "output": 0.00015,
        },
        ModelType.GOOGLEAI_GEMINI_2_0_FLASH: {
            "input": 0.0001,
            "output": 0.0004,
        },
        ModelType.GOOGLEAI_GEMINI_2_0_FLASH_THINKING_EXP_0121: {
            "input": 0.0, # free for now
            "output": 0.0,
        },
        ModelType.GOOGLEAI_GEMINI_2_0_FLASH_LITE_PREVIEW_0205: {
            "input": 0.000075,
            "output": 0.0003,
        },
        ModelType.GOOGLEAI_GEMINI_2_0_PRO_EXP: {
            "input": 0.0, # free for now
            "output": 0.0,
        },
        ModelType.GOOGLEAI_GEMINI_2_5_PRO_EXP: {
            "input": 0.0, # free for now
            "output": 0.0,
        },
        ModelType.GOOGLEAI_GEMINI_2_5_PRO_PREVIEW: {    
            "input": 0.00125, # Asumption: prompts <= 200k tokens
            "output": 0.01, # Asumption: prompts <= 200k tokens
        },
        ModelType.GOOGLEAI_GEMINI_2_5_FLASH_PREVIEW: {
            "input": 0.00015,
            "output": 0.0006, # Asumption: model is Non-thinking
        },
        ModelType.GOOGLEAI_GEMINI_1_5_FLASH_SCHEMA_PRUNING_FT: {
            "input": 0.00035,
            "output": 0.0015,
        },
    },
    LLMType.ANTHROPIC: {
        ModelType.ANTHROPIC_CLAUDE_3_5_SONNET: {
            "input": 0.003,
            "output": 0.015,
        },
        ModelType.ANTHROPIC_CLAUDE_3_7_SONNET: {
            "input": 0.003,
            "output": 0.015,
        },
        ModelType.ANTHROPIC_CLAUDE_3_SONNET: {
            "input": 0.003,
            "output": 0.015,
        },
        ModelType.ANTHROPIC_CLAUDE_3_5_HAIKU: {
            "input": 0.0008,
            "output": 0.004,
        },
        ModelType.ANTHROPIC_CLAUDE_3_HAIKU: {
            "input": 0.00025,
            "output": 0.00125,
        },
        ModelType.ANTHROPIC_CLAUDE_3_OPUS: {
            "input": 0.015,
            "output": 0.075,
        }
    },
    LLMType.DASHSCOPE: {
        ModelType.DASHSCOPE_QWEN_MAX: {
            "input": 0.0016,
            "output": 0.0064,
        },
        ModelType.DASHSCOPE_QWEN_PLUS: {
            "input": 0.0004,
            "output": 0.0012,
        },
        ModelType.DASHSCOPE_QWEN_TURBO: {
            "input": 0.0,
            "output": 0.0,
        },
        ModelType.DASHSCOPE_QWEN2_5_14B_INSTRUCT_1M: {
            "input": 0.0,
            "output": 0.0,
        },
        ModelType.DASHSCOPE_QWEN2_5_7B_INSTRUCT_1M: {
            "input": 0.0,
            "output": 0.0,
        },
        ModelType.DASHSCOPE_QWEN2_5_72B_INSTRUCT: {
            "input": 0.0,
            "output": 0.0,
        },
        ModelType.DASHSCOPE_QWEN2_5_32B_INSTRUCT: {
            "input": 0.0,
            "output": 0.0,
        },
        ModelType.DASHSCOPE_QWEN2_5_14B_INSTRUCT: {
            "input": 0.0,
            "output": 0.0,
        },
        ModelType.DASHSCOPE_QWEN2_5_7B_INSTRUCT: {
            "input": 0.0,
            "output": 0.0,
        },
        ModelType.DASHSCOPE_QWEN1_5_110B_CHAT: {
            "input": 0.0,
            "output": 0.0,
        },
        ModelType.DASHSCOPE_QWEN1_5_72B_CHAT: {
            "input": 0.0,
            "output": 0.0,
        },
        ModelType.DASHSCOPE_QWEN1_5_32B_CHAT: {
            "input": 0.0,
            "output": 0.0,
        },
        ModelType.DASHSCOPE_QWEN1_5_14B_CHAT: {
            "input": 0.0,
            "output": 0.0,
        },
        ModelType.DASHSCOPE_QWEN1_5_7B_CHAT: {
            "input": 0.0,
            "output": 0.0,
        },
    },
    LLMType.DEEPSEEK: {
        ModelType.DEEPSEEK_CHAT: {
            "input": 0.00027, # assuming cache miss
            "output": 0.0011,
        },
        ModelType.DEEPSEEK_REASONER: {
            "input": 0.00055, # assuming cache miss
            "output": 0.00219,
        },
    }
}