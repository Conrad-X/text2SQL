from enum import Enum

class LLMType(Enum):
    OPENAI = "openai"
    ANTHROPIC = "anthropic"

class ModelType(Enum):
    # OpenAI Models
    GPT4_O = "gpt-4o"
    GPT4_O_MINI = "gpt-4o-mini"

    # Anthropic Models
    CLAUDE_3_5_SONNET = "claude-3-5-sonnet-20240620" 
    CLAUDE_3_SONNET = "claude-3-sonnet-20240229"
    CLAUDE_3_HAIKU = "claude-3-haiku-20240307"

VALID_LLM_MODELS = {
    LLMType.OPENAI: [
        ModelType.GPT4_O,
        ModelType.GPT4_O_MINI
    ],
    LLMType.ANTHROPIC: [
        ModelType.CLAUDE_3_5_SONNET,
        ModelType.CLAUDE_3_SONNET,
        ModelType.CLAUDE_3_HAIKU
    ]
}
