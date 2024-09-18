from enum import Enum

class LLMType(Enum):
    OPENAI = "openai"
    ANTHROPIC = "anthropic"

class ModelType(Enum):
    # OpenAI Models
    OPENAI_GPT4_O = "gpt-4o"
    OPENAI_GPT4_O_MINI = "gpt-4o-mini"

    # Anthropic Models
    ANTHROPIC_CLAUDE_3_5_SONNET = "claude-3-5-sonnet-20240620" 
    ANTHROPIC_CLAUDE_3_SONNET = "claude-3-sonnet-20240229"
    ANTHROPIC_CLAUDE_3_HAIKU = "claude-3-haiku-20240307"

VALID_LLM_MODELS = {
    LLMType.OPENAI: [
        ModelType.OPENAI_GPT4_O,
        ModelType.OPENAI_GPT4_O_MINI
    ],
    LLMType.ANTHROPIC: [
        ModelType.ANTHROPIC_CLAUDE_3_5_SONNET,
        ModelType.ANTHROPIC_CLAUDE_3_SONNET,
        ModelType.ANTHROPIC_CLAUDE_3_HAIKU
    ]
}
