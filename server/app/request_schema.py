from pydantic import BaseModel
from typing import Optional, Any
from utilities.constants.LLM_enums import LLMType, ModelType
from utilities.constants.prompts_enums import PromptType
from utilities.constants.database_enums import DatabaseType
from utilities.config import DatabaseConfig, UNMASKED_SAMPLE_DATA_FILE_PATH

# Pydantic models for request body validation
class QueryGenerationRequest(BaseModel):
    question: str
    prompt_type: PromptType
    shots: Optional[int]
    llm_type: Optional[LLMType] = LLMType.ANTHROPIC
    model: Optional[ModelType] = ModelType.ANTHROPIC_CLAUDE_3_5_SONNET
    temperature: Optional[float] = 0.7
    max_tokens: Optional[int] = 1000

class QuestionRequest(BaseModel):
    question: str = 'List all tables'
    shots: int = 0

class QueryExecutionResponse(BaseModel):
    prompt_type: PromptType
    query: str
    execution_result: Any
    prompt: str
    error: Optional[str] = None

class MaskRequest(BaseModel):
    question: str
    sql_query: str

class MaskFileRequest(BaseModel):
    database_name: str = DatabaseConfig.ACTIVE_DATABASE

class PromptGenerationRequest(BaseModel):
    prompt_type: PromptType
    shots: Optional[int] = 0
    question: str

class ChangeDatabaseRequest(BaseModel):
    database_type: str
    sample_path: Optional[str] = None

class BatchJobRequest(BaseModel):
    prompt_type: PromptType = PromptType.DAIL_SQL
    shots: int = 5
    model: ModelType = ModelType.OPENAI_GPT4_O_MINI
    temperature: float = 0.7
    max_tokens: int = 1000
    database_name: str = DatabaseConfig.ACTIVE_DATABASE