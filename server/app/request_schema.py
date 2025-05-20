from typing import Any, Optional

from pydantic import BaseModel
from utilities.config import PATH_CONFIG
from utilities.constants.database_enums import DatabaseType
from utilities.constants.services.llm_enums import LLMType, ModelType
from utilities.constants.prompts_enums import PromptType


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
    database_name: str = PATH_CONFIG.database_name

class PromptGenerationRequest(BaseModel):
    prompt_type: PromptType
    shots: Optional[int] = 0
    question: str

class ChangeDatabaseRequest(BaseModel):
    database_name: str
    sample_path: Optional[str] = None

class BatchJobRequest(BaseModel):
    prompt_type: PromptType = PromptType.DAIL_SQL
    shots: int = 5
    model: ModelType = ModelType.OPENAI_GPT4_O_MINI
    temperature: float = 0.7
    max_tokens: int = 1000
    database_name: str = PATH_CONFIG.database_name