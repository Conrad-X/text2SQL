from pydantic import BaseModel
from typing import Optional, Any
from utilities.constants.LLM_enums import LLMType, ModelType
from utilities.constants.prompts_enums import PromptType
from utilities.constants.database_enums import DatabaseType
from utilities.config import DatabaseConfig

# Pydantic models for request body validation
class QueryGenerationRequest(BaseModel):
    question: str = "List all Stores"
    prompt_type: PromptType = PromptType.DAIL_SQL
    shots: Optional[int] = 3
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
    file_name: str = f'{DatabaseConfig.ACTIVE_DATABASE.value}_schema.json'

class PromptGenerationRequest(BaseModel):
    prompt_type: PromptType
    shots: Optional[int] = 0
    question: str

class ChangeDatabaseRequest(BaseModel):
    database_type: DatabaseType