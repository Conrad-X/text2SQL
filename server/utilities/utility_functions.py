from sqlalchemy import text
from sqlalchemy.orm import Session

from utilities.constants.message_templates import (
    ERROR_DATABASE_QUERY_FAILURE, 
    ERROR_SQL_QUERY_REQUIRED, 
    ERROR_INVALID_MODEL_FOR_TYPE,
    ERROR_UNSUPPORTED_CLIENT_TYPE
)

from utilities.constants.LLM_config import LLMType, ModelType, VALID_LLM_MODELS

def execute_sql_query(db: Session, sql_query: str):
    """
    Executes a SQL query and returns the results as a list of dictionaries.
    """
    if not sql_query:
        raise ValueError(ERROR_SQL_QUERY_REQUIRED)

    try:
        result = db.execute(text(sql_query))
        columns = result.keys()
        rows = [dict(zip(columns, row)) for row in result.fetchall()]
        return rows
    except Exception as e:
        raise RuntimeError(ERROR_DATABASE_QUERY_FAILURE.format(error=str(e)))

def validate_llm_and_model(llm_type: LLMType, model: ModelType):
    """
    Validates that the model corresponds to the LLM type.
    """
    if llm_type not in VALID_LLM_MODELS:
        raise ValueError(ERROR_UNSUPPORTED_CLIENT_TYPE)

    if model not in VALID_LLM_MODELS[llm_type]:
        raise ValueError(ERROR_INVALID_MODEL_FOR_TYPE.format(model=model.value, llm_type=llm_type.value))
