from fastapi import FastAPI, HTTPException, Depends

from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import Optional

from app import db

from services.client_factory import ClientFactory

from utilities.utility_functions import * 
from utilities.constants.database_schema_representation import * 
from utilities.constants.LLM_config import LLMType, ModelType
from utilities.constants.message_templates import ERROR_QUESTION_REQUIRED

app = FastAPI()

# Pydantic models for request body validation
class QueryRequest(BaseModel):
    query: str

class QueryGenerationRequest(BaseModel):
    question: str
    llm_type: Optional[LLMType] = LLMType.ANTHROPIC 
    model: Optional[ModelType] = ModelType.GPT4_O 
    temperature: Optional[float] = 0.7
    max_tokens: Optional[int] = 1000

@app.post("/generate_and_execute_sql_query/")
async def generate_and_execute_sql_query(body: QueryGenerationRequest, db: Session = Depends(db.get_db)):
    question = body.question
    llm_type = body.llm_type
    model = body.model
    temperature = body.temperature
    max_tokens = body.max_tokens

    if not question:
        raise HTTPException(status_code=400, detail=ERROR_QUESTION_REQUIRED)
    
    try:
        validate_llm_and_model(llm_type=llm_type, model=model)
        client = ClientFactory.get_client(type=llm_type, model=model, temperature=temperature, max_tokens=max_tokens)

        client.set_prompt(code_representation_database_schema, question) 
        sql_query = client.execute_prompt()
        result = execute_sql_query(db, sql_query)

        response = {
            "result": result,
            "query": sql_query,
            "prompt_used": client.prompt
        }
        
        return response
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
