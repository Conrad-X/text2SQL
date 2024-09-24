from fastapi import FastAPI, HTTPException, Depends

from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import Optional, List, Any
import textwrap

from app import db

from services.client_factory import ClientFactory

from utilities.utility_functions import * 
from utilities.constants.LLM_enums import LLMType, ModelType
from utilities.constants.prompts_enums import PromptType, FormatType
from utilities.constants.response_messages import ERROR_QUESTION_REQUIRED, ERROR_SHOTS_REQUIRED
from utilities.prompts.prompt_factory import PromptFactory

app = FastAPI()

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
    question: str
    shots: int

class QueryExecutionResponse(BaseModel):
    prompt_type: PromptType
    query: str
    execution_result: Any
    prompt: str
    error: Optional[str] = None

@app.post("/generate_and_execute_sql_query/")
async def generate_and_execute_sql_query(body: QueryGenerationRequest, db: Session = Depends(db.get_db)):
    question = body.question
    prompt_type = body.prompt_type
    shots = body.shots
    llm_type = body.llm_type
    model = body.model
    temperature = body.temperature
    max_tokens = body.max_tokens

    if not question:
        raise HTTPException(status_code=400, detail=ERROR_QUESTION_REQUIRED)
    
    if prompt_type in {PromptType.FULL_INFORMATION, PromptType.SQL_ONLY, PromptType.DAIL_SQL} and shots is None:
        raise HTTPException(status_code=400, detail=ERROR_SHOTS_REQUIRED)
     
    try:       
        prompt = PromptFactory.get_prompt_class(prompt_type=prompt_type, target_question=question, shots=shots)

        validate_llm_and_model(llm_type=llm_type, model=model)
        client = ClientFactory.get_client(type=llm_type, model=model, temperature=temperature, max_tokens=max_tokens)

        sql_query = client.execute_prompt(prompt=prompt)
        result = execute_sql_query(sql_query=sql_query)

        response = {
            "result": result,
            "query": sql_query,
            "prompt_used": prompt
        }
        
        return response
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Function for testing purposes only
@app.post("/execute_query_for_prompts/", response_model=List[QueryExecutionResponse])
async def execute_query_for_prompts(body: QuestionRequest, db: Session = Depends(db.get_db)):
    question = body.question
    shots = body.shots

    if not question:
        raise HTTPException(status_code=400, detail=ERROR_QUESTION_REQUIRED)
    if shots < 0:
        raise HTTPException(status_code=400, detail=ERROR_SHOTS_REQUIRED)

    prompt_types = []
    if shots == 0:
        prompt_types = [PromptType.OPENAI_DEMO, PromptType.CODE_REPRESENTATION]
    else:
        prompt_types = [PromptType.DAIL_SQL, PromptType.FULL_INFORMATION, PromptType.SQL_ONLY]

    responses = []

    for prompt_type in prompt_types:
        try:
            prompt = PromptFactory.get_prompt_class(prompt_type=prompt_type, target_question=question, shots=shots)

            llm_type = LLMType.OPENAI
            model = ModelType.OPENAI_GPT4_O_MINI
            client = ClientFactory.get_client(type=llm_type, model=model, temperature=0.7, max_tokens=1000)

            sql_query = client.execute_prompt(prompt=prompt)

            formatted_query = sql_query.strip()
            formatted_prompt = prompt.strip()
            # Printing these in terminal as it is easier to copy paste to sheet formatted
            print("Query:\n", formatted_query)
            print("\nPrompt:\n", formatted_prompt)

            result = execute_sql_query(sql_query=sql_query)

            responses.append(QueryExecutionResponse(
                prompt_type=prompt_type,
                query=sql_query,
                execution_result=result,
                prompt=prompt,
                error=None
            ))
        except Exception as e:
            responses.append(QueryExecutionResponse(
                prompt_type=prompt_type,
                query=sql_query,
                execution_result=[], 
                prompt=prompt,
                error=str(e)  
            ))
            
    return responses if responses else []

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
