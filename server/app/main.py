from fastapi import FastAPI, HTTPException, Depends

from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import Optional

from app import db

from services.client_factory import ClientFactory

from utilities.utility_functions import * 
from utilities.constants.LLM_enums import LLMType, ModelType
from utilities.constants.prompts_enums import PromptType
from utilities.constants.response_messages import ERROR_QUESTION_REQUIRED, ERROR_SHOTS_REQUIRED
from utilities.prompts.prompt_factory import PromptFactory

app = FastAPI()

# Pydantic models for request body validation
class QueryGenerationRequest(BaseModel):
    question: str
    prompt_type: PromptType = PromptType.SQL_ONLY
    shots: Optional[int] = None
    llm_type: Optional[LLMType] = LLMType.OPENAI
    model: Optional[ModelType] = ModelType.OPENAI_GPT4_O_MINI
    temperature: Optional[float] = 0.7
    max_tokens: Optional[int] = 1000

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
        #result = execute_sql_query(sql_query=sql_query)

        response = {
            #"result": result,
            "query": sql_query,
            "prompt_used": prompt
        }
        
        return response
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
