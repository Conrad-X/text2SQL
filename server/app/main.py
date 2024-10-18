from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from typing import List

from app import db
from app.request_schema import *

from services.client_factory import ClientFactory

from utilities.utility_functions import * 
from utilities.constants.LLM_enums import LLMType, ModelType
from utilities.constants.prompts_enums import PromptType
from utilities.constants.response_messages import ERROR_QUESTION_REQUIRED, ERROR_SHOTS_REQUIRED, ERROR_NON_NEGATIVE_SHOTS_REQUIRED, ERROR_ZERO_SHOTS_REQUIRED
from utilities.prompts.prompt_factory import PromptFactory
from utilities.config import DatabaseConfig
from utilities.vectorize import vectorize_data_samples, fetch_few_shots

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"], 
    allow_headers=["*"], 
)

@app.get("/test_vector_store")
async def test():
    vectorize_data_samples()
    fetch_few_shots(3, "Show all hotels")

@app.post("/queries/generate-and-execute/")
async def generate_and_execute_sql_query(body: QueryGenerationRequest):
    question = body.question
    prompt_type = body.prompt_type
    shots = body.shots
    llm_type = body.llm_type
    model = body.model
    temperature = body.temperature
    max_tokens = body.max_tokens

    if prompt_type in {PromptType.FULL_INFORMATION, PromptType.SQL_ONLY, PromptType.DAIL_SQL} and shots is None:
        raise HTTPException(status_code=400, detail=ERROR_SHOTS_REQUIRED)
    
    if prompt_type not in {PromptType.FULL_INFORMATION, PromptType.SQL_ONLY, PromptType.DAIL_SQL} and shots > 0:
        raise HTTPException(status_code=400, detail=ERROR_ZERO_SHOTS_REQUIRED)
     
    sql_query = ''  
    result = ''  

    try:       
        prompt = PromptFactory.get_prompt_class(prompt_type=prompt_type, target_question=question, shots=shots)

        validate_llm_and_model(llm_type=llm_type, model=model)
        client = ClientFactory.get_client(type=llm_type, model=model, temperature=temperature, max_tokens=max_tokens)

        sql_query = client.execute_prompt(prompt=prompt)
        connection = sqlite3.connect(DatabaseConfig.DATABASE_URL)
        result = execute_sql_query(connection, sql_query=sql_query)

        return {
            "result": result,
            "query": sql_query,
            "prompt_used": prompt
        }
        
    except Exception as e:
        error_detail = {
            "error": str(e),
            "result": result,
            "query": sql_query  
        }
        raise HTTPException(status_code=400, detail=error_detail)

# Function for testing purposes only
@app.post("/queries/generate-and-execute-for-prompts/", response_model=List[QueryExecutionResponse])
async def execute_query_for_prompts(body: QuestionRequest):
    question = body.question
    shots = body.shots

    if not question:
        raise HTTPException(status_code=400, detail=ERROR_QUESTION_REQUIRED)
    if shots < 0:
        raise HTTPException(status_code=400, detail=ERROR_NON_NEGATIVE_SHOTS_REQUIRED)

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

@app.post("/masking/question-and-query/")
def mask_single_question_and_query(request: MaskRequest):
    try:
        table_and_column_names = get_array_of_table_and_column_name(DatabaseConfig.DATABASE_URL)
        
        masked_question = mask_question(request.question, table_and_column_names=table_and_column_names)
        masked_query = mask_sql_query(request.sql_query)

        return {
            "masked_question": masked_question,
            "masked_sql_query": masked_query
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/masking/file/")
def mask_question_and_answer_file_by_filename(request: MaskFileRequest):
    try:
        table_and_column_names = get_array_of_table_and_column_name(DatabaseConfig.DATABASE_URL)
        masked_file_name = mask_question_and_answer_files(file_name=request.file_name, table_and_column_names=table_and_column_names)

        return {
            "masked_file_name": masked_file_name
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/prompts/generate/")
async def generate_prompt(request: PromptGenerationRequest):
    prompt_type = request.prompt_type
    shots = request.shots
    question = request.question

    if shots < 0:
        raise HTTPException(status_code=400, detail=ERROR_NON_NEGATIVE_SHOTS_REQUIRED)
    
    try:
        prompt = PromptFactory.get_prompt_class(prompt_type=prompt_type, target_question=question, shots=shots)
        return {"generated_prompt": prompt}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/database/change/")
async def change_database(request: ChangeDatabaseRequest):
    try:
        db.set_database(request.database_type)
        schema = format_schema(FormatType.CODE, DatabaseConfig.DATABASE_URL)
        return {"database_type": DatabaseConfig.ACTIVE_DATABASE.value, "schema": schema}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/database/schema/")
async def get_database_schema():
    try:
        schema = format_schema(FormatType.CODE, DatabaseConfig.DATABASE_URL)
        return {"database_type": DatabaseConfig.ACTIVE_DATABASE.value, "schema": schema}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
