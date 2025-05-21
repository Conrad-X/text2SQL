from typing import List

from app import db
from app.request_schema import *
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from services.clients.client_factory import ClientFactory
from services.validators.model_validator import validate_llm_and_model
from utilities.config import PATH_CONFIG
from utilities.constants.prompts_enums import FormatType, PromptType
from utilities.constants.response_messages import (
    ERROR_NON_NEGATIVE_SHOTS_REQUIRED, ERROR_QUESTION_REQUIRED,
    ERROR_SHOTS_REQUIRED, ERROR_ZERO_SHOTS_REQUIRED)
from utilities.constants.services.llm_enums import (LLMConfig, LLMType,
                                                    ModelType)
from utilities.cost_estimation import *
from utilities.format_schema import format_schema
from utilities.prompts.prompt_factory import PromptFactory
from utilities.utility_functions import *

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"], 
    allow_headers=["*"], 
)

@app.get("/test_cost_estimations")
async def test_cost_estimations():
    results = {}
    try:
        file_path = PATH_CONFIG.batch_input_path()
        total_tokens, total_cost, warnings = calculate_cost_and_tokens_for_file(file_path=file_path, model=ModelType.OPENAI_GPT4_O, is_batched=False)
        
        results['total_tokens'] = total_tokens
        results['total_cost'] = total_cost
        results['warnings'] = warnings

        example_messages = [
            {
                "role": "system",
                "content": "You are a helpful, pattern-following assistant that translates corporate jargon into plain English.",
            },
            {
                "role": "system",
                "name": "example_user",
                "content": "New synergies will help drive top-line growth.",
            },
            {
                "role": "system",
                "name": "example_assistant",
                "content": "Things working well together will increase revenue.",
            },
            {
                "role": "system",
                "name": "example_user",
                "content": "Let's circle back when we have more bandwidth to touch base on opportunities for increased leverage.",
            },
            {
                "role": "system",
                "name": "example_assistant",
                "content": "Let's talk later when we're less busy about how to do better.",
            },
            {
                "role": "user",
                "content": "This late pivot means we don't have time to boil the ocean for the client deliverable.",
            },
        ]

        single_example_tokens, warnings = validate_and_calculate_token_count(model=ModelType.OPENAI_GPT4_O, messages=example_messages)
        results['single_example_tokens'] = single_example_tokens

        average_output_tokens = calculate_average_output_tokens_for_all_samples(model=ModelType.OPENAI_GPT4_O)
        results['average_output_tokens'] = average_output_tokens

        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    
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

        llm_config = LLMConfig(
            llm_type=llm_type,
            model_type=model,
            temperature=temperature,
            max_tokens=max_tokens
        )

        validate_llm_and_model(llm_config.llm_type, llm_config.model_type)
        client = ClientFactory.get_client(llm_config)

        sql_query = client.execute_prompt(prompt=prompt)
        connection = sqlite3.connect(PATH_CONFIG.sqlite_path())
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

            llm_config = LLMConfig(
                llm_type=LLMType.OPENAI,
                model_type=ModelType.OPENAI_GPT4_O_MINI,
                temperature=0.7,
                max_tokens=1000
            )
            client = ClientFactory.get_client(llm_config)

            sql_query = client.execute_prompt(prompt=prompt)

            formatted_query = sql_query.strip()
            formatted_prompt = prompt.strip()
            # Printing these in terminal as it is easier to copy paste to sheet formatted
            print("Query:\n", formatted_query)
            print("\nPrompt:\n", formatted_prompt)
            connection = sqlite3.connect(PATH_CONFIG.sqlite_path())
            result = execute_sql_query(connection = connection, sql_query=sql_query)

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
        table_and_column_names = get_array_of_table_and_column_name(PATH_CONFIG.sqlite_path())
        
        masked_question = mask_question(request.question, table_and_column_names=table_and_column_names)
        masked_query = mask_sql_query(request.sql_query)

        return {
            "masked_question": masked_question,
            "masked_sql_query": masked_query
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
        db.set_database(request.database_name)
        schema = format_schema(FormatType.CODE, database_name=request.database_name)
        return {"database_type": PATH_CONFIG.database_name, "schema": schema}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/database/schema/")
async def get_database_schema():
    try:
        schema = format_schema(FormatType.CODE, PATH_CONFIG.database_name)
        return {"database_type": PATH_CONFIG.database_name, "schema": schema}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
