from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy.orm import Session
from app import utils, db
from pydantic import BaseModel

app = FastAPI()

# Pydantic models for request body validation
class QueryRequest(BaseModel):
    query: str

class QuestionRequest(BaseModel):
    question: str

@app.post("/execute_sql_query/")
async def execute_sql_query(body: QueryRequest, db: Session = Depends(db.get_db)):
    """
    Execute a provided SQL query on the database.
    ---
    - **query**: SQL query string (required)
    """
    sql_query = body.query

    if not sql_query:
        raise HTTPException(status_code=400, detail="Query parameter is required")

    try:
        # Run the SQL execution in a thread pool to prevent blocking
        result = utils.execute_sql_query(db, sql_query)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/generate_and_execute_sql_query_openai/")
async def generate_and_execute_sql_query_openai(body: QuestionRequest, db: Session = Depends(db.get_db)):
    """
    Generate an SQL query from a natural language question and execute it using openai.
    ---
    - **question**: Natural language question (required)
    """
    question = body.question

    if not question:
        raise HTTPException(status_code=400, detail="Question parameter is required")

    try:
        sql_query, prompt_used = utils.generate_sql_query_openai(question)
        result = utils.execute_sql_query(db, sql_query)

        return {"result": result, "query": sql_query, "prompt_used": prompt_used}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    
@app.post("/generate_and_execute_sql_query_anthropic/")
async def generate_and_execute_sql_query_anthropic(body: QuestionRequest, db: Session = Depends(db.get_db)):
    """
    Generate an SQL query from a natural language question and execute it using anthropic.
    ---
    - **question**: Natural language question (required)
    """
    question = body.question

    if not question:
        raise HTTPException(status_code=400, detail="Question parameter is required")

    try:
        sql_query, prompt_used = utils.generate_sql_query_anthrophic(question)
        result = utils.execute_sql_query(db, sql_query)

        return {"result": result, "query": sql_query, "prompt_used": prompt_used}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
