from sqlalchemy import text
from sqlalchemy.orm import Session
from openai import OpenAI
import anthropic
from dotenv import load_dotenv
import os

from app.database_schema import database_schema

load_dotenv()

def execute_sql_query(db: Session, sql_query: str):
    """
    Executes a SQL query and returns the results as a list of dictionaries.

    Args:
        db (Session): SQLAlchemy session object.
        sql_query (str): SQL query string.

    Returns:
        List[dict]: Query results as a list of dictionaries where each dictionary represents a row.

    Raises:
        ValueError: If sql_query is empty.
        RuntimeError: If there is a database query error.
    """
    if not sql_query:
        raise ValueError("Query parameter is required")

    try:
        result = db.execute(text(sql_query))
        columns = result.keys()
        rows = [dict(zip(columns, row)) for row in result.fetchall()]
        return rows
    except Exception as e:
        raise RuntimeError(f"Database query error: {str(e)}")


def generate_sql_query_openai(question: str):
    """
    Generates an SQL query from a natural language question using the OpenAI API.

    Args:
        question (str): Natural language question.

    Returns:
        tuple: A tuple containing the generated SQL query string and the prompt used.

    Raises:
        RuntimeError: If there is an error with the OpenAI API request.
    """
    prompt = (
        f"### Complete sqlite SQL query only and with no explanation\n"
        f"### Given the following database schema :\n {database_schema}\n"
        f"### Answer the following: {question}\nSELECT*/"
    )

    if not question:
        raise ValueError("Question parameter is required")

    try:
        client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=150,
            temperature=0.5,
            stop=[";"],
        )
        sql_query = response.choices[0].message.content.strip('```sql\n')
        return sql_query, prompt
    except Exception as e:
        raise RuntimeError(f"OpenAI API error: {str(e)}")

def generate_sql_query_anthrophic(question: str):
    """
    Generates an SQL query from a natural language question using the OpenAI API.

    Args:
        question (str): Natural language question.

    Returns:
        tuple: A tuple containing the generated SQL query string and the prompt used.

    Raises:
        RuntimeError: If there is an error with the OpenAI API request.
    """
    prompt = (
        f"### Complete sqlite SQL query only and with no explanation\n"
        f"### Given the following database schema :\n {database_schema}\n"
        f"### Answer the following: {question}\nSELECT*/"
    )

    if not question:
        raise ValueError("Question parameter is required")

    try:
        client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        response = client.messages.create(
            model="claude-3-5-sonnet-20240620",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=150,
            temperature=0.5,
        )
        print(response)
        sql_query = response.content[0].text 
        return sql_query, prompt
    except Exception as e:
        raise RuntimeError(f"OpenAI API error: {str(e)}")