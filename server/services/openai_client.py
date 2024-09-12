from openai import OpenAI
from dotenv import load_dotenv
import os

from utilities.prompts.database_schema_representation import *

load_dotenv()

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
        f"### Given the following database schema :\n {code_representation_database_schema}\n"
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