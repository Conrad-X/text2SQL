from anthropic import Anthropic
from dotenv import load_dotenv
import os

from utilities.prompts.database_schema_representation import *

load_dotenv()

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
        f"### Given the following database schema :\n {code_representation_database_schema}\n"
        f"### Answer the following: {question}\nSELECT*/"
    )

    if not question:
        raise ValueError("Question parameter is required")

    try:
        client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
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