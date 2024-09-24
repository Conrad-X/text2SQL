import sqlite3

from utilities.constants.response_messages import (
    ERROR_DATABASE_QUERY_FAILURE, 
    ERROR_SQL_QUERY_REQUIRED, 
    ERROR_INVALID_MODEL_FOR_TYPE,
    ERROR_UNSUPPORTED_CLIENT_TYPE
)

from utilities.constants.LLM_enums import LLMType, ModelType, VALID_LLM_MODELS
from utilities.constants.prompts_enums import FormatType

from app.db import DATABASE_URL

def execute_sql_query(connection: sqlite3.Connection = sqlite3.connect(DATABASE_URL), sql_query: str = ""):
    """
    Executes a SQL query and returns the results as a list of dictionaries.
    """
    if not sql_query:
        raise ValueError(ERROR_SQL_QUERY_REQUIRED)

    try:
        cursor = connection.cursor()
        cursor.execute(sql_query)
        columns = [description[0] for description in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
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


def get_table_names(connection: sqlite3.Connection):
    """
    Retrieves the names of all tables in the SQLite database.
    """
    query = "SELECT name FROM sqlite_master WHERE type='table';"
    try:
        result = execute_sql_query(connection, query)
        return [row['name'] for row in result]
    except Exception as e:
        raise RuntimeError(f"Failed to fetch table names: {str(e)}")

def get_table_columns(connection: sqlite3.Connection, table_name: str):
    """
    Fetches the column names for a given table in the SQLite database.
    """
    query = f"PRAGMA table_info('{table_name}')"
    cursor = connection.cursor()
    cursor.execute(query)
    return [row[1] for row in cursor.fetchall()]

def format_schema(format_type: FormatType):
    """
    Formats the database schema based on the specified format type.
    """
    connection = sqlite3.connect(DATABASE_URL)
    
    try:
        table_names = get_table_names(connection)
        filtered_table_names = [name for name in table_names if "alembic" not in name.lower() and "index" not in name.lower()]
        formatted_schema = []

        for table in filtered_table_names:
            columns = get_table_columns(connection, table)

            if format_type == FormatType.BASIC:
                # Format: Table table_name, columns = [ col1, col2, col3 ]
                formatted_schema.append(f"Table {table}, columns = [ {', '.join(columns)} ]")
            elif format_type == FormatType.TEXT:
                # Format: table_name: col1, col2, col3
                formatted_schema.append(f"{table}: {', '.join(columns)}")
            elif format_type == FormatType.CODE:
                # Format in SQL create table form
                cursor = connection.cursor()
                cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table}';")
                create_table_sql = cursor.fetchone()
                formatted_schema.append(create_table_sql[0] if create_table_sql else f"-- Missing SQL for {table}")
            elif format_type == FormatType.OPENAI:
                # Format in OpenAI demo style: # table_name ( col1, col2, col3 )
                formatted_schema.append(f"# {table} ( {', '.join(columns)} )")
            else:
                raise ValueError(f"Unsupported format type: {format_type}")

        return "\n".join(formatted_schema)
    finally:
        connection.close()
