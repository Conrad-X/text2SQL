import sqlite3
from nltk import word_tokenize, pos_tag
from nltk.corpus import wordnet
import re
import json
import os

from utilities.constants.response_messages import (
    ERROR_DATABASE_QUERY_FAILURE, 
    ERROR_SQL_QUERY_REQUIRED, 
    ERROR_INVALID_MODEL_FOR_TYPE,
    ERROR_UNSUPPORTED_CLIENT_TYPE,
    ERROR_SQL_MASKING_FAILED,
    ERROR_FILE_MASKING_FAILED,
    ERROR_UNSUPPORTED_FORMAT_TYPE,
    ERROR_FAILED_FETCH_COLUMN_NAMES,
    ERROR_FAILED_FETCH_TABLE_NAMES
)

from utilities.constants.LLM_enums import LLMType, ModelType, VALID_LLM_MODELS
from utilities.constants.prompts_enums import FormatType
from utilities.config import DatabaseConfig, MASKED_SAMPLE_DATA_FILE_PATH, UNMASKED_SAMPLE_DATA_FILE_PATH

def execute_sql_query(connection: sqlite3.Connection, sql_query: str):
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
        raise RuntimeError((ERROR_FAILED_FETCH_TABLE_NAMES.format(error=str(e))))

def get_table_columns(connection: sqlite3.Connection, table_name: str):
    """
    Fetches the column names for a given table in the SQLite database.
    """
    try: 
        query = f"PRAGMA table_info('{table_name}')"
        cursor = connection.cursor()
        cursor.execute(query)
        return [row[1] for row in cursor.fetchall()]
    except Exception as e:
        raise RuntimeError((ERROR_FAILED_FETCH_COLUMN_NAMES.format(error=str(e))))

    
def get_array_of_table_and_column_name(database_path:str):
    try:
        connection = sqlite3.connect(database_path)
        connection.row_factory = sqlite3.Row

        table_names = get_table_names(connection)
        column_names = []
        for table_name in table_names:
            column_names.extend(get_table_columns(connection, table_name))

        return table_names + column_names
    finally:
        connection.close()

def format_schema(format_type: FormatType, db_path: str):
    """
    Formats the database schema based on the specified format type.
    """
    connection = sqlite3.connect(db_path)
    
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
                raise ValueError((ERROR_UNSUPPORTED_FORMAT_TYPE.format(format_type=format_type)))
        return "\n".join(formatted_schema)
    finally:
        connection.close()

def convert_word_to_singular_form(word) -> str:
    singular_word = wordnet.morphy(word)
    return singular_word if singular_word else word

def mask_question(question: str, table_and_column_names: list, mask_tag: str = '<mask>', value_tag: str = '<unk>') -> str:
    """
    Masks specified table and column names with mask tag and other values with value tag in a question.
    """
    tokens = word_tokenize(question)
    pos_tags = pos_tag(tokens)

    table_and_column_names = set(convert_word_to_singular_form(name.lower()) for name in table_and_column_names)

    masked_question = []

    for word, tag in pos_tags:
        word_lower = word.lower()
        
        if tag in ['NNS', 'NNPS']:
            word_lower = convert_word_to_singular_form(word_lower)

        is_table_or_column = any(word_lower in tab for tab in table_and_column_names)
        is_numeric_value = word.isdigit() or word.replace('.', '', 1).isdigit()
        is_noun_or_adjective = tag in ['NN', 'NNS', 'NNP', 'NNPS', 'JJ']
        is_stop_word = tag in ['DT', 'CC', 'IN', 'WP', 'PRP$']

        if is_table_or_column and not is_stop_word:
            masked_question.append(mask_tag)
        elif is_noun_or_adjective and not is_table_or_column or is_numeric_value:
            masked_question.append(value_tag)
        else:
            masked_question.append(word)

    return ' '.join(masked_question)

def mask_sql_query(sql_query: str, mask_tag: str = '<mask>', value_tag: str = '<unk>') -> str:
    """
    Masks table names, column names, and values/integers in a SQL query.
    """
    sql_keywords = r'\b(select|from|where|and|or|insert|update|delete|set|values|' \
                   r'join|on|group by|order by|having|limit|distinct|as|inner|' \
                   r'left|right|full|cross|natural|outer|with|concat|' \
                   r'sum|avg|count|min|max|group_concat|like)\b'
    
    table_column_pattern = rf'\b(?!{sql_keywords})\w+\b'
    value_pattern = r'\'[^\']*\'|\d+(\.\d+)?'
    combined_pattern = rf'({table_column_pattern})|({value_pattern})'

    def apply_mask(matched_text):
        matched_string = matched_text.group(0)
        return mask_tag if re.match(table_column_pattern, matched_string) else value_tag

    try:
        masked_query = re.sub(combined_pattern, apply_mask, sql_query, flags=re.IGNORECASE)
        return masked_query
    except Exception as e:
        raise ValueError(ERROR_SQL_MASKING_FAILED.format(error=e))

def mask_question_and_answer_files(database_name: str, table_and_column_names: list, mask_tag: str = '<mask>', value_tag: str = '<unk>'):
    """
    Reads a JSON file containing questions and answers, applies masking to both the question and SQL query,
    and saves the masked result in a new JSON file with the prefix 'masked_' and returns the maked file name
    """
    try:
        with open(UNMASKED_SAMPLE_DATA_FILE_PATH.format(database_name=database_name), 'r') as file:
            data = json.load(file)

        masked_data = []

        for item in data:
            question = item['question']
            answer = item['answer']

            masked_question = mask_question(question, table_and_column_names, mask_tag, value_tag)
            masked_answer = mask_sql_query(answer, mask_tag, value_tag)

            masked_data.append({
                "id": item.get("id"),
                "masked_question": masked_question,
                "masked_answer": masked_answer
            })
        
        with open(MASKED_SAMPLE_DATA_FILE_PATH.format(database_name = database_name), 'w') as masked_file:
            json.dump(masked_data, masked_file, indent=4)

        return MASKED_SAMPLE_DATA_FILE_PATH.format(database_name = database_name)

    except Exception as e:
        raise ValueError(ERROR_FILE_MASKING_FAILED.format(error=e))