from collections import defaultdict
from utilities.vectorize import fetch_similar_columns
from utilities.schema_linking.extract_keyword import (
    get_keywords_from_question,
    get_keywords_using_LLM,
)
from utilities.schema_linking.value_retrieval import get_table_column_of_similar_values
from utilities.constants.LLM_enums import LLMType, ModelType


def get_relevant_tables_and_columns(
    query: str,
    evidence: str,
    n_description: int,
    n_value: int,
    database_name: str,
    use_llm: bool,
    LLMType: LLMType = None,
    ModelType: ModelType = None,
    temperature: float = 0.7,
    max_tokens: int = 8000,
) -> dict:
    """
    This function takes a str as input and returns a dictionary of relevant tables and columns from the current Database.
    """

    if use_llm:
        keywords = get_keywords_using_LLM(
            query, evidence, LLMType, ModelType, temperature, max_tokens
        )
    else:
        keywords = get_keywords_from_question(query, evidence)

    schema = {}
    similar_columns = {}
    value_columns = {}

    similar_columns = fetch_similar_columns(n_description, keywords, database_name)
    value_columns = get_table_column_of_similar_values(keywords, n_value, database_name)

    for table, columns in similar_columns.items():
        if table not in schema:
            schema[table] = set()
        schema[table].update(columns)

    for table, columns in value_columns.items():
        if table not in schema:
            schema[table] = set()
        schema[table].update(columns)

    return schema
