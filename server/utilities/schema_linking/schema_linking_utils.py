from collections import defaultdict
from utilities.vectorize import fetch_similar_columns
from utilities.schema_linking.extract_keyword import get_keywords_from_question
from utilities.schema_linking.value_retrieval import get_table_column_of_similar_values

def get_relevant_tables_and_columns(query: str, n_description: int, n_value: int, database_name: str) -> dict:
    """
    This function takes a str as input and returns a dictionary of relevant tables and columns from the current Database.
    """
    keywords = get_keywords_from_question(query)
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