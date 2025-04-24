import json
import sqlite3
import time
import re
from typing import Dict, List, Tuple
from datasketch import MinHash, MinHashLSH

from services.base_client import Client
from utilities.logging_utils import setup_logger
from utilities.vectorize import fetch_similar_columns
from utilities.schema_linking.extract_keyword import (
    get_keywords_from_question,
    get_keywords_using_LLM,
)
from utilities.schema_linking.value_retrieval import get_table_column_of_similar_values
from utilities.constants.prompts_enums import FormatType
from utilities.prompts.prompt_templates import SCHEMA_SELECTOR_PROMPT_TEMPLATE
from utilities.utility_functions import format_schema, get_table_foreign_keys
from utilities.constants.script_constants import GOOGLE_RESOURCE_EXHAUSTED_EXCEPTION_STR
from services.base_client import Client

logger = setup_logger(__name__)


def get_relevant_tables_and_columns(
    query: str,
    evidence: str,
    n_description: int,
    n_value: int,
    database_name: str,
    lsh: MinHashLSH,
    minhash:Dict[str, Tuple[MinHash, str, str, str]],
    keyword_extraction_client: Client = None,
) -> dict:
    """
    Retrieves a dictionary of relevant tables and columns from the database
    based on the given query, evidence, and optional LLM configuration.

    Args:
        query (str): The target question
        evidence (str): Evidence/Hint with the target question.
        n_description (int): Number of similar columns to fetch based on descriptions.
        n_value (int): Number of columns to fetch based on similar values.
        database_name (str): The name of the database to query.
        llm_config (dict, optional): Configuration for the LLM, including type,
            model, temperature, and max tokens. Defaults to None.

    Returns:
        dict: A dictionary where keys are table names and values are sets of
        column names relevant to the query.
    """

    if keyword_extraction_client is not None:
        keywords = get_keywords_using_LLM(
            query,
            evidence,
            keyword_extraction_client,
        )
    else:
        keywords = get_keywords_from_question(query, evidence)

    schema = {}
    similar_columns = {}
    value_columns = {}

    similar_columns = fetch_similar_columns(n_description, keywords, database_name)
    value_columns = get_table_column_of_similar_values(keywords, n_value, lsh, minhash)

    for table, columns in similar_columns.items():
        if table not in schema:
            schema[table] = set()
        schema[table].update(columns)

    for table, columns in value_columns.items():
        if table not in schema:
            schema[table] = set()
        schema[table].update(columns)

    return schema


def select_relevant_schema(
    database_name: str, query: str, evidence: str, schema_selector_client: Client ,pipeline_args: dict
):
    """
    Selects the relevant schema from the database based on the given query,
    evidence, and pipeline arguments.

    Args:
        database_name (str): The name of the database to query.
        query (str): The query string to analyze.
        evidence (str): Additional evidence to consider for keyword extraction.
        pipeline_args (dict): A dictionary containing parameters for the LLM
            pipeline, including n_description and n_value for fetching similar
            columns and columns with similar values, and llm_config for the LLM
            configuration.

    Returns:
        dict: A dictionary where keys are table names and values are sets of
        column names relevant to the query.
    """

    if pipeline_args is not None:
        schema = get_relevant_tables_and_columns(
            query,
            evidence,
            n_description=pipeline_args["n_description"],
            n_value=pipeline_args["n_value"],
            database_name=database_name,
            lsh=pipeline_args["lsh"],
            minhash=pipeline_args["minhash"],
            keyword_extraction_client=pipeline_args["keyword_extraction_client"],
        )
    else:
        schema = None

    # Pass the schema (or None) to the format_schema function.
    formatted_schema = format_schema(
        FormatType.M_SCHEMA, database_name, schema
    )

    # Format the prompt with the formatted schema, query, and evidence.
    prompt = SCHEMA_SELECTOR_PROMPT_TEMPLATE.format(
        database_schema=formatted_schema, question=query, hint=evidence
    )

    # Execute the prompt
    final_schema = None
    while not final_schema:
        try:
            final_schema = schema_selector_client.execute_prompt(prompt=prompt)
            
            # Remove markdown formatting if present and parse the JSON.
            final_schema = re.sub(r"```json\s*([\s\S]*?)```", r"\1", final_schema)
            final_schema = json.loads(final_schema)

        except Exception as e:
            if GOOGLE_RESOURCE_EXHAUSTED_EXCEPTION_STR in str(e):
                logger.warning("Quota exhausted. Retrying in 5 seconds...")
                time.sleep(5)
            else:
                logger.error(f"Unhandled exception: {e}")
                final_schema = None

    # Remove the chain-of-thought key if it exists.
    final_schema.pop("chain_of_thought_reasoning", None)
    final_schema = {
            table_name: list(columns.keys())
            for table_name, columns in final_schema.get("tables", {}).items()
        }

    return final_schema


def is_token_mentioned(token: str, text: str) -> bool:
    """Checks if the given token is mentioned in the given text"""

    token_lower = token.lower()

    if re.search(r'[^a-zA-Z0-9]', token_lower):
        # If token has special characters, use direct substring check
        return token_lower in text.lower()
    else:   
        # Use regex to check for whole word match
        token_regex_pattern = r'\b' + re.escape(token_lower) + r'\b'
        return bool(re.search(token_regex_pattern, text.lower()))


def extract_mentioned_schema_elements_from_text(schema: Dict[str, List[str]], text: str) -> Dict[str, List[str]]:
    """Extracts schema elements mentioned in the given text through string comparisons given the schema"""
    
    # Dictionary to store mentioned schema elements in the format { table1: [col1, col2], table2: [col3] ... }
    mentioned_schema = {}
    text_lower = text.lower()  # convert to lowercase for case-insensitive comparison

    for table, columns in schema.items():
        matched_columns = [
            col for col in columns if is_token_mentioned(col, text_lower)
        ]

        if is_token_mentioned(table, text_lower) or matched_columns:
            mentioned_schema[table] = matched_columns

    return mentioned_schema


def include_referenced_fk_columns_in_schema(schema: Dict[str, List[str]], database_path: str) -> Dict[str, List[str]]:
    """If any column in the schema is a foreign key in another table, it includes the referenced table/column in the schema."""

    with sqlite3.connect(database_path) as connection:
        connection.row_factory = sqlite3.Row

        for table in list(schema.keys()):
            foreign_keys = get_table_foreign_keys(connection, table)

            for fk in foreign_keys:
                to_table = fk["to_table"]
                from_col = fk["from_column"]
                to_col = fk["to_column"]

                # Proceed only if the foreign key column is in the pruned schema
                if from_col in schema.get(table, []):
                    if to_table not in schema:
                        schema[to_table] = []

                    if to_col not in schema[to_table]:
                        schema[to_table].append(to_col)

    return schema