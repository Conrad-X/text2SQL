import json
import time
import re

from app.db import set_database
from utilities.logging_utils import setup_logger
from services.client_factory import ClientFactory
from utilities.config import DatabaseConfig
from utilities.vectorize import fetch_similar_columns
from utilities.schema_linking.extract_keyword import (
    get_keywords_from_question,
    get_keywords_using_LLM,
)
from utilities.schema_linking.value_retrieval import get_table_column_of_similar_values
from utilities.constants.LLM_enums import LLMType, ModelType
from utilities.constants.prompts_enums import FormatType
from utilities.prompts.prompt_templates import SCHEMA_SELECTOR_PROMPT_TEMPLATE
from utilities.utility_functions import format_schema
from utilities.constants.script_constants import GOOGLE_RESOURCE_EXHAUSTED_EXCEPTION_STR

logger = setup_logger(__name__)

def get_relevant_tables_and_columns(
    query: str,
    evidence: str,
    n_description: int,
    n_value: int,
    database_name: str,
    llm_config: dict = None,
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

    if llm_config is not None:
        keywords = get_keywords_using_LLM(
            query,
            evidence,
            llm_config.get('LLMType'),
            llm_config.get('ModelType'),
            llm_config.get('temperature', 0.7),
            llm_config.get('max_tokens', 8000),
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

def select_relevant_schema(database_name: str, query: str, evidence: str, pipeline_args: dict):
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
    set_database(database_name)

    if pipeline_args is not None:
        schema = get_relevant_tables_and_columns(query, evidence,n_description=pipeline_args["n_description"], n_value=pipeline_args["n_value"], database_name=database_name, llm_config=pipeline_args["llm_config"])  
    else:
        schema = None
    
    # Pass the schema (or None) to the format_schema function.
    formatted_schema = format_schema(FormatType.M_SCHEMA, DatabaseConfig.DATABASE_URL, schema)
    
    # Format the prompt with the formatted schema, query, and evidence.
    prompt = SCHEMA_SELECTOR_PROMPT_TEMPLATE.format(database_schema=formatted_schema, question=query, hint=evidence)
    
    # Get the client.
    client = ClientFactory.get_client(
        LLMType.GOOGLE_AI,
        ModelType.GOOGLEAI_GEMINI_2_0_FLASH_THINKING_EXP_1219,
        0.2,
        8000
    )

    # Execute the prompt
    final_schema = ""
    while not final_schema:
        try:
            final_schema = client.execute_prompt(prompt=prompt)
        except Exception as e:
            if GOOGLE_RESOURCE_EXHAUSTED_EXCEPTION_STR in str(e):
                logger.warning("Quota exhausted. Retrying in 5 seconds...")
                time.sleep(5)
            else:
                logger.error(f"Unhandled exception: {e}")
    
    # Remove markdown formatting if present and parse the JSON.
    final_schema = re.sub(r"```json\s*([\s\S]*?)```", r"\1", final_schema)
    final_schema = json.loads(final_schema)

    # Remove the chain-of-thought key if it exists.
    final_schema.pop("chain_of_thought_reasoning", None)

    return final_schema