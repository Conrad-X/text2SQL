"""
This script is for control and checking purposes to ensure a baseline prediction with only a single prompt being used 
This will aid in futher comparisons with the text2sql solution.

Results are stored in:
    1. "minimal_sql_predictions.json"
    2. "sql_eval_results.json"

Last checked results on this showed 73% accuracy on 200 queries
"""

import json
import os
import time
import sqlite3

import pandas as pd
from app import db
from utilities.config import PATH_CONFIG
from utilities.constants.database_enums import DatasetType
from utilities.constants.LLM_enums import LLMType, ModelType
from utilities.constants.prompts_enums import FormatType, PromptType
from utilities.constants.script_constants import GOOGLE_RESOURCE_EXHAUSTED_EXCEPTION_STR
from utilities.prompts.prompt_factory import PromptFactory
from services.client_factory import ClientFactory
from utilities.utility_functions import format_sql_response
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
database_name = 'test'
dataset_type = DatasetType.WIKI_TEST

DESCRIPTION_FOLDER = str(PATH_CONFIG.description_dir(database_name=database_name, dataset_type=dataset_type))
print(DESCRIPTION_FOLDER)

# --- Load Descriptions Once ---
def load_descriptions():
    """Load all table and column descriptions into memory for fast access."""
    table_desc_path = os.path.join(DESCRIPTION_FOLDER, "test_tables.csv")
    table_desc_df = pd.read_csv(table_desc_path)

    # Map of table name -> table description
    table_descriptions = dict(zip(table_desc_df['table_name'], table_desc_df['table_description']))

    # Map of table name -> dataframe with detailed schema
    schema_descriptions = {}
    for file in os.listdir(DESCRIPTION_FOLDER):
        if file != "test.csv" and file.endswith(".csv"):
            table_name = file.replace(".csv", "")
            df = pd.read_csv(os.path.join(DESCRIPTION_FOLDER, file))
            schema_descriptions[table_name] = df

    return table_descriptions, schema_descriptions

# Enhance schema with descriptions
def build_enhanced_schema(table_name, raw_schema, table_descriptions, schema_descriptions):
    description_parts = []
    table_desc = table_descriptions.get(table_name, "")
    if table_desc:
        description_parts.append(f"Table `{table_name}`: {table_desc}")

    columns_df = schema_descriptions.get(table_name)
    example_row = {}
    if columns_df is not None:
        for _, row in columns_df.iterrows():
            col_name = row.get("column_name", "")
            col_desc = row.get("improved_column_description", row.get("column_description", ""))
            value_desc = row.get("value_description", "")
            
            # Extract example value
            example_value = ""
            if "is:" in value_desc:
                parts = value_desc.split("is:")
                if len(parts) > 1:
                    example_value = parts[1].split("and")[0].strip().strip("'").strip('"')
            
            if col_name:
                if col_desc:
                    description_parts.append(f"- `{col_name}`: {col_desc.strip()}")
                if example_value:
                    example_row[col_name] = example_value

    # Add example row table (if data is available)
    example_row_str = ""
    if example_row:
        headers = " | ".join(example_row.keys())
        values = " | ".join(example_row.values())
        example_row_str = f"\n\nExample row:\n| {headers} |\n| {' | '.join(['-' * len(h) for h in example_row.keys()])} |\n| {values} |"

    full_schema = f"{raw_schema}\n\n" + "\n".join(description_parts) + example_row_str
    return full_schema

# Updated function
def basic_generate_sql(question, schema, database_name):
    client = ClientFactory.get_client(
        LLMType.GOOGLE_AI, 
        ModelType.GOOGLEAI_GEMINI_2_0_FLASH,
        0.2, 
        4096
    )

    prompt = f"""
You are a SQL generator for the database '{database_name}'.

Use the **actual schema column names**, such as 'col0', 'col1', etc., in the query.
The original column names and descriptions are provided below only to help you understand their meaning.

Question: "{question}"

Schema with descriptions:
{schema}

Important:
- Always use internal column names (e.g., col0, col1) in your query.
- Do not use original names for the columns like 'Average Population' or 'Nationality' in the SQL.
- Use examples to understand the formatting for string literals where appropriate.
- Use case-insensitive matching when filtering text (e.g., use LOWER(colX) = 'value').

Only return a single valid SQL query.
"""

    sql = ""
    retry_count = 0
    while sql == "" and retry_count < 5:
        try:
            response = client.execute_prompt(prompt)
            sql = format_sql_response(response)
            return sql
        except Exception as e:
            if GOOGLE_RESOURCE_EXHAUSTED_EXCEPTION_STR in str(e):
                retry_count += 1
                logger.warning(f"Quota exhausted. Retrying in 5 seconds... (attempt {retry_count}/5)")
                time.sleep(5)
            else:
                logger.error(f"Error: {e}")
                return f"-- ERROR: {str(e)} --"
    return "-- ERROR: Failed after maximum retries --"



# Execute SQL on SQLite database
def execute_sql(database_path, query):
    try:
        conn = sqlite3.connect(database_path)
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        conn.close()
        return result
    except Exception as e:
        return f"ERROR: {e}"

# Main processing function
def process_items_minimal(limit=100):
    output_file = "minimal_sql_predictions.json"
    eval_output_path = "sql_eval_results.json"

    predictions = {}
    eval_data = []

    if os.path.exists(output_file):
        with open(output_file, 'r') as f:
            predictions = json.load(f)

    if os.path.exists(eval_output_path):
        with open(eval_output_path, "r") as f:
            eval_data = json.load(f)

    with open(PATH_CONFIG.processed_test_path(dataset_type=DatasetType.WIKI_TEST), "r") as f:
        test_data = json.load(f)

    test_data = test_data[:limit]
    to_process = [item for item in test_data if str(item["question_id"]) not in predictions]
    logger.info(f"Processing {len(to_process)} out of {limit} items")

    # Load schema and table descriptions
    table_desc_map, schema_desc_map = load_descriptions()

    for idx, item in enumerate(to_process):
        question_id = str(item["question_id"])
        database = item['db_id']
        table_name = item['table_name']
        question = item['question']
        gold_sql = item.get("SQL", "")

        db.set_database(database)

        logger.info(f"Processing item {idx+1}/{len(to_process)}: {question_id}")

        raw_schema = item.get("schema_used", "")
        full_schema = build_enhanced_schema(table_name, raw_schema, table_desc_map, schema_desc_map)

        predicted_sql = basic_generate_sql(
            question=question,
            schema=full_schema,
            database_name=database
        )

        predictions[question_id] = f"{predicted_sql}\t----- bird -----\t{database}"

        # Save predictions
        with open(output_file, 'w') as f:
            json.dump(predictions, f)

        standard_format = {int(k): v for k, v in predictions.items()}
        with open(PATH_CONFIG.formatted_predictions_path(global_file=True), 'w') as f:
            json.dump(standard_format, f)

        # Evaluate SQLs
        database_path = os.path.join(PATH_CONFIG.sqlite_path(dataset_type=DatasetType.WIKI_TEST))

        gold_result = execute_sql(database_path, gold_sql)
        predicted_result = execute_sql(database_path, predicted_sql)
        is_correct = gold_result == predicted_result

        eval_entry = {
            "question_id": question_id,
            "question": question,
            "table": database,
            "predicted_sql": predicted_sql,
            "gold_sql": gold_sql,
            "predicted_result": predicted_result,
            "gold_result": gold_result,
            "is_correct": is_correct
        }

        eval_data.append(eval_entry)

        with open(eval_output_path, "w") as f:
            json.dump(eval_data, f, indent=2)

        logger.info(f"Completed {idx+1}/{len(to_process)}. Waiting before next item...")
        time.sleep(5)

    logger.info(f"Processing complete! Generated {len(predictions)} predictions.")
    return predictions

if __name__ == "__main__":
    """
    This script processes results via a singular prompt and saves 2 files
    It is used for the purpose of a 'control' where we can ensure the text2sql ssolution performs at or above par this technique of just calling an llm.

    1. output_file = "minimal_sql_predictions.json"
    this contains the predicted SQL queries after a single call to gemini

    2. eval_output_path = "sql_eval_results.json"
    This contains the comparisions of the different queries for futher analysis on where the predictions are failing.
    """
    process_items_minimal(1000)
