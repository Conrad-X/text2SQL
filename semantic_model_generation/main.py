from snowflake_connector import SnowflakeConnector
import pandas as pd
from typing import List, Optional
import sqlite3
from generate_raw_model import (
    generate_model_str_from_snowflake
)
import os
from dotenv import load_dotenv

from relationships import (
    get_relationships
)

from config import(
    SAMPLE_VALUES,
    ADD_JOINS
)

load_dotenv()

def fetch_tables_views_in_schema(
    conn, schema_name: str
) -> list[str]:
    """
    Fetches all tables and views that the current user has access to in the current schema
    Args:
        conn: SnowflakeConnectioserver/scripts/evaluate_cortex_analyst.pyn to run the query
        schema_name: The name of the schema to connect to.

    Returns: a list of fully qualified table names.
    """
    query = f"show tables in schema {schema_name};"
    cursor = conn.cursor()
    cursor.execute(query)
    tables = cursor.fetchall()
    # Each row in the result has columns (created_on, table_name, database_name, schema_name, ...)
    results = [f"{result[2]}.{result[3]}.{result[1]}" for result in tables]

    query = f"show views in schema {schema_name};"
    cursor = conn.cursor()
    cursor.execute(query)
    views = cursor.fetchall()
    # Each row in the result has columns (created_on, view_name, reserved, database_name, schema_name, ...)
    results += [f"{result[3]}.{result[4]}.{result[1]}" for result in views]

    return results

if __name__ == "__main__":
    
    snowflake_db_name="BIRD_TRAIN"
    snowflake_schema="RETAILS"
    file_path='./semantic_model.yaml'
    sqlite_database='retails'


    db_path=f'../server/data/bird/train/train_databases/{sqlite_database}/{sqlite_database}.sqlite'
    sample_path=f'../server/data/bird/train/train_databases/{sqlite_database}/samples/unmasked_{sqlite_database}.json'

    # make snowflake connection
    snowflake_connector = SnowflakeConnector()
    conn = snowflake_connector.snowflake_connection(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse="COMPUTE_WH",
        role="ACCOUNTADMIN",
        host=os.getenv("SNOWFLAKE_HOST"),
        database=snowflake_db_name
    )

    # get tables and views from the DB
    tables=fetch_tables_views_in_schema(conn, snowflake_schema)

    # generate semantic model
    semantic_model_string=generate_model_str_from_snowflake(
        base_tables = tables,
        semantic_model_name = "temp",
        conn = conn,
        n_sample_values=5,
        allow_joins=ADD_JOINS,
        db_path=db_path,
        sample_path=sample_path
    )

    # save semantic model
    with open(file_path, 'w') as file:
        file.write(semantic_model_string)
        file.close()