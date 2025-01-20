import os
import pandas as pd
import sqlite3
import time
from tqdm import tqdm

from utilities.logging_utils import setup_logger
from utilities.prompts.prompt_templates import COLUMN_DESCRIPTION_PROMPT_TEMPLATE, TABLE_DESCRIPTION_PROMPT_TEMPLATE
from utilities.constants.prompts_enums import FormatType
from utilities.config import (
    DATASET_DESCRIPTION_PATH,
    DATASET_DIR,
    DATABASE_SQLITE_PATH,
)
from utilities.utility_functions import format_schema, get_table_columns, get_table_names
from utilities.constants.LLM_enums import LLMType, ModelType
from utilities.constants.script_constants import (
    UNKNOWN_COLUMN_DATA_TYPE_STR,
    GOOGLE_RESOURCE_EXHAUSTED_EXCEPTION_STR
)
from services.client_factory import ClientFactory

logger = setup_logger(__name__)

def read_csv(file_path, encodings=["utf-8-sig", "ISO-8859-1"]):
    """ Tries to read a CSV file using multiple encodings. If all encodings fail, raises an error. """
    
    for encoding in encodings:
        try:
            return pd.read_csv(file_path, encoding=encoding)
        except (UnicodeDecodeError, Exception) as e:
            logger.warning(f"Failed with encoding {encoding}: {e}. Trying next...")
    
    raise ValueError(f"All encoding attempts failed for {file_path}")

def extract_column_type_from_schema(connection, table_name, column_name):
    """ Fetches the column data type from a SQLite table schema. """
    
    # Fetch the table schema from SQLite
    cursor = connection.cursor()
    cursor.execute(f"PRAGMA table_info({table_name});")
    columns = cursor.fetchall()

    # Search for the column_name in the fetched schema
    for column in columns:
        if column[1].lower() == column_name.lower():
            column_type = column[2]  # column[2] contains the data type
            return column_type

    return UNKNOWN_COLUMN_DATA_TYPE_STR


def get_imrpoved_coloumn_description(row, table_name, first_row, connection, client, table_description, errors, database_name):
    """ Process and generate improved column description. """
    
    column_name = str(row["original_column_name"]).strip() 
    column_type = (
        row["data_format"] if pd.notna(row["data_format"]) 
        else extract_column_type_from_schema(connection, table_name, column_name)
    )
    column_description = row["column_description"] if pd.notna(row["column_description"]) else ""
    column_comment_part = f"Column description: {column_description}\n" if column_description else ""
    
    prompt = COLUMN_DESCRIPTION_PROMPT_TEMPLATE.format(
        table_name=table_name,
        table_description=table_description,
        first_row=first_row,
        column_name=column_name,
        datatype=column_type,
        column_comment_part=column_comment_part
    )
    
    improved_description = ""
    while improved_description == "":
        try:
            improved_description = client.execute_prompt(prompt)
        except Exception as e:
            if GOOGLE_RESOURCE_EXHAUSTED_EXCEPTION_STR in str(e):
                # Rate limit exceeded: Too many requests. Retrying in 5 seconds...
                time.sleep(5)
            else:
                errors.append({
                    'database': database_name,
                    'error': f"Column '{row['original_column_name']}' does not exist. Please check {table_name}.csv."
                })
                break
    
    return improved_description

def get_first_row(connection, table_name):
    """Fetch the first row from the specified table."""
    
    cursor = connection.cursor()
    cursor.execute(f'SELECT * FROM "{table_name}" LIMIT 1')
    first_row = cursor.fetchone()
    return [str(value) if value is not None else "N/A" for value in first_row] if first_row else []


def improve_column_descriptions(database_name, client):
    """ Improve column descriptions for a specific database. """
    
    errors = []
    try:
        connection = sqlite3.connect(
            DATABASE_SQLITE_PATH.format(database_name=database_name)
        )

        base_path = DATASET_DESCRIPTION_PATH.format(database_name=database_name)
        table_description_csv_path = os.path.join(
            base_path, f"{database_name}_tables.csv"
        )  # TO DO: Update file path as a constant after finalizing path organization.

        table_description_df = pd.read_csv(table_description_csv_path)
        tables_in_database = get_table_names(connection)

        for table_csv in os.listdir(base_path):
            # Skip the Overall Tables Description File
            if table_csv == f"{database_name}_tables.csv":
                continue

            if table_csv.endswith(".csv"):
                table_name = table_csv.split(".csv")[0]
                if table_name not in tables_in_database:
                    errors.append({
                        'database': database_name,
                        'error': f"Table '{table_name}' does not exist in the SQLite database. Please check {table_csv}"
                    })
                    logger.error(f"Table '{table_name}' does not exist in the SQLite database. Please check {table_csv}")
                    continue

                table_df = read_csv(os.path.join(base_path, table_csv))

                # Fetch the first row of data for the table
                first_row = get_first_row(connection, table_name)

                # Get table description from the table_description_df
                table_description = (
                    table_description_df.loc[
                        table_description_df["table_name"] == table_name,
                        "table_description",
                    ].values[0]
                    if len(table_description_df) > 0
                    else "No description available."
                )

                # Get column names from the SQLite database for validation
                column_names = get_table_columns(connection, table_name)

                # Generate improved column descriptions
                for idx, row in table_df.iterrows():
                    # If imrpoved column description already exists update improved_description and skip llm call
                    if pd.notna(row.get("improved_column_description", None)):
                        improved_description = row["improved_column_description"]
                        logger.info(
                            f"Improved description for column {row['original_column_name']} already exists. Skipping LLM call."
                        )
                        continue
                    
                    if str(row["original_column_name"]).strip()  not in column_names:
                        errors.append({
                            'database': database_name,
                            'error': f"Column '{row['original_column_name']}' does not exist. Please check {table_csv}."
                        })
                        logger.error(f"Column '{row['original_column_name']}' does not exist. Please check {table_csv}.")
                        continue
                    
                    improved_description = get_imrpoved_coloumn_description(row, table_name, first_row, connection, client, table_description, errors, database_name)
                    
                    # Update the improved description in the DataFrame and save it to the CSV file
                    table_df.loc[idx, "improved_column_description"] = (
                        improved_description
                    )
                    updated_csv_path = os.path.join(base_path, table_csv)
                    table_df.to_csv(updated_csv_path, index=False)

        connection.close()
    except Exception as e:
        errors.append({
            'database': database_name,
            'error': f"{str(e)}"
        })
        raise RuntimeError(str(e))
    finally:
        return errors


def create_database_tables_csv(database_name, client):
    """ Create a {database_name}_tables.csv file with table descriptions and connected tables. """
    
    errors = []
    try:
        base_path = DATASET_DESCRIPTION_PATH.format(database_name=database_name)
        table_description_csv_path = os.path.join(
            base_path, f"{database_name}_tables.csv"
        )  # TO DO: Update file path as a constant after finalizing path organization.

        table_descriptions = pd.DataFrame(columns=['table_name', 'table_description'])
        if os.path.exists(table_description_csv_path):
            try:
                table_descriptions = pd.read_csv(table_description_csv_path)
            except Exception as e:
                table_descriptions = pd.DataFrame(columns=['table_name', 'table_description'])

        connection = sqlite3.connect(DATABASE_SQLITE_PATH.format(database_name=database_name))
        cursor = connection.cursor()
        
        schema_ddl = format_schema(FormatType.CODE, DATABASE_SQLITE_PATH.format(database_name=database_name))
        tables = get_table_names(connection)

        for table_name in tables:
            if table_name in table_descriptions['table_name'].values and not pd.isna(table_descriptions.loc[table_descriptions['table_name'] == table_name, 'table_description'].values[0]):
                logger.info(f"Table {table_name} already has a description. Skipping LLM call.")
                continue
            
            cursor.execute(
                f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}';"
            )
            table_schema = cursor.fetchone()[0]

            first_row = get_first_row(connection, table_name)
            
            prompt = TABLE_DESCRIPTION_PROMPT_TEMPLATE.format(
                schema_ddl=schema_ddl, ddl=table_schema, first_row=first_row
            )
        
            table_description = ""
            while table_description == "":
                try:
                    table_description = client.execute_prompt(prompt)
                except Exception as e:
                    if GOOGLE_RESOURCE_EXHAUSTED_EXCEPTION_STR in str(e):
                        # Rate limit exceeded: Too many requests. Retrying in 5 seconds...
                        time.sleep(5)
                    else:
                        errors.append({
                            'database': database_name,
                            'error': f"Error generating table {table_name} description: {str(e)}"
                        })
                        break
    
            new_row = pd.DataFrame({
                "table_name": [table_name],
                "table_description": [table_description]
            })
            table_descriptions = pd.concat([table_descriptions, new_row], ignore_index=True)


        # Create a DataFrame for all tables
        result_df = pd.DataFrame(table_descriptions)
        result_df.to_csv(table_description_csv_path, index=False)
        
        connection.close()
    except Exception as e:
        errors.append({
            'database': database_name,
            'error': f"{str(e)}"
        })
        raise RuntimeError(str(e))
    finally:
        return errors

def process_all_databases(
    dataset_dir: str,
    llm_type: LLMType,
    model: ModelType,
    temperature: float,
    max_tokens: int,
):
    """ Process all databases in the specified directory, improving column descriptions and table descriptions. """

    client = ClientFactory.get_client(llm_type, model, temperature, max_tokens)
    databases = [
        d for d in os.listdir(dataset_dir)
        if os.path.isdir(os.path.join(dataset_dir, d)) and d not in {".DS_Store", "train_tables.json"}
    ]

    error_list = []
    for database in tqdm(databases, desc="Generating Descriptions for databases"):
        try:
            errors = create_database_tables_csv(database_name=database, client=client)
            if not errors:
                errors = improve_column_descriptions(database_name=database, client=client)
                
            error_list.extend(errors)
        except Exception as e:
            logger.error(f"Error processing database {database}: {e}")
        
    if error_list:
        for error in error_list:
            logger.error(f"- Database: {error['database']}, Error: {error['error']}")
    else:
        logger.info("\nAll databases processed successfully.")


if __name__ == "__main__":
    """
    To run this script:
    
    1. Ensure you have set the correct `DATASET_TYPE` in `utilities.config`:
        - Set `DATASET_TYPE` to DatasetType.BIRD_TRAIN for training data.
        - Set `DATASET_TYPE` to DatasetType.BIRD_DEV for development data.
        - This script will only work for BIRD Datasets
    
    2. Run the script:
        python add_descriptions_bird_dataset.py
        
    3. Expected Outputs:
        - Generates a {database_name}_tables.csv file for each database with table descriptions. 
        - Updated {table_name}.csv with improved column descriptions for each schema.
        - Detailed logs for each database processed, including errors (if any).

    4. Notes:
        - BIRD_DEV: There should be no errors during processing; the dataset is expected to be clean and consistent.
        - BIRD_TRAIN: Errors are expected and will require manual resolution, such as renaming files or correcting column names.
    """
    
    # Initial variables
    
    # LLM Configurations
    llm_type = LLMType.GOOGLE_AI
    model = ModelType.GOOGLEAI_GEMINI_2_0_FLASH_EXP
    temperature = 0.7
    max_tokens = 8192

    process_all_databases(
        dataset_dir=DATASET_DIR,
        llm_type=llm_type,
        model=model,
        temperature=temperature,
        max_tokens=max_tokens,
    )
