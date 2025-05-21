import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Union

import pandas as pd
from preprocess.AddDescriptionErrorLogs import AddDescriptionErrorLogs
from services.client_factory import Client, ClientFactory
from tqdm import tqdm
from utilities.bird_utils import read_csv
from utilities.config import PATH_CONFIG
from utilities.constants.database_enums import DatasetType
from utilities.constants.LLM_enums import LLMType, ModelType
from utilities.constants.preprocess.add_descriptions_bird_dataset.indexing_constants import (
    COLUMN_DESCRIPTION_COL, DATA_FORMAT_COL, IMPROVED_COLUMN_DESCRIPTIONS_COL,
    ORIG_COLUMN_NAME_COL, TABLE_DESCRIPTION_COL, TABLE_NAME_COL)
from utilities.constants.preprocess.add_descriptions_bird_dataset.response_messages import (
    ERROR_COLUMN_DOES_NOT_EXIST, ERROR_COLUMN_MEANING_FILE_NOT_FOUND,
    ERROR_ENSURING_DESCRIPTION_FILES_EXIST,
    ERROR_GENERATING_COLUMN_DESCRIPTIONS, ERROR_GENERATING_TABLE_DESCRIPTIONS,
    ERROR_INITIALIZING_DESCRIPTION_FILES, ERROR_SQLITE_EXECUTION_ERROR,
    ERROR_TABLE_DOES_NOT_EXIST, ERROR_UPDATING_DESCRIPTION_FILES,
    INFO_COLUMN_ALREADY_HAS_DESCRIPTIONS, INFO_TABLE_ALREADY_HAS_DESCRIPTIONS)
from utilities.constants.prompts_enums import FormatType
from utilities.constants.script_constants import UNKNOWN_COLUMN_DATA_TYPE_STR
from utilities.format_schema import format_schema
from utilities.logging_utils import setup_logger
from utilities.prompts.prompt_templates import (
    COLUMN_DESCRIPTION_PROMPT_TEMPLATE, TABLE_DESCRIPTION_PROMPT_TEMPLATE)
from utilities.utility_functions import (get_table_columns, get_table_ddl,
                                         get_table_names)

logger = setup_logger(__name__)


# TODO: SQL Constants to be replaced in another file once main connections made
SQL_GET_TABLE_INFO = 'PRAGMA table_info("{table_name}");'
SQL_SELECT_FIRST_ROW = 'SELECT * FROM "{table_name}" LIMIT 1'

DESCRIPTION_FILE_EXTENSION = ".csv"
TABLE_DESCRIPION_FILE = "{database_name}_tables.csv"
TABLE_DESCRIPTION_PLACEHOLDER = "No Description Available"

PRAGMA_COLUMN_NAME_INDEX = 1
PRAGMA_COLUMN_TYPE_INDEX = 2


def extract_column_type_from_schema(
    connection: sqlite3.Connection, table_name: str, column_name: str
) -> str:
    """
    Retrieves the data type of a specific column from a SQLite table schema.

    Executes a PRAGMA table_info query on the given table to inspect its schema and
    return the declared data type of the specified column. If the column is not found,
    a predefined constant indicating an unknown data type is returned.

    Args:
        connection (sqlite3.Connection): An active SQLite database connection.
        table_name (str): The name of the table to inspect.
        column_name (str): The name of the column whose data type is to be retrieved.

    Returns:
        str: The data type of the column, or UNKNOWN_COLUMN_DATA_TYPE_STR if not found.
    """

    cursor = connection.cursor()

    try:
        cursor.execute(SQL_GET_TABLE_INFO.format(table_name=table_name))
        columns = cursor.fetchall()
    except sqlite3.Error as e:
        raise RuntimeError(
            ERROR_SQLITE_EXECUTION_ERROR.format(
                sql=SQL_GET_TABLE_INFO.format(table_name=table_name), error=str(e)
            )
        )
    finally:
        cursor.close()

    for column in columns:
        if column[PRAGMA_COLUMN_NAME_INDEX].lower() == column_name.lower():
            column_type = column[
                PRAGMA_COLUMN_TYPE_INDEX
            ]  # column[2] contains the data type
            return column_type

    return UNKNOWN_COLUMN_DATA_TYPE_STR


def get_table_first_row_values(connection: sqlite3.Connection, table_name: str) -> list:
    """
    Retrieves the first row of data from the specified SQLite table.

    Executes a SQL query to fetch the first row of the table. Each value in the row
    is converted to a string; if a value is None, it is replaced with "N/A".
    If the table is empty, an empty list is returned.

    Args:
        connection (sqlite3.Connection): An active SQLite database connection.
        table_name (str): The name of the table to query.

    Returns:
        list: A list of stringified values from the first row, or an empty list if the table is empty.
    """

    cursor = connection.cursor()

    try:
        cursor.execute(SQL_SELECT_FIRST_ROW.format(table_name=table_name))
        first_row = cursor.fetchone()
    except sqlite3.Error as e:
        raise RuntimeError(
            ERROR_SQLITE_EXECUTION_ERROR.format(
                sql=SQL_SELECT_FIRST_ROW.format(table_name=table_name),
                error=str(e),
            )
        )
    finally:
        cursor.close()

    if first_row:
        return [str(value) if value is not None else "N/A" for value in first_row]
    else:
        return []


def get_improved_column_description(
    row: pd.Series,
    table_name: str,
    table_row: list,
    connection: sqlite3.Connection,
    client: Client,
    table_description: str,
    database_name: str,
):
    """
    Processes and generates an improved description for a database column using an LLM client.

    This function builds a prompt based on the table and column metadata and sends it to a
    language model client for generating an enhanced column description. If an error occurs
    during processing, it logs the issue to a shared error list.

    Args:
        row (pd.Series): A row from a DataFrame representing column metadata.
        table_name (str): The name of the table containing the column.
        table_row (list): The first row of data from the table, used to infer context.
        connection (sqlite3.Connection): An active connection to the SQLite database.
        client (Client): A client interface for executing prompts (e.g., an LLM wrapper).
        table_description (str): A textual description of the table, if available.
        database_name (str): The name of the database, used for logging purposes.

    Returns:
        str: An improved description of the column generated by the language model.
    """

    error_log_store = AddDescriptionErrorLogs()

    improved_description = ""
    try:
        column_name = str(row[ORIG_COLUMN_NAME_COL]).strip()
        column_type = (
            row[DATA_FORMAT_COL]
            if pd.notna(row[DATA_FORMAT_COL])
            else extract_column_type_from_schema(connection, table_name, column_name)
        )
        column_description = (
            row[COLUMN_DESCRIPTION_COL] if pd.notna(
                row[COLUMN_DESCRIPTION_COL]) else ""
        )
        column_comment_part = (
            f"Column description: {column_description}\n" if column_description else ""
        )

        prompt = COLUMN_DESCRIPTION_PROMPT_TEMPLATE.format(
            table_name=table_name,
            table_description=table_description,
            table_first_row_values=table_row,
            column_name=column_name,
            datatype=column_type,
            column_comment_part=column_comment_part,
        )

        improved_description = client.execute_prompt(prompt)
    except Exception as e:
        error_log_store.errors.append(
            {
                "database": database_name,
                "error": ERROR_GENERATING_COLUMN_DESCRIPTIONS.format(
                    column_name=row[ORIG_COLUMN_NAME_COL], error=str(e)
                ),
            }
        )

    return improved_description


def table_in_db_check(
    table_column_description_csv: str, database_description_path: Path, tables_in_database: list, database_name: str
) -> Union[None, list]:
    """
    Checks whether a CSV file corresponds to an existing table in the database.

    This function verifies that the given CSV file name (without the .csv suffix)
    matches a table name in the provided list of database tables. If the table
    does not exist, it logs an error and returns a corresponding error dictionary.

    Args:
        table_column_description_csv (str): The name of the CSV file to check (including ".csv").
        database_description_path (Path): The base path where the CSV file is located.
        tables_in_database (list): List of table names present in the database.
        database_name (str): Name of the database, used in error reporting.

    Returns:
        Union[None, list]: None if the table exists; otherwise, a list containing
        an error dictionary describing the missing table.
    """

    if (
        table_column_description_csv.endswith(DESCRIPTION_FILE_EXTENSION) and table_column_description_csv.removesuffix(
            DESCRIPTION_FILE_EXTENSION) not in tables_in_database
    ):
        error = {
            "database": database_name,
            "error": ERROR_TABLE_DOES_NOT_EXIST.format(
                table_name=table_column_description_csv.removesuffix(
                    DESCRIPTION_FILE_EXTENSION),
                file_path=os.path.join(
                    database_description_path, table_column_description_csv),
            ),
        }
        logger.error(error["error"])

        return error
    return None


def is_column_description_file(file_name: str, database_name: str) -> bool:
    """
    Determines whether a file should be processed based on its name.

    The function returns True if the file has a .csv extension and is not the
    special "{database_name}_tables.csv" file, which is excluded from processing.
    """
    return file_name.endswith(
        DESCRIPTION_FILE_EXTENSION
    ) and file_name != TABLE_DESCRIPION_FILE.format(database_name=database_name)


def get_table_description_df(database_name: str) -> pd.DataFrame:
    """
    Retrieves a DataFrame containing table descriptions for a specified database.

    This function reads a CSV file that holds table descriptions for the given database.
    The CSV file is located using a predefined path configuration. The function then
    loads the CSV file into a pandas DataFrame and returns it.  The DataFrame is
    expected to have columns such as "table_name" and "description".

    Args:
        database_name (str): The name of the database for which table descriptions are requested.

    Returns:
        pd.DataFrame: A DataFrame containing table descriptions, with each row representing
                      a table and its corresponding description.
    """
    table_description_path = PATH_CONFIG.table_description_file(
        database_name=database_name
    )
    try:
        table_description_df = read_csv(table_description_path)
        return table_description_df
    except FileNotFoundError:
        raise FileNotFoundError(
            f"Table description file not found: {table_description_path}"
        )
    except pd.errors.EmptyDataError:
        raise ValueError(
            f"Table description file is empty: {table_description_path}")


def get_table_description(table_name: str, table_description_df: pd.DataFrame) -> str:
    """
    Retrieves the description of a specific table from a CSV metadata file.

    This function reads a CSV file containing table descriptions (based on the given
    database name) and returns the description for the specified table. If the table
    is not found or the file is empty, a default placeholder description is returned.

    Args:
        database_name (str): The name of the database.
        table_name (str): The name of the table whose description is requested.

    Returns:
        str: The table description, or a placeholder string if not available.
    """

    try:
        table_description = table_description_df.loc[
            table_description_df[TABLE_NAME_COL] == table_name,
            TABLE_DESCRIPTION_COL,
        ].values[0]

    except Exception:
        table_description = TABLE_DESCRIPTION_PLACEHOLDER

    return table_description


def improve_column_descriptions_for_table(
    table_df: pd.DataFrame,
    connection: sqlite3.Connection,
    table_name: str,
    database_name: str,
    improvement_client: Client,
    table_description_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Enhances column descriptions in a DataFrame using metadata and a language model client.

    This function processes each row in the given DataFrame representing table columns. If a column
    lacks an improved description and exists in the actual SQLite table, it generates an improved
    description using a language model. Errors encountered during processing (e.g., missing columns)
    are logged and stored in a singleton error tracking object.

    Args:
        table_df (pd.DataFrame): DataFrame containing column metadata.
        connection (sqlite3.Connection): SQLite database connection for schema inspection.
        table_name (str): Name of the table being processed.
        database_name (str): Name of the database (used for logging).
        improvement_client (Client): Client interface for generating improved descriptions.

    Returns:
        pd.DataFrame: The input DataFrame updated with improved column descriptions.
    """

    error_log_store = AddDescriptionErrorLogs()

    # Get column names from the SQLite database for validation
    column_names = get_table_columns(connection, table_name)

    # Get table description from the table_description_df if table found in table_df
    table_description = get_table_description(
        table_description_df=table_description_df, table_name=table_name
    )

    table_first_row_values = get_table_first_row_values(
        connection, table_name=table_name)

    for index, row in table_df.iterrows():

        existing_column_description = row.get(
            IMPROVED_COLUMN_DESCRIPTIONS_COL, None)

        # Only process rows where the improved_column_description is None
        if existing_column_description is None:

            # if column name from the dataframe exists in the database
            if str(row[ORIG_COLUMN_NAME_COL]).strip() in column_names:

                improved_description = get_improved_column_description(
                    row,
                    table_name,
                    table_first_row_values,
                    connection,
                    improvement_client,
                    table_description,
                    database_name,
                )

                # Update the improved description in the DataFrame and save it to the CSV file
                table_df.loc[index, IMPROVED_COLUMN_DESCRIPTIONS_COL] = (
                    improved_description
                )

            else:
                error = {
                    "database": database_name,
                    "error": ERROR_COLUMN_DOES_NOT_EXIST.format(
                        column_name=row[ORIG_COLUMN_NAME_COL], table_name=table_name
                    ),
                }
                error_log_store.errors.append(error)
                logger.error(error["error"])
        else:
            logger.info(
                INFO_COLUMN_ALREADY_HAS_DESCRIPTIONS.format(
                    column_name=row[ORIG_COLUMN_NAME_COL]
                )
            )

    return table_df


def improve_column_descriptions(
    database_name: str,
    dataset_type: DatasetType,
    improvement_client: Client,
    connection: sqlite3.Connection,
) -> None:
    """
    Improves column descriptions for all relevant tables in a specified database.

    This function iterates through CSV files representing database tables, and for each one,
    it checks if the corresponding table exists in the SQLite database. If so, it updates
    the column descriptions using a language model client, writing the results back to the file.
    Errors encountered during the process are collected in a singleton error tracker.

    Args:
        database_name (str): The name of the target database.
        dataset_type (DatasetType): Enum or identifier for the dataset type (e.g., staging, production).
        improvement_client (Client): Client used to generate improved column descriptions (e.g., an LLM wrapper).
        connection (sqlite3.Connection): SQLite connection used to query database metadata.

    Returns:
        None

    Raises:
        RuntimeError: If an unexpected error occurs during processing.
    """

    error_log_store = AddDescriptionErrorLogs()

    try:

        database_description_path = PATH_CONFIG.description_dir(
            database_name=database_name, dataset_type=dataset_type
        )

        tables_in_database = get_table_names(connection)
        table_description_df = get_table_description_df(
            database_name=database_name)

        for table_column_description_csv in os.listdir(database_description_path):

            # Only iterate over those files for seperate tables that end with .csv
            if is_column_description_file(file_name=table_column_description_csv, database_name=database_name):

                table_name = table_column_description_csv.split(
                    DESCRIPTION_FILE_EXTENSION)[0]

                table_not_in_db = table_in_db_check(
                    table_column_description_csv=table_column_description_csv,
                    database_description_path=database_description_path,
                    tables_in_database=tables_in_database,
                    database_name=database_name,
                )
                if table_not_in_db is not None:
                    error_log_store.errors.append(table_not_in_db)
                    continue

                table_df = read_csv(os.path.join(
                    database_description_path, table_column_description_csv))

                updated_table_df = improve_column_descriptions_for_table(
                    table_df=table_df,
                    connection=connection,
                    table_name=table_name,
                    database_name=database_name,
                    improvement_client=improvement_client,
                    table_description_df=table_description_df
                )

                updated_table_df.to_csv(os.path.join(
                    database_description_path, table_column_description_csv), index=False)

    except Exception as e:
        error_log_store.errors.append(
            {"database": database_name, "error": f"{str(e)}"})
        raise RuntimeError(str(e))


def create_database_tables_csv(
    database_name: str,
    dataset_type: DatasetType,
    client: Client,
    connection: sqlite3.Connection,
) -> None:
    """
    Generates or updates a `{database_name}_tables.csv` file with table descriptions for a database.

    This function retrieves all table names from the given SQLite database and checks whether each
    already has a description in the existing CSV file. If not, it generates a description using
    a language model client based on the table DDL and first row of data. The results are saved to
    a CSV file that holds descriptions for all tables.

    Args:
        database_name (str): The name of the database being described.
        dataset_type (DatasetType): The type of dataset (e.g., dev, prod), used for formatting schema context.
        client (Client): A prompt-executing client used to generate table descriptions.
        connection (sqlite3.Connection): A connection to the SQLite database.

    Returns:
        None

    Raises:
        RuntimeError: If any unexpected error occurs during the generation or file writing process.
    """
    error_log_store = AddDescriptionErrorLogs()

    try:
        table_description_csv_path = PATH_CONFIG.table_description_file(
            database_name=database_name
        )

        try:
            table_descriptions = read_csv(table_description_csv_path)
        except FileNotFoundError:
            table_descriptions = pd.DataFrame(
                columns=[TABLE_NAME_COL, TABLE_DESCRIPTION_COL]
            )

        schema_ddl = format_schema(
            FormatType.CODE, database_name=database_name, dataset_type=dataset_type
        )
        tables = get_table_names(connection)

        for table_name in tables:
            if table_name in table_descriptions[TABLE_NAME_COL].values and not pd.isna(
                table_descriptions.loc[
                    table_descriptions[TABLE_NAME_COL] == table_name,
                    TABLE_DESCRIPTION_COL,
                ].values[0]
            ):
                logger.info(
                    INFO_TABLE_ALREADY_HAS_DESCRIPTIONS.format(
                        table_name=table_name)
                )
                continue

            table_create_statement = get_table_ddl(connection, table_name)
            table_first_row_values = get_table_first_row_values(
                connection, table_name)

            table_description_prompt = TABLE_DESCRIPTION_PROMPT_TEMPLATE.format(
                schema_ddl=schema_ddl,
                ddl=table_create_statement,
                first_row=table_first_row_values,
            )

            table_description = ""
            try:
                table_description = client.execute_prompt(
                    table_description_prompt)
            except Exception as e:
                error_log_store.errors.append(
                    {
                        "database": database_name,
                        "error": ERROR_GENERATING_TABLE_DESCRIPTIONS.format(
                            table_name=table_name, error=str(e)
                        ),
                    }
                )

            new_row = pd.DataFrame(
                {
                    TABLE_NAME_COL: [table_name],
                    TABLE_DESCRIPTION_COL: [table_description],
                }
            )
            table_descriptions = pd.concat(
                [table_descriptions, new_row], ignore_index=True
            )

            # Create a DataFrame for all tables
            result_df = pd.DataFrame(table_descriptions)
            result_df.to_csv(table_description_csv_path, index=False)

    except Exception as e:

        error_log_store.errors.append(
            {"database": database_name, "error": f"{str(e)}"})
        raise RuntimeError(str(e))


def initialize_column_descriptions(
    database_description_path: Path,
    column_meaning: dict,
    database_name: str,
    connection: sqlite3.Connection,
) -> None:
    """
    Creates initial CSV files containing column descriptions for each table in a database.

    This function reads column metadata from a dictionary (typically parsed from a
    `column_meaning.json` file), filters it for the specified database, and extracts
    column data types from the SQLite schema. It then creates individual CSV files
    for each table containing the original column name, data type, and provided description.

    Args:
        database_description_path (Path): Directory path where the CSV files will be saved.
        column_meaning (dict): Dictionary mapping keys of the format "database|table|column"
                               to human-readable column descriptions.
        database_name (str): Name of the database for which descriptions are being initialized.
        connection (sqlite3.Connection): SQLite connection used to fetch column types.

    Returns:
        None

    Raises:
        RuntimeError: If the initialization process fails (e.g., directory creation, file writing).
    """

    try:
        os.makedirs(database_description_path)
        table_data = {}

        # Iterate over the column meanings
        for key, description in column_meaning.items():
            database, table, column = key.split("|")
            if database == database_name:
                if table not in table_data:
                    table_data[table] = []
                data_format = extract_column_type_from_schema(
                    connection, table, column)
                table_data[table].append(
                    {
                        ORIG_COLUMN_NAME_COL: column,
                        DATA_FORMAT_COL: data_format,
                        COLUMN_DESCRIPTION_COL: description.strip("#").strip(),
                    }
                )

        # Create CSV files for each table
        for table, columns in table_data.items():
            table_df = pd.DataFrame(columns)
            table_column_description_csv_path = os.path.join(
                database_description_path, f"{table}.csv")
            table_df.to_csv(table_column_description_csv_path, index=False)
    except Exception as e:
        raise RuntimeError(
            ERROR_INITIALIZING_DESCRIPTION_FILES.format(error=str(e)))


def update_column_descriptions(
    database_description_path: Path, column_meaning: dict, database_name: str
) -> None:
    """
    Updates existing column description CSV files using values from a column_meaning dictionary.

    This function scans all table-specific `.csv` files (excluding `{database_name}_tables.csv`)
    in the specified directory, and for each column, checks if a corresponding description exists
    in the `column_meaning` dictionary. If so, it updates the column description to the longer
    of the existing or new description.

    Args:
        database_description_path (Path): Directory containing the column description CSV files.
        column_meaning (dict): Dictionary mapping "database|table|column" keys to new descriptions.
        database_name (str): The name of the database (used to filter and construct keys).

    Returns:
        None

    Raises:
        RuntimeError: If an error occurs during reading, updating, or writing CSV files.
    """

    try:
        for table_file in os.listdir(database_description_path):
            if (
                table_file.endswith(DESCRIPTION_FILE_EXTENSION)
                and not table_file == f"{database_name}_tables.csv"
            ):
                table_name = table_file.replace(DESCRIPTION_FILE_EXTENSION, "")
                table_column_description_csv_path = os.path.join(
                    database_description_path, table_file)
                existing_df = read_csv(table_column_description_csv_path)

                # Update / check all columns mentioned in the csv files
                updated_columns = []
                for _, row in existing_df.iterrows():
                    key = (
                        f"{database_name}|{table_name}|{str(row[ORIG_COLUMN_NAME_COL])}"
                    )

                    # If the key exists in the column meaning, update the description to the longer of the two
                    if key in column_meaning:
                        new_description = column_meaning[key].strip(
                            "#").strip()
                        row[COLUMN_DESCRIPTION_COL] = max(
                            str(row[COLUMN_DESCRIPTION_COL]), new_description, key=len
                        )

                    updated_columns.append(row)

                updated_df = pd.DataFrame(updated_columns)
                updated_df.to_csv(
                    table_column_description_csv_path, index=False)
    except Exception as e:
        raise RuntimeError(
            ERROR_UPDATING_DESCRIPTION_FILES.format(error=str(e)))


def ensure_description_files_exist(
    database_name: str, dataset_type: DatasetType, connection: sqlite3.Connection
):
    """
    Ensure that column description files exist for the specified database. If they do not exist,
    create them using data from the `column_meaning.json` file. If they already exist, update
    the descriptions in the existing CSV files with the latest and more detailed information
    from the `column_meaning.json`.

    This function performs the following checks and actions:
    1. It checks if the `column_meaning.json` file exists.
    2. If the description files do not exist, it will create them based on the data from
       `column_meaning.json`.
    3. If the description files already exist, it will update the descriptions in those files
       with the longer descriptions found in `column_meaning.json`.

    Args:
        database_name (str): The name of the database for which description files are being ensured.
        dataset_type (DatasetType): The type of dataset (e.g., staging, production), used to locate paths.
        connection (sqlite3.Connection): A connection to the SQLite database, used for schema inspection.

    Returns:
        None

    Raises:
        RuntimeError: If an error occurs while ensuring the existence of description files,
                      or if the required `column_meaning.json` file is missing or invalid.
    """
    try:
        # Get the base path for the description files and the path to the column meaning file
        database_description_path = PATH_CONFIG.description_dir(
            database_name=database_name, dataset_type=dataset_type
        )
        column_meaning_path = PATH_CONFIG.column_meaning_path(
            dataset_type=dataset_type)

        # If the column meaning file exists, load it
        column_meaning = None
        if os.path.exists(column_meaning_path):
            with open(column_meaning_path, "r") as f:
                column_meaning = json.load(f)

        # If the description directory does not exist and column_meaning is loaded, create the directory and files
        if not os.path.exists(database_description_path) and column_meaning:
            initialize_column_descriptions(
                database_description_path=database_description_path,
                column_meaning=column_meaning,
                connection=connection,
                database_name=database_name,
            )

        # If the description directory exists and column_meaning is loaded, update the CSV files with longer descriptions
        elif os.path.exists(database_description_path) and column_meaning:
            update_column_descriptions(
                database_description_path=database_description_path,
                column_meaning=column_meaning,
                database_name=database_name,
            )

        else:
            raise RuntimeError(
                ERROR_COLUMN_MEANING_FILE_NOT_FOUND.format(
                    file_path=column_meaning_path
                )
            )
    except Exception as e:
        logger.error(
            ERROR_ENSURING_DESCRIPTION_FILES_EXIST.format(
                database_name=database_name, error=str(e)
            )
        )
        raise RuntimeError(e)


def add_database_descriptions(
    dataset_type: str,
    llm_type: LLMType,
    model: ModelType,
    temperature: float,
    max_tokens: int,
):
    """
    Process and improve descriptions for databases in the given dataset.

    For each database, the script performs the following steps:
    1. Establishes a connection to the SQLite database.
    2. Ensures that description files for tables and columns exist and are populated. If they do not exist, it creates them.
    3. Creates or updates a CSV file with table descriptions.
    4. Uses a language model client to improve column descriptions, provided no errors are encountered.
    5. Logs any errors encountered during the processing of a database.

    This process is repeated for each database found in the dataset directory, and a progress bar is displayed during execution. If any errors are accumulated, they are logged after all databases have been processed.

    Args:
        dataset_type (DatasetType): The type of dataset (e.g., "production" or "staging") used to determine paths.
        llm_type (str): The type of language model to use for improving descriptions.
        model (str): The identifier of the LLM model to be used.
        temperature (float): Controls the randomness of the model's output.
        max_tokens (int): The maximum number of tokens to generate for descriptions.

    Returns:
        None

    Raises:
        Exception: If any error occurs during the processing of a database, it is logged and the exception is raised.
    """

    error_log_store = AddDescriptionErrorLogs()
    dataset_dir = PATH_CONFIG.dataset_dir(dataset_type=dataset_type)

    client = ClientFactory.get_client(llm_type, model, temperature, max_tokens)

    databases = [
        d
        for d in os.listdir(dataset_dir)
        if os.path.isdir(os.path.join(dataset_dir, d))
    ]

    for database in tqdm(databases, desc="Generating Descriptions for databases"):
        try:
            with sqlite3.connect(
                PATH_CONFIG.sqlite_path(
                    database_name=database, dataset_type=dataset_type
                )
            ) as connection:

                ensure_description_files_exist(
                    database_name=database,
                    dataset_type=dataset_type,
                    connection=connection,
                )
                create_database_tables_csv(
                    database_name=database,
                    dataset_type=dataset_type,
                    client=client,
                    connection=connection,
                )
                if len(error_log_store.errors) == 0:
                    improve_column_descriptions(
                        database_name=database,
                        dataset_type=dataset_type,
                        improvement_client=client,
                        connection=connection,
                    )

        except Exception as e:
            logger.error(
                f"Error processing database {database}: {e}", exc_info=True)

    if error_log_store.errors:
        for error in error_log_store.errors:
            logger.error(
                f"- Database: {error['database']}, Error: {error['error']}")
    else:
        logger.info("\nAll databases processed successfully.")


if __name__ == "__main__":
    """
    To run this script:

    1. Ensure you have set the correct DATASET_TYPE in .env:
        - Set DATASET_TYPE to DatasetType.BIRD_TRAIN for training data.
        - Set DATASET_TYPE to DatasetType.BIRD_DEV for development data.
        - This script will only work for BIRD Datasets

    3. Expected Outputs:
        - Generates a {database_name}_tables.csv file for each database with table descriptions.
        - Updated {table_name}.csv with improved column descriptions for each schema.
        - Detailed logs for each database processed, including errors (if any).

    4. Notes:
        - BIRD_DEV: There should be no errors during processing; the dataset is expected to be clean and consistent.
        - BIRD_TRAIN: Errors are expected and will require manual resolution, such as renaming files or correcting column names.
    """

    add_database_descriptions(
        dataset_type=PATH_CONFIG.dataset_type,
        llm_type=LLMType.GOOGLE_AI,
        model=ModelType.GOOGLEAI_GEMINI_2_0_FLASH,
        temperature=0.7,
        max_tokens=8192,
    )
