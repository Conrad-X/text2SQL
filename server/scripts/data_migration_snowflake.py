import os
import json
import sqlite3
import pandas as pd
import snowflake.connector
from snowflake.connector import errors
from tqdm import tqdm
from datetime import datetime

from utilities.config import PATH_CONFIG
from utilities.logging_utils import setup_logger

TEMP_CSV_FOLDER = "./data/bird/temp_csv"

logger = setup_logger(__name__)

SQLITE_TO_SNOWFLAKE_TYPE_MAP = {
    "BLOB": "TEXT",
    "NUMERIC": "NUMBER",
    "DATETIME": "TIMESTAMP_NTZ",
}


def connect_to_snowflake():
    try:
        snowflake_connection = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            role="ACCOUNTADMIN",
        )
        logger.info("Successfully connected to Snowflake.")
        return snowflake_connection
    except Exception as e:
        logger.error(f"Error connecting to Snowflake: {e}")
        raise


def initialize_snowflake_db(snowflake_connection):
    cursor = snowflake_connection.cursor()
    try:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {PATH_CONFIG.dataset_type.value}")
        cursor.execute(f"USE DATABASE {PATH_CONFIG.dataset_type.value}")
        logger.info(f"Database {PATH_CONFIG.dataset_type.value} initialized successfully.")
    except snowflake.connector.errors.ProgrammingError as e:
        logger.error(f"Error creating database: {e}")
        raise
    finally:
        cursor.close()


def get_existing_schemas(snowflake_connection):
    cursor = snowflake_connection.cursor()
    try:
        cursor.execute(f"SHOW SCHEMAS IN DATABASE {PATH_CONFIG.dataset_type.value.upper()}")
        schemas = [row[1] for row in cursor.fetchall()]
        logger.info(f"Fetched existing schemas")
        return schemas
    except snowflake.connector.errors.ProgrammingError as e:
        logger.error(f"Error getting existing schemas: {e}")
        raise
    finally:
        cursor.close()


def verify_table_exists(cursor, db_name, table_name):
    # Check if table exists
    cursor.execute(f"SHOW TABLES LIKE '{table_name}' IN SCHEMA {db_name}")
    table_exists = cursor.fetchone()

    if not table_exists:
        logger.warning(f"Table {table_name} does not exist in Snowflake.")
        return False

    logger.info(f"Table {table_name} exists in {db_name}.")
    return True


def verify_columns(cursor, db_name, table_name, sql_connection):
    # Get columns and types in SQLite
    sqlite_columns = pd.read_sql(f'PRAGMA table_info("{table_name}");', sql_connection)
    sqlite_columns_with_types = {
        row["name"].lower(): SQLITE_TO_SNOWFLAKE_TYPE_MAP.get(
            row["type"].upper(), row["type"].upper()
        )
        for _, row in sqlite_columns.iterrows()
    }

    # Get columns and types in Snowflake
    cursor.execute(f'SHOW COLUMNS IN TABLE {db_name}."{table_name}"')
    snowflake_columns = cursor.fetchall()
    snowflake_columns_with_types = {
        row[2].lower(): json.loads(row[3])["type"].upper() for row in snowflake_columns
    }

    # Compare column names and types
    for sqlite_col_name, sqlite_type in sqlite_columns_with_types.items():
        snowflake_type = (
            snowflake_columns_with_types.get(sqlite_col_name) or "NOT FOUND"
        )

        if (
            "NUMBER" in sqlite_type
            or "DECIMAL" in sqlite_type
            or "INTEGER" in sqlite_type
        ) and "FIXED" in snowflake_type:
            continue
        elif sqlite_type != snowflake_type:
            logger.warning(
                f"Column type mismatch for table {table_name}, column {sqlite_col_name}: "
                f"SQLite type {sqlite_type}, Snowflake type {snowflake_type or 'NOT FOUND'}."
            )
            return False

    logger.info(f"Table {table_name} have correct coloums.")
    return True


def verify_rows(cursor, db_name, table_name, sql_connection):
    # Get rows in SQLite
    sqlite_row_count = pd.read_sql(
        f'SELECT COUNT(*) FROM "{table_name}";', sql_connection
    ).iloc[0, 0]

    # Get rows in Snowflake
    cursor.execute(f'SELECT COUNT(*) FROM {db_name}."{table_name}"')
    snowflake_row_count = cursor.fetchone()[0]

    # Check if row counts match
    if abs(sqlite_row_count - snowflake_row_count) > 1:  # Allowing 1 row difference
        logger.warning(
            f"Row count mismatch for table {table_name}: "
            f"SQLite rows: {sqlite_row_count}, Snowflake rows: {snowflake_row_count}."
        )
        return False

    logger.info(f"Table {table_name} have correct rows.")
    return True


def verify_and_correct_schema_migration(
    snowflake_connection, db_name, db_path, migration_errors
):
    with sqlite3.connect(db_path) as sql_connection:
        sqlite_tables = pd.read_sql(
            "SELECT name FROM sqlite_master WHERE type='table';", sql_connection
        )

        for table_name in tqdm(
            sqlite_tables["name"], desc=f"Verifying schema for {db_name}", unit="table"
        ):
            if table_name == "sqlite_sequence":
                logger.info(f"Skipping internal SQLite table: {table_name}")
                continue

            cursor = snowflake_connection.cursor()
            try:
                # Step 1: Verify if table exists in Snowflake and Create and Load Data if it does not
                if not verify_table_exists(cursor, db_name, table_name):
                    logger.info(
                        f"Creating and loading table {table_name} in Snowflake."
                    )
                    create_snowflake_table(
                        snowflake_connection, db_name, table_name, migration_errors
                    )
                    load_data_in_snowflake_table(
                        snowflake_connection, db_name, table_name, migration_errors
                    )

                # Step 2: Verify and correct columns and Drop the table and create and load it again if the coloumns are not correct
                elif not verify_columns(cursor, db_name, table_name, sql_connection):
                    logger.info(
                        f"Dropping and recreating table {table_name}, then reloading data."
                    )
                    cursor.execute(f'DROP TABLE IF EXISTS {db_name}."{table_name}";')
                    create_snowflake_table(
                        snowflake_connection, db_name, table_name, migration_errors
                    )
                    load_data_in_snowflake_table(
                        snowflake_connection, db_name, table_name, migration_errors
                    )

                # Step 3: Verify and correct rows and add Data if the row count is not correct
                elif not verify_rows(cursor, db_name, table_name, sql_connection):
                    logger.info(f"Row count mismatch for {table_name}. Reloading data.")
                    cursor.execute(f'DELETE FROM {db_name}."{table_name}";')
                    load_data_in_snowflake_table(
                        snowflake_connection, db_name, table_name, migration_errors
                    )

                else:
                    logger.info(
                        f"Table {table_name} in Database {db_name} was correctly migrated"
                    )

            except errors.ProgrammingError as e:
                migration_errors.append(
                    {
                        "database": db_name,
                        "table": table_name,
                        "error": f"Error verifying table {table_name} in {db_name}: {e}",
                    }
                )
                logger.error(
                    f"Error while verifying {db_name}.{table_name}, error = {str(e)}"
                )
                continue
            finally:
                cursor.close()


def process_database(snowflake_connection, db_path, existing_schemas, migration_errors):
    db_name = os.path.basename(db_path).replace(".sqlite", "")
    cursor = snowflake_connection.cursor()

    try:
        # Check if schema exists
        if db_name.upper() in existing_schemas:
            logger.info(f"Schema {db_name} already exists. Verifying migration...")

            cursor.execute(f"USE SCHEMA {PATH_CONFIG.dataset_type.value}.{db_name}")
            verify_and_correct_schema_migration(
                snowflake_connection, db_name, db_path, migration_errors
            )

            return

        logger.info(f"Schema {db_name} does not exist. Processing database {db_name}.")

        # Creating all tables and loading data if schema not found
        with sqlite3.connect(db_path) as sql_connection:
            tables = pd.read_sql(
                "SELECT name FROM sqlite_master WHERE type='table';", sql_connection
            )

            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {db_name}")
            cursor.execute(f"USE SCHEMA {PATH_CONFIG.dataset_type.value}.{db_name}")

            for table_name in tqdm(
                tables["name"], desc=f"Processing tables in {db_name}", unit="table"
            ):
                if table_name == "sqlite_sequence":
                    logger.info(f"Skipping internal SQLite table: {table_name}")
                    continue
                create_snowflake_table(
                    snowflake_connection, db_name, table_name, migration_errors
                )
                load_data_in_snowflake_table(
                    snowflake_connection, db_name, table_name, migration_errors
                )
    except errors.ProgrammingError as e:
        migration_errors.append(
            {
                "database": db_name,
                "error": f"Error processing database {db_name} in Snowflake: {e}",
            }
        )
        logger.error(f"Error processing database {db_name} in Snowflake: {e}")
    finally:
        cursor.close()


def get_create_table_sql(db_name, table_name):
    sqlite_db_path = PATH_CONFIG.sqlite_path(database_name=db_name)
    sql_connection = sqlite3.connect(sqlite_db_path)
    cursor = sql_connection.cursor()

    try:
        # Fetch column information from PRAGMA table_info
        cursor.execute(f'PRAGMA table_info("{table_name}");')
        columns = cursor.fetchall()

        if columns:
            # Start building the CREATE TABLE SQL statement
            create_table_sql = f'CREATE TABLE "{table_name}" (\n'
            column_definitions = []

            # Loop through the columns and build the column definitions
            for column in columns:
                column_name = column[1]
                column_type = column[2].upper()

                snowflake_type = SQLITE_TO_SNOWFLAKE_TYPE_MAP.get(
                    column_type, column_type
                )  # Default is sqlite type

                column_definitions.append(f'    "{column_name}" {snowflake_type}')

            # Join the column definitions
            create_table_sql += ",\n".join(column_definitions)
            create_table_sql += "\n);"

            logger.info(f"Create table SQL for {table_name}: {create_table_sql}")
            return create_table_sql
        else:
            logger.warning(
                f"Table {table_name} not found in {db_name} SQLite database."
            )
            return None
    except sqlite3.Error as e:
        logger.error(f"Error generating create table SQL for {table_name}: {e}")
        return None
    finally:
        cursor.close()
        sql_connection.close()


def create_snowflake_table(snowflake_connection, db_name, table_name, migration_errors):
    cursor = snowflake_connection.cursor()
    try:
        # Create the table
        create_table_sql = get_create_table_sql(db_name, table_name)
        logger.debug(f"Create table SQL: {create_table_sql}")

        cursor.execute(f"USE SCHEMA {db_name}")
        cursor.execute(create_table_sql)

        logger.info(f"Table created successfully: {db_name}.{table_name}")
    except errors.ProgrammingError as e:
        migration_errors.append(
            {
                "database": db_name,
                "table": table_name,
                "error": f"Snowflake error: {str(e)}",
            }
        )
        logger.error(f"Error while creating {db_name}.{table_name}, error = {str(e)}")
    finally:
        cursor.close()


def normalize_date(value):
    if pd.isnull(value) or value in ["", "NULL"]:
        return None

    try:
        # Handle formats like 'YYYY/MM/DD'
        parsed_date = datetime.strptime(value, "%Y/%m/%d")
        return parsed_date.strftime("%Y-%m-%d")
    except ValueError:
        pass

    # Handle malformed dates explicitly (e.g., '1960-0-0')
    components = value.split("-") if "-" in value else value.split("/")
    if len(components) == 3:
        year, month, day = components
        try:
            year = int(year)
            month = int(month) if month.isdigit() and 1 <= int(month) <= 12 else 1
            day = int(day) if day.isdigit() and 1 <= int(day) <= 31 else 1

            # Create a valid date
            corrected_date = datetime(year, month, day)
            return corrected_date.strftime("%Y-%m-%d")
        except ValueError:
            return None  # Return None if parsing fails completely

    return value


def clean_dataframe(df):
    # Remove duplicate header rows
    df = df[~df.apply(lambda row: row.equals(df.iloc[0]), axis=1)].reset_index(
        drop=True
    )

    # Normalize datetime columns
    for column in df.columns:
        if "date" in column.lower() or "time" in column.lower():
            df[column] = df[column].apply(
                lambda x: normalize_date(x) if isinstance(x, str) else x
            )

    return df


def export_table_to_csv(db_name, table_name):
    sqlite_db_path = PATH_CONFIG.sqlite_path(database_name=db_name)
    sql_connection = sqlite3.connect(sqlite_db_path)

    try:
        # Read the table data into a DataFrame
        df = pd.read_sql(f'SELECT * FROM "{table_name}";', sql_connection)

        # Clean the DataFrame
        df = clean_dataframe(df)

        # Export the DataFrame to a CSV
        csv_path = os.path.join(TEMP_CSV_FOLDER, db_name, f"{table_name}.csv")
        df.to_csv(
            csv_path, index=False, encoding="utf-8", quotechar='"', escapechar="\\"
        )

        logger.info(f"Exported {table_name} to {csv_path}")
    except sqlite3.Error as e:
        logger.error(f"SQLite error: {e}")
        return None
    finally:
        sql_connection.close()


def load_data_in_snowflake_table(
    snowflake_connection, db_name, table_name, migration_errors
):
    cursor = snowflake_connection.cursor()

    db_csv_folder = os.path.join(TEMP_CSV_FOLDER, db_name)
    os.makedirs(db_csv_folder, exist_ok=True)
    csv_path = os.path.join(db_csv_folder, f"{table_name}.csv")

    if not os.path.exists(csv_path):
        export_table_to_csv(db_name, table_name)

    try:
        cursor.execute(
            f"""REMOVE $$@{PATH_CONFIG.dataset_type.value.upper()}.{db_name.upper()}.\"%{table_name}\"$$; """
        )
        # Copy the CSV file to the stage
        cursor.execute(
            f"""PUT 'file://{csv_path}' $$@{PATH_CONFIG.dataset_type.value.upper()}.{db_name.upper()}.\"%{table_name}\"$$; """
        )

        # Copy the data from the stage to the table
        cursor.execute(
            f"""
            COPY INTO {db_name}.\"{table_name}\"
            FROM $$@{PATH_CONFIG.dataset_type.value.upper()}.{db_name.upper()}.%\"{table_name}\"$$
            FILE_FORMAT = (
                TYPE = 'CSV' 
                FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
                FIELD_DELIMITER = ',' 
                ESCAPE_UNENCLOSED_FIELD = None 
                SKIP_HEADER = 1
                BINARY_FORMAT = 'HEX'
            )
        """
        )

        logger.info(f"Copied CSV data into table: {table_name}")
    except errors.ProgrammingError as e:
        migration_errors.append(
            {"database": db_name, "table": table_name, "error": f"Snowflake error: {e}"}
        )
        logger.error(f"Error while loading {db_name}.{table_name}, error = {str(e)}")
    finally:
        cursor.close()


if __name__ == "__main__":
    """
        To run this script:

        1. Ensure you have set the correct `PATH_CONFIG.dataset_type` and `PATH_CONFIG.sample_dataset_type` in `utilities.config`:
            - Set `PATH_CONFIG.dataset_type` to DatasetType.BIRD_TRAIN for training data.
            - Set `PATH_CONFIG.dataset_type` to DatasetType.BIRD_DEV for development data.

        2. Make sure the specified dataset exists. Also make sure that the following environment variales are configured in the .env
            - SNOWFLAKE_USER
            - SNOWFLAKE_PASSWORD
            - SNOWFLAKE_ACCOUNT

        3. Run the script to verify and migrate data:
            - In the terminal, navigate to the main project (server) folder.
            - Run the script to process and migrate the databases:
                python3 -m script.data_migration_snowflake

            - The script will:
                - Connect to Snowflake and verify the schema migration for each database.
                - Export SQLite tables to CSV.
                - Create the corresponding tables in Snowflake if necessary and load the CSV data into them.
            
        4. Expected Output:
            - The script will output detailed logs for each table processed, showing whether the schema migration was successful, or if any errors were encountered during the migration (e.g., column or row count mismatches).
            - A summary of any migration errors will be printed at the end of the script.

        Other info:
            The folder structure in Snowflake for BIRD_TRAIN would be as follows:
            - BIRD_TRAIN (Database)
            - ADDRESS (Schema)
                - TABLE 1
                - TABLE 2
            There is a difference in terminology between Snowflake and SQLite:
                In SQLite, a "Database" is equivalent to a "Schema" in Snowflake.
                In SQLite, a "Dataset" corresponds to a "Database" in Snowflake.
                The term Table remains the same in both platforms.
    """

    snowflake_connection = connect_to_snowflake()
    migration_errors = []

    try:
        initialize_snowflake_db(snowflake_connection)
        existing_schemas = get_existing_schemas(snowflake_connection)

        for db_name in tqdm(
            os.listdir(PATH_CONFIG.dataset_dir()), desc="Processing databases", unit="database"
        ):
            db_path = os.path.join(PATH_CONFIG.dataset_dir(), db_name, f"{db_name}.sqlite")
            if os.path.exists(db_path):
                process_database(
                    snowflake_connection, db_path, existing_schemas, migration_errors
                )

        # To test a single database, use the following code:

        # initialize_snowflake_db(snowflake_connection)
        # db_name = "car_retails"
        # db_path = os.path.join(PATH_CONFIG.dataset_dir(), db_name, f"{db_name}.sqlite")
        # if os.path.exists(db_path):
        #     process_database(snowflake_connection, db_path, [db_name.upper()], migration_errors)

    finally:
        snowflake_connection.close()

    if migration_errors:
        logger.error(
            f"Migration completed with errors: {json.dumps(migration_errors, indent=2)}"
        )
    else:
        logger.info("Migration completed successfully.")
