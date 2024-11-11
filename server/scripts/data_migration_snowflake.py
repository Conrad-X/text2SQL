import os
import re
import json
import sqlite3
import pandas as pd
import snowflake.connector
from snowflake.connector import errors
from tqdm import tqdm

from utilities.config import DATASET_TYPE, DATASET_DIR, DATABASE_SQLITE_PATH

TEMP_CSV_FOLDER = './data/bird/temp_csv'

def connect_to_snowflake():
    return snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
    )

def initialize_snowflake_db(conn):
    cursor = conn.cursor()
    cursor.execute(f"USE DATABASE {DATASET_TYPE.value}")
    cursor.close()

def verify_column_types_and_names(conn, db_name, table_name, sqlite_columns, sqlite_column_types, migration_errors, failed_tables):
    """Verify that columns and types match between SQLite and Snowflake."""

    cursor = conn.cursor()
    try:
        # Fetch columns and types from Snowflake
        cursor.execute(f"SHOW COLUMNS IN TABLE {db_name}.{table_name}")
        columns = cursor.fetchall()

        # Extract columns and types
        snowflake_columns = [row[2] for row in columns]
        snowflake_column_types = {}

        for row in columns:
            column_json = json.loads(row[3])
            column_type = column_json.get("type", "UNKNOWN")
            snowflake_column_types[row[2]] = column_type 

        # Check for column mismatch
        if set(sqlite_columns) != set(snowflake_columns):
            migration_errors.append({
                "database": db_name,
                "table": table_name,
                "error": "Column mismatch",
                "sqlite_columns": sqlite_columns,
                "snowflake_columns": snowflake_columns
            })
            failed_tables.append(table_name)
            return False

        # Check for column type mismatch
        for col, sqlite_type in zip(sqlite_columns, sqlite_column_types):
            snowflake_type = snowflake_column_types.get(col, None)
            if snowflake_type and get_snowflake_data_type(sqlite_type) != snowflake_type:
                migration_errors.append({
                    "database": db_name,
                    "table": table_name,
                    "error": f"Column type mismatch for {col}",
                    "sqlite_type": sqlite_type,
                    "snowflake_type": snowflake_type
                })
                failed_tables.append(table_name)
                return False  

        return True

    except errors.ProgrammingError as e:
        migration_errors.append({
            "database": db_name,
            "table": table_name,
            "error": f"Error fetching columns or types from Snowflake: {e}"
        })
        failed_tables.append(table_name)
        return False 
    finally:
        cursor.close()

def verify_schema_migration(conn, db_name, db_path, migration_errors):
    """Verify if all tables and columns in SQLite database are correctly migrated to Snowflake, including row counts"""

    migration_status = True
    failed_tables = []

    with sqlite3.connect(db_path) as sql_connection:
        sqlite_tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", sql_connection)

        for table_name in tqdm(sqlite_tables['name'], desc=f"Verifying schema for {db_name}", unit="table"):
            # Get columns and types in SQLite
            sqlite_columns = pd.read_sql(f'PRAGMA table_info("{table_name}");', sql_connection)
            sqlite_column_names = sqlite_columns['name'].tolist()
            sqlite_column_types = sqlite_columns['type'].tolist()
            sqlite_row_count = pd.read_sql(f"SELECT COUNT(*) FROM \"{table_name}\";", sql_connection).iloc[0, 0]

            # Check columns and types in Snowflake
            if not verify_column_types_and_names(conn, db_name, table_name, sqlite_column_names, sqlite_column_types, migration_errors, failed_tables):
                migration_status = False
                continue

            # Fetch row count from Snowflake
            cursor = conn.cursor()
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {db_name}.{table_name}")
                snowflake_row_count = cursor.fetchone()[0]
            except errors.ProgrammingError as e:
                migration_errors.append({
                    "database": db_name,
                    "table": table_name,
                    "error": f"Error fetching row count from Snowflake: {e}"
                })
                failed_tables.append(table_name)
                migration_status = False
                continue
            finally:
                cursor.close()

            # Check for row count mismatch
            if abs(sqlite_row_count - snowflake_row_count) > 1:  # Allowing minor differences (e.g., 1 row mismatch)
                migration_errors.append({
                    "database": db_name,
                    "table": table_name,
                    "error": "Row count mismatch",
                    "sqlite_row_count": sqlite_row_count,
                    "snowflake_row_count": snowflake_row_count
                })
                failed_tables.append(table_name)
                migration_status = False

    # Output result for schema verification
    if migration_status:
        print(f"Schema migration verification for {db_name} was successful.")
    else:
        print(f"Schema migration verification for {db_name} failed.")
        print("Reasons for failure:")
        for error in migration_errors:
            if error['database'] == db_name:
                print(f"- Table: {error['table']}, Error: {error['error']}")
                if 'sqlite_columns' in error and 'snowflake_columns' in error:
                    print(f"  SQLite Columns: {error['sqlite_columns']}")
                    print(f"  Snowflake Columns: {error['snowflake_columns']}")
                if 'sqlite_row_count' in error and 'snowflake_row_count' in error:
                    print(f"  SQLite Row Count: {error['sqlite_row_count']}")
                    print(f"  Snowflake Row Count: {error['snowflake_row_count']}")

    return migration_status, failed_tables

def get_existing_schemas(conn):
    cursor = conn.cursor()
    cursor.execute(f"SHOW SCHEMAS IN DATABASE {DATASET_TYPE.value.upper()}")
    schemas = [row[1] for row in cursor.fetchall()]
    cursor.close()
    return schemas

def process_database(conn, db_path, csv_base_path, existing_schemas, migration_errors):
    db_name = os.path.basename(db_path).replace('.sqlite', '')

    # Verification of successful migration
    if db_name.upper() in existing_schemas:
        print(f"Schema {db_name} already exists. Verifying migration...")
        migration_status, failed_tables = verify_schema_migration(conn, db_name, db_path, migration_errors)

        if migration_status:
            print(f"All good with {db_name}")
            return
        else:
            print(f"Migrating again for db {db_name}")

    with sqlite3.connect(db_path) as sql_connection:
        tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", sql_connection)
        db_csv_folder = os.path.join(csv_base_path, db_name)
        os.makedirs(db_csv_folder, exist_ok=True)

        conn.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {db_name}")
        conn.cursor().execute(f"USE SCHEMA {DATASET_TYPE.value}.{db_name}")

        for table_name in tqdm(tables['name'], desc=f"Processing tables in {db_name}", unit="table"):
            if table_name not in failed_tables:
                print(f"Skipping table {table_name}, it passed migration verification.")
                continue

            df = export_table_to_csv(sql_connection, db_csv_folder, table_name)
            create_and_load_snowflake_table(conn, db_name, table_name, db_csv_folder, migration_errors, df)

def export_table_to_csv(sql_connection, db_csv_folder, table_name):
    df = pd.read_sql(f"SELECT * FROM [{table_name}];", sql_connection)
    csv_path = os.path.join(db_csv_folder, f"{table_name}.csv")
    df.to_csv(csv_path, index=False)
    print(f"Exported {table_name} to {csv_path}")
    return df 

def get_snowflake_data_type(sqlite_dtype):
    """Map SQLite data type to Snowflake data type."""

    dtype_map = {
        'INTEGER': 'INTEGER',
        'REAL': 'FLOAT',
        'TEXT': 'STRING',
        'BOOLEAN': 'BOOLEAN',
        'BLOB': 'STRING',
        'DATETIME': 'TIMESTAMP_LTZ',
    }
    return dtype_map.get(sqlite_dtype.upper(), 'STRING')

def get_create_table_sql(db_name, table_name):
    """Fetch the CREATE TABLE SQL command from the SQLite database."""

    # Create the connection to the SQLite database
    sqlite_db_path = DATABASE_SQLITE_PATH.format(database_name=db_name)
    connection = sqlite3.connect(sqlite_db_path)
    cursor = connection.cursor()

    try:
        # Query the SQLite master table to get the table creation SQL
        cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}';")
        create_table_sql = cursor.fetchone()

        if create_table_sql:
            return create_table_sql[0]
        else:
            print(f"Table {table_name} not found in {db_name} SQLite database.")
            return None
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        return None
    finally:
        cursor.close()
        connection.close()

def create_and_load_snowflake_table(conn, db_name, table_name, db_csv_folder, migration_errors, df):
    cursor = conn.cursor()
    csv_path = os.path.join(db_csv_folder, f"{table_name}.csv")

    # Get the CREATE TABLE SQL from SQLite
    create_table_sql = get_create_table_sql(db_name, table_name)
    if create_table_sql:

        columns_with_types = []
        matches = re.findall(r'`?(\w+)`?\s+(\w+)', create_table_sql)  # Extract column name and data type
        for col_name, col_type in matches:
            snowflake_type = get_snowflake_data_type(col_type)
            columns_with_types.append(f'"{col_name}" {snowflake_type}')

        # Create the table in Snowflake
        try:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {db_name}.{table_name} (
                    {', '.join(columns_with_types)}
                );
            """)

            cursor.execute(f"PUT file://{csv_path} @%{table_name};")

            cursor.execute(f"""
                COPY INTO {db_name}.{table_name}
                FROM @%{table_name}
                ON_ERROR = 'CONTINUE';
            """)
            print(f"Copied CSV data into table: {table_name}")

        except errors.ProgrammingError as e:
            migration_errors.append({
                "database": db_name,
                "table": table_name,
                "error": f"Snowflake error: {e}",
                "csv_path": csv_path
            })
            print(f"Error while creating and loading {db_name}, error = {str(e)}")
    else:
        print(f"CREATE TABLE SQL not found for {table_name}")

    cursor.close()

if __name__ == "__main__":
    """
    To run this script:

    1. Set the correct `DATASET_TYPE` in `utilities.config`:
        - Set `DATASET_TYPE` to `DatasetType.BIRD_TRAIN` for training data.
        - Set `DATASET_TYPE` to `DatasetType.BIRD_DEV` for development data.

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
    """

    base_path = DATASET_DIR
    csv_base_path = TEMP_CSV_FOLDER

    conn = connect_to_snowflake()
    migration_errors = []

    try:
        initialize_snowflake_db(conn)
        existing_schemas = get_existing_schemas(conn)
        print("Existing schemas:", existing_schemas)

        for db_name in tqdm(os.listdir(base_path), desc="Processing databases", unit="database"):
            db_path = DATABASE_SQLITE_PATH.format(database_name=db_name)
            if os.path.isdir(os.path.join(base_path, db_name)) and os.path.exists(db_path):
                process_database(conn, db_path, csv_base_path, existing_schemas, migration_errors)

    finally:
        conn.close()

    # Display accumulated migration errors
    if migration_errors:
        print("Migration errors encountered:")
        for error in migration_errors:
            print(error)
    else:
        print("All migrations were successful.")
