import os
import pandas as pd
import sqlite3
import json
import traceback
from tqdm import tqdm

def connect_db(db_path):
    """
    Establishes a connection to a SQLite database.

    Args:
        db_path (str): Path to the SQLite database file.

    Returns:
        sqlite3.Connection: Connection object to the SQLite database.
    """
    conn = sqlite3.connect(db_path)
    return conn

def fetch_tables(conn):
    """
    Fetches the list of all table names in the connected SQLite database.

    Args:
        conn (sqlite3.Connection): Connection object to the SQLite database.

    Returns:
        pd.DataFrame: DataFrame containing table names.
    """
    query = "SELECT name FROM sqlite_master WHERE type='table';"
    tables = pd.read_sql_query(query, conn)
    return tables

def get_table_columns(conn, table_name):
    """
    Retrieves a dictionary with column names as keys and their datatypes as values for a given table.

    Args:
        conn (sqlite3.Connection): SQLite database connection.
        table_name (str): Name of the table to query.

    Returns:
        dict: Dictionary with column names as keys and datatypes as values.
    """
    try:
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info(\"{table_name}\")")
        table_info = cursor.fetchall()


        # Create dictionary of column names and datatypes
        columns = {col_info[1]: col_info[2] for col_info in table_info}
        
        if not columns:
            raise ValueError(f"Table '{table_name}' does not exist or has no columns.")

        return columns

    except sqlite3.Error as e:
        raise RuntimeError(f"SQLite error: {e}, query: PRAGMA table_info({table_name})")
    
def get_column_samples_list(conn, table_name, column_name, num_samples=3):
    """
    Retrieves random samples from a specified column in a SQLite database table.

    Args:
        db_path (str): Path to the SQLite database file.
        table_name (str): Name of the table to query.
        column_name (str): Name of the column to sample.
        num_samples (int): Number of samples to retrieve.

    Returns:
        list: A list of samples from the specified column.
    """
    try:

        cursor = conn.cursor()

        # Retrieve samples from the column
        cursor.execute(f"SELECT \"{column_name}\" FROM \"{table_name}\" LIMIT {num_samples}")
        all_rows = cursor.fetchall()
        
        if not all_rows:
            print(f"Table '{table_name}' or column '{column_name}' contains no data.")
            return None
     
        
        # Flatten the list of tuples to a list of values
        samples = [row[0] for row in all_rows]

        return samples

    except sqlite3.Error as e:
        raise RuntimeError(f"SQLite error: {e}, on query: SELECT {column_name} FROM {table_name} LIMIT {num_samples}")


if __name__ == "__main__":  # Ensures the code runs only when executed directly, not when imported as a module
    DATASET_PATH = "../server/data/bird/train/train_databases"  # Path to the dataset directory
    FINE_TUNE_SYSTEM_MESSAGE = (
        "You are a bot that generates business descriptions for columns in a database. "
        "You will be provided with the table name, column name, column data type and some sample values from the column. "
        "You have to provide a business description for the column. Only return the description without any other text."
    )  # Instruction message for the AI model
    FT_EXAMPLES_DIR = "./col_ft_dataset.jsonl"  # Output file path for the fine-tuning examples

    # Get a list of directories in the dataset path (each represents a database)
    databases = [folder for folder in os.listdir(DATASET_PATH) if os.path.isdir(os.path.join(DATASET_PATH, folder))]

    training_data = []  # Initialize an empty list to store training examples
    for db in tqdm(databases, desc=f'Processing Databases:'):  # Iterate over each database with a progress bar
        try:
            # Skip 'retails' database or hidden folders (starting with '.')
            if db == 'retails' or db.startswith('.'):
                continue

            db_path = f"{DATASET_PATH}/{db}/{db}.sqlite"  # Construct the database file path
            conn = connect_db(db_path)  # Connect to the SQLite database
            tables = fetch_tables(conn)  # Fetch all tables in the database

            for table in tables['name']:  # Iterate through the table names
                # Skip tables without a description file
                if not os.path.exists(f"{DATASET_PATH}/{db}/database_description/{table}.csv"):
                    continue

                # Read the column description file for the table
                with open(f"{DATASET_PATH}/{db}/database_description/{table}.csv", encoding="utf-8", errors="replace") as file:
                    col_df = pd.read_csv(file).dropna(subset=['original_column_name'])  # Drop rows with missing column names
                    col_df.dropna(subset=['column_description'], inplace=True)  # Drop rows with missing descriptions
                    file.close()
                
                col_type = get_table_columns(conn, table)  # Get column data types for the table

                for index, row in col_df.iterrows():  # Iterate over each row in the column description file
                    col = row['original_column_name'].rstrip()  # Get the column name and remove trailing whitespace
                    samples = get_column_samples_list(conn, table, col, 3)  # Fetch up to 3 sample values for the column

                    # Skip processing if no samples, column is not in the table, or column type is BLOB
                    if not samples or col not in col_type or col_type[col] == "BLOB":
                        continue

                    # Skip processing if sample values exceed a length of 500 characters
                    value_len = len('; '.join([str(i) for i in samples]))
                    if value_len > 500:
                        continue

                    # Prompt to generate the business description for the column
                    comment_prompt = f"""Here is column from table {table}:
    name: {col};
    type: {col_type[col]};
    values: {'; '.join([str(i) for i in samples])};
    Please provide a business description for the column. Only return the description without any other text."""
                    
                    col_description = col_df['column_description']  # Extract column descriptions from the DataFrame

                    # Create a training data entry with system, user, and assistant roles
                    entry = {"messages": [
                        {"role": "system", "content": FINE_TUNE_SYSTEM_MESSAGE},
                        {"role": "user", "content": comment_prompt},
                        {"role": "assistant", "content": row['column_description']}
                    ]}

                    # Add the entry to the training data
                    training_data.append(entry)
            
            conn.close()  # Close the database connection
        except Exception as e:
            traceback.print_exc()  # Print the traceback of the exception
            print(f"Error processing {db}, {table}: {e}")  # Log the error with database and table name
            print(f"row: {row}")  # Log the current row being processed
            print(f"col_df: {col_df}")  # Log the DataFrame containing column descriptions
            print(f"col_type: {col_type}")  # Log the column types for the table
            exit()  # Exit the script if an error occurs

    # Write the training data to the fine-tuning examples file
    with open(f"{FT_EXAMPLES_DIR}", 'w') as file:
        for i, item in enumerate(training_data):  # Iterate over the training data
            json.dump(item, file)  # Write each training data entry as a JSON object
            if i < len(training_data) - 1:  # Add a newline after every entry except the last
                file.write('\n')
        file.close()