import sqlite3
import json
import csv
import os
from utilities.constants.database_enums import DatasetType
from utilities.config import PATH_CONFIG
from utilities.logging_utils import setup_logger
logger = setup_logger(__name__)


def get_dataset_paths(database_name:str, dataset_type: DatasetType):
    """
    Helper function to generate paths for a given WIKISQL dataset type ('dev' or 'test').
    """
    return {
        'db_path': PATH_CONFIG.sqlite_path(database_name=database_name, dataset_type=dataset_type),
        'jsonl_path': PATH_CONFIG.column_meaning_path(dataset_type=dataset_type),
        'output_dir': PATH_CONFIG.description_dir(database_name=database_name, dataset_type=dataset_type),
    }


def add_table_descriptions_to_csv(db_path, jsonl_path, output_dir):
    """
    Processes the database and generates a CSV file containing column name mapping and descriptions, value descriptions, and data types.
    
    Args:
    - db_path: The path to the SQLite database.
    - jsonl_path: The path to the JSONL file containing table information.
    - output_dir: The directory where the CSV files will be saved: data/wikisql/dev_dataset/dev/database_descriptions
    """
    os.makedirs(output_dir, exist_ok=True)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    with open(jsonl_path, 'r') as f:
        tables_data = [json.loads(line) for line in f]

    # Process each table in the JSONL file
    for table in tables_data:
        table_id = table['id']
        table_name = f"table_{table_id.replace('-', '_')}" # Adjusting for the difference in actual table names in db vs how they are written in the jsonl file
        page_title = table.get('page_title')
        headers = table['header']
        types = table['types']
        rows = table['rows']

        # Get columns of the table from the SQLite database
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()

        # Prepare CSV 
        csv_header = ['original_column_name', 'column_name', 'column_description', 'data_format', 'value_description']
        csv_filename = os.path.join(output_dir, f"{table_name}.csv")
        
        # Iterate through tables in jsonl file
        with open(csv_filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(csv_header)  
            
            # Iterate through each column
            for column in columns:
                original_column_name = column[1]  # The original column names (e.g., col0, col1, col2)

                # Write the details in the csv file for this column
                if original_column_name.startswith('col'):
                    index = int(original_column_name[3:])
                    if index < len(headers):
                        column_name = headers[index] # Mapped, meaningful column name
                        column_description = f"This column is about {column_name} and belongs to the table {page_title}"
                        data_format = types[index] if index < len(types) else 'unknown'
                        example_value = rows[0][index] if len(rows) > 0 and index < len(rows[0]) else 'N/A'
                        value_description = f"An example of a value in the column '{column_name}' is: '{example_value}' and the values are of type {data_format}"
                        
                        writer.writerow([original_column_name, column_name, column_description, data_format, value_description])
    conn.close()


# TODO
def add_schema_to_tables(db_path):
    """
    Adds schema information to the dataset.
    """
    # Placeholder for schema addition logic

    pass


if __name__ == "__main__":
    """
    To run this script:

    1. Ensure you have unzipped the dataset into the appropriate directory:
            The dataset should be unzipped in the following directories:
                DEV: server/data/wikisql/dev_dataset
                TEST: server/data/wikisql/test_dataset

    2. Run the following command:
       python3 -m data.wikisql.preprocess_wiki
            
    3. Expected Outputs:
       - This script will generate CSV files for each table in the dataset, containing descriptions for each column and value.
       - The CSV files will be saved in the descriptions directory specified for each dataset's database folder.
            e.g: data/wikisql/dev_dataset/dev/database_descriptions
       - Schema information will be added to the datasets (placeholder function: add_schema_to_tables).
       
    4. Processing Details:
       - The script will iterate over both the 'dev' and 'test' datasets, performing the following steps for each:
         • Adding column descriptions to CSV files for each table in the dataset.
         • Adding schema information to the tables (currently a TODO).

    """

    # Iterate over both datasets (dev and test)
    dataset_types = [DatasetType.WIKI_DEV, DatasetType.WIKI_TEST]
    for dataset_type in dataset_types:
        logger.info(f"Processing {dataset_type.name} dataset...")

        # Set Paths
        database_name = "dev" if dataset_type == DatasetType.WIKI_DEV else "test"
        paths = get_dataset_paths(database_name=database_name, dataset_type=dataset_type)
        db_path = paths['db_path']
        jsonl_path = paths['jsonl_path']
        output_dir = paths['output_dir']
        
        logger.info(f"\n DB PATH: {db_path}, \n JSONL PATH: {jsonl_path}, \n OUTPUT DIR PATH: {output_dir} \n")
        # DEBUG Expected paths: 
            # DB PATH: /Users/macbookpro/text2SQL/server/data/wikisql/test_dataset/test/test.db, 
            # JSONL PATH: /Users/macbookpro/text2SQL/server/data/wikisql/test_dataset/test.tables.jsonl, 
            # OUTPUT DIR PATH: /Users/macbookpro/text2SQL/server/data/wikisql/test_dataset/test/database_description 
        

        # Add table descriptions to CSV
        add_table_descriptions_to_csv(db_path, jsonl_path, output_dir)

        # TODO: Add schema to JSON
        add_schema_to_tables(db_path)

    print("\nProcessing complete!")
