import sqlite3
import json
import csv
import os
import pandas as pd
from utilities.constants.database_enums import DatasetType
from utilities.config import PATH_CONFIG
from utilities.logging_utils import setup_logger
logger = setup_logger(__name__)


def get_dataset_paths(database_name:str, dataset_type: DatasetType):
    """
    Helper function:  Generate paths for a given WIKISQL dataset type ('dev' or 'test').
    """
    return {
        'db_path': PATH_CONFIG.sqlite_path(database_name=database_name, dataset_type=dataset_type),
        'data_jsonl_path': PATH_CONFIG.wiki_file_path(dataset_type=dataset_type),
        'schema_jsonl_path': PATH_CONFIG.column_meaning_path(dataset_type=dataset_type),
        'description_dir': PATH_CONFIG.description_dir(database_name=database_name, dataset_type=dataset_type),
        'schema_output_file': PATH_CONFIG.wiki_schema_file_path(dataset_type=dataset_type),
        'processed_test_file':PATH_CONFIG.processed_test_path(database_name=database_name)
    }


def get_column_data(table_name, database_description_path):
    """
    Helper function:  Read CSV file for a given table and extract columns data.
    Returns column_names_original, column_names, column_types.
    """
    csv_file_path = os.path.join(database_description_path, f'{table_name}.csv')
    df = pd.read_csv(csv_file_path)
    
    column_names_original = []
    column_names = []
    column_types = []
    
    for _, row in df.iterrows():
        column_names_original.append([row['original_column_name']])
        column_names.append([row['column_name']])
        column_types.append(row['data_format'])
    
    return column_names_original, column_names, column_types


def add_schema(database_name, tables_jsonl_path, database_description_path, schema_output_file):
    """
    Preprocessing function: 
    Read tables from *.tables.jsonl, process schema in format of BIRD solution, and write to schema_output_file.

    Result:
    - Creates schema_output file: A JSON file located at data/wikisql/dev_dataset/*_tables.json
    """
     # Load tables data from the JSONL file
    with open(tables_jsonl_path, 'r') as f:
        tables_data = [json.loads(line) for line in f]
    
    # Initialize the final structure for the JSON output
    final_data = {
        "db_id": database_name,
        "table_names_original": [],
        "table_names": [],
        "column_names_original": [],
        "column_names": [],
        "column_types": [],
        "primary_keys": [],
        "foreign_keys": []
    }

    # Process each table and collect schema data
    table_index = 0
    for table in tables_data:
        table_id = table['id']
        table_name = f"table_{table_id.replace('-', '_')}"
        page_title = table.get('page_title', table_name)
        
        # Add table names
        final_data["table_names_original"].extend([table_name])
        final_data["table_names"].extend([page_title])

        # Get column data from the respective CSV
        column_names_original, column_names, column_types = get_column_data(table_name, database_description_path)
        
        # Add columns with their respective table index
        for i, (col_orig, col_name) in enumerate(zip(column_names_original, column_names)):
            # Add the column with the index of its table
            final_data["column_names_original"].append([table_index, col_orig[0]])
            final_data["column_names"].append([table_index, col_name[0]])

        # Add column types, primary keys, and foreign keys
        final_data["column_types"].extend(column_types)
        final_data["primary_keys"].extend([]) # Not assigned in WIKI Dataset 
        final_data["foreign_keys"].extend([]) # Not assigned in WIKI Dataset 

        table_index += 1

    result = [final_data]

    # Write the output JSON file
    with open(schema_output_file, 'w') as outfile:
        json.dump(result, outfile, indent=4)

    logger.info(f"Schema and JSON conversion completed. Output saved to {schema_output_file}")

def add_table_descriptions_to_csv(db_path, jsonl_path, description_dir):
    """
    Preprocessing function:
    - Processes the database and generates a CSV file containing column name mapping and descriptions, value descriptions, and data types.
    
    Result:
    - Creates description_dir: The directory where the CSV files will be saved: data/wikisql/dev_dataset/dev/database_descriptions
    """
    os.makedirs(description_dir, exist_ok=True)
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
        csv_filename = os.path.join(description_dir, f"{table_name}.csv")
        
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


def generate_processed_test(jsonl_file, output_file):
    # Read dev.jsonl file
    with open(jsonl_file, 'r') as infile:
        data = [json.loads(line) for line in infile]

    # Prepare the output format
    processed_data = []

    for idx, entry in enumerate(data):
        question_entry = {
            "question_id": idx,  
            "db_id": "dev",  
            "question": entry.get("question", ""),  
            "evidence": "", 
            "SQL": entry.get("sql", ""), 
            "difficulty": "" 
        }
        processed_data.append(question_entry)

    # Write the processed data to the output JSON file
    with open(output_file, 'w') as outfile:
        json.dump(processed_data, outfile, indent=4)

if __name__ == "__main__":
    """
    To run this script:

    1. Ensure you have unzipped the dataset into the appropriate directory:
            The dataset should be unzipped in the following directories:
                DEV: server/data/wikisql/dev_dataset
                TEST: server/data/wikisql/test_dataset

    2. Run the following command:
       python3 -m python3 -m preprocess.prepare_wiki
            
    3. Expected Outputs:
       - This script will generate CSV files for each table in the dataset, containing descriptions for each column and value.
       - The CSV files will be saved in the descriptions directory specified for each dataset's database folder.
            e.g: data/wikisql/dev_dataset/dev/database_descriptions
       - Schema information will be added to the datasets in dev_tables.json or test_tables.json respectively.
       - Processed test file will be added to prepare for predictions
       
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
        data_jsonl_path = paths['data_jsonl_path']
        schema_jsonl_path = paths['schema_jsonl_path']
        description_dir = paths['description_dir']
        schema_output_file = paths['schema_output_file']
        processed_test_file = paths['processed_test_file']

        # Add table descriptions to CSV
        add_table_descriptions_to_csv(db_path, schema_jsonl_path, description_dir)

        # Add schema file
        add_schema(database_name, schema_jsonl_path, description_dir, schema_output_file)

        # Add processed test file
        generate_processed_test(data_jsonl_path, processed_test_file)

    print("\nProcessing complete!")
