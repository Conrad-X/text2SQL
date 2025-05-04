import sqlite3
import json
import csv
import os
import pandas as pd
import argparse
import re
import shutil
from enum import Enum
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
        'schema_jsonl_path': PATH_CONFIG.wiki_schema_file_path(dataset_type=dataset_type),
        'description_dir': PATH_CONFIG.description_dir(database_name=database_name, dataset_type=dataset_type),
        'schema_output_file': PATH_CONFIG.column_meaning_path(dataset_type=dataset_type),
        'processed_test_file': PATH_CONFIG.processed_test_path(dataset_type=dataset_type),
        'processed_train_file': PATH_CONFIG.processed_train_path(dataset_type=dataset_type),
        'json_input_file': os.path.join(os.path.dirname(PATH_CONFIG.wiki_file_path(dataset_type=dataset_type)), f"{database_name}.json")
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
        column_names_original.append(row['original_column_name']) 
        column_names.append(row['column_name'])  
        column_types.append(row['data_format'])
    
    return column_names_original, column_names, column_types

def tokenize_question(question):
    """
    Helper function: Split a question into tokens (words).
    """
    # Simple tokenization by splitting on spaces and keeping punctuation separate
    tokens = []
    for punct in ['.', ',', '?', '!', ';', ':', '(', ')', '[', ']', '{', '}']:
        question = question.replace(punct, f' {punct} ')
    
    # Split by whitespace and filter out empty tokens
    raw_tokens = question.split()
    tokens = [token for token in raw_tokens if token.strip()]
    
    return tokens

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
            final_data["column_names_original"].append([table_index, col_orig]) 
            final_data["column_names"].append([table_index, col_name]) 

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
    logger.info(f"Column Descriptions completed. Output saved to {description_dir}")


def generate_processed_test(json_file, jsonl_file, database_description_path, output_file):
    """
    Preprocessing function:

    Create enhanced processed_test.json file with additional fields:
    - question_toks: tokenized question
    - schema_used: schema information used in the query (using original col0, col1 format)
    - query: alias for SQL field
    
    Args:
        json_file: Path to the input JSON file (dev.json or test.json)
        jsonl_file: Path to the original JSONL file (for obtaining table_id and sql dict)
        db_path: Path to the SQLite database
        output_file: Path to the output file (processed_test.json)
    """
    try:
        # Load the JSON input file (from WikiSQL creator)
        with open(json_file, 'r') as f:
            data = json.load(f)
        
        # Load the original JSONL file (for table_id and sql dict)
        with open(jsonl_file, 'r') as f:
            jsonl_data = [json.loads(line) for line in f]
        
        # Create mapping of questions to table_ids and sql dicts
        question_mapping = {}
        for entry in jsonl_data:
            question = entry.get('question', '')
            table_id = entry.get('table_id', '')
            sql_dict = entry.get('sql', {})
            question_mapping[question] = {'table_id': table_id, 'sql_dict': sql_dict}
        
        # Enhance each entry in the data
        enhanced_data = []
        for entry in data:
            question = entry.get('question', '')
            
            # Create question_toks
            question_toks = tokenize_question(question)
            
            # Get table_id and sql_dict for this question
            table_info = question_mapping.get(question, {})
            table_id = table_info.get('table_id', '')
            
            # Extract table name
            table_name = f"table_{table_id.replace('-', '_')}"
            
            # Get columns from SQL dict
            columns, column_names, column_types = get_column_data(table_name, database_description_path)
            
            # Create schema_used with the new format - a simple list instead of list of lists
            schema_used = {table_name: columns}
            
            # Create enhanced entry
            enhanced_entry = entry.copy()
            enhanced_entry['question_toks'] = question_toks
            enhanced_entry['query'] = entry.get('SQL', '')
            enhanced_entry['schema_used'] = schema_used
            
            enhanced_data.append(enhanced_entry)
        
        # Write enhanced data to output file
        with open(output_file, 'w') as f:
            json.dump(enhanced_data, f, indent=4)
        
        logger.info(f"Created enhanced processed test file at {output_file}")
        
    except FileNotFoundError as e:
        logger.error(f"File not found: {str(e)}")
        logger.error("Make sure you've run the WikiSQL creator script first")
    except Exception as e:
        logger.error(f"Error generating processed test file: {str(e)}")

def process_dataset(dataset_type):
    """
    Core Preprocessing Function:
    Process a specific dataset (dev or test).
        1. Add table descriptions
        2. Add schema
        3. Generate Processed test file
        4. Copy test file to train file location
    """
    # Set Paths based on dataset type
    database_name = "dev" if dataset_type == DatasetType.WIKI_DEV else "test"
    logger.info(f"Processing {database_name} dataset...")
    
    paths = get_dataset_paths(database_name=database_name, dataset_type=dataset_type)
    db_path = paths['db_path']
    json_input_file = paths['json_input_file']
    data_jsonl_path = paths['data_jsonl_path']
    schema_jsonl_path = paths['schema_jsonl_path']
    description_dir = paths['description_dir']
    schema_output_file = paths['schema_output_file']
    processed_test_file = paths['processed_test_file']
    processed_train_file = paths['processed_train_file']

    # Add table descriptions to CSV
    add_table_descriptions_to_csv(db_path, schema_jsonl_path, description_dir)

    # Add schema file
    add_schema(database_name, schema_jsonl_path, description_dir, schema_output_file)

    # Create enhanced processed_test.json
    generate_processed_test(json_input_file, data_jsonl_path, description_dir, processed_test_file)
    
    # Copy test file to train file location
    shutil.copy2(processed_test_file, processed_train_file)

    logger.info(f"Completed processing {database_name} dataset")

def main():
    """Main function to process WikiSQL datasets based on command-line arguments."""
    parser = argparse.ArgumentParser(description='WikiSQL Dataset Preparation')
    parser.add_argument('--dataset', choices=['dev', 'test', 'all'], default='all',
                       help='Which dataset to process (dev, test, or all)')
    
    args = parser.parse_args()
    
    if args.dataset == 'dev' or args.dataset == 'all':
        process_dataset(DatasetType.WIKI_DEV)
    
    if args.dataset == 'test' or args.dataset == 'all':
        process_dataset(DatasetType.WIKI_TEST)
    
    print("\nProcessing complete!")


if __name__ == "__main__":
    """
    WikiSQL Dataset Preparation Script

    This script adds schema information, descriptions, and the processed_test file to the WikiSQL dataset
    after it has been processed by the WikiSQL creator script.

    This is to ensure that the fields and files that are lacking in WikiSQL dataset, but are required for this solution to work, are taken care of

    Usage (from server directory):
        python3 -m preprocess.wikisql.prepare_wiki --dataset dev
        python3 -m preprocess.wikisql.prepare_wiki --dataset test
        python3 -m preprocess.wikisql.prepare_wiki --dataset all  (processes both dev and test)
    """
    main()