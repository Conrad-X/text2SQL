"""
WikiSQL Dataset Setup Script

This script downloads the WikiSQL dataset, extracts it, and organizes it
into the proper folder structure for the text2SQL project.

Usage:
    python3 preprocess/wikisql/setup_wikisql.py

The script should be run from the server directory.
"""
import os
import sys
import shutil
import subprocess
import urllib.request
import tempfile
from pathlib import Path

# GitHub raw content URL for the data.tar.bz2 file
WIKISQL_DATA_URL = "https://github.com/salesforce/WikiSQL/raw/master/data.tar.bz2"

# Define relative paths

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SERVER_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, "../.."))
DATA_DIR = os.path.join(SERVER_DIR, "data", "wikisql")

# Define dataset directories
DEV_DATASET_DIR = os.path.join(DATA_DIR, "dev_dataset")
TEST_DATASET_DIR = os.path.join(DATA_DIR, "test_dataset")

# Define database directories
DEV_DB_DIR = os.path.join(DEV_DATASET_DIR, "dev")
TEST_DB_DIR = os.path.join(TEST_DATASET_DIR, "test")

def create_directory_structure():
    """Create the necessary directory structure"""
    print("Creating directory structure...")
    
    # Create all directories
    for directory in [DATA_DIR, DEV_DATASET_DIR, TEST_DATASET_DIR, DEV_DB_DIR, TEST_DB_DIR]:
        os.makedirs(directory, exist_ok=True)
        
    print(f"Directory structure created at {DATA_DIR}")

def download_wikisql_data(url, target_file):
    """Download the WikiSQL dataset"""
    print(f"Downloading WikiSQL dataset from {url}...")
    
    try:
        # Create a progress bar for download
        def report_progress(block_num, block_size, total_size):
            downloaded = block_num * block_size
            percent = min(100, downloaded * 100 / total_size)
            sys.stdout.write(f"\rProgress: {percent:.1f}% ({downloaded/1024/1024:.1f} MB)")
            sys.stdout.flush()
        
        # Download the file
        urllib.request.urlretrieve(url, target_file, report_progress)
        print("\nDownload complete.")
        return True
    
    except Exception as e:
        print(f"\nError downloading file: {e}")
        return False

def extract_tar_file(tar_file, extract_dir):
    """Extract the tar.bz2 file"""
    print(f"Extracting {tar_file} to {extract_dir}...")
    
    try:
        # Use tar command through subprocess
        result = subprocess.run(
            ["tar", "xvjf", tar_file, "-C", extract_dir],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        if result.returncode == 0:
            print("Extraction complete.")
            return True
        else:
            print(f"Error during extraction: {result.stderr}")
            return False
    
    except Exception as e:
        print(f"Error extracting file: {e}")
        return False

def organize_files(extracted_dir):
    """Organize the files into the proper structure"""
    print("Organizing files into the proper structure...")
    
    # Define file mappings (source -> destination)
    file_mappings = {
        # Dev dataset files
        os.path.join(extracted_dir, "data", "dev.db"): 
            os.path.join(DEV_DB_DIR, "dev.db"),
        os.path.join(extracted_dir, "data", "dev.jsonl"): 
            os.path.join(DEV_DATASET_DIR, "dev.jsonl"),
        os.path.join(extracted_dir, "data", "dev.tables.jsonl"): 
            os.path.join(DEV_DATASET_DIR, "dev.tables.jsonl"),
        
        # Test dataset files
        os.path.join(extracted_dir, "data", "test.db"): 
            os.path.join(TEST_DB_DIR, "test.db"),
        os.path.join(extracted_dir, "data", "test.jsonl"): 
            os.path.join(TEST_DATASET_DIR, "test.jsonl"),
        os.path.join(extracted_dir, "data", "test.tables.jsonl"): 
            os.path.join(TEST_DATASET_DIR, "test.tables.jsonl"),
    }
    
    # Move each file to its destination
    for src, dest in file_mappings.items():
        if os.path.exists(src):
            print(f"Moving {os.path.basename(src)} to {dest}")
            shutil.copy2(src, dest)
        else:
            print(f"Warning: Source file {src} not found")
    
    print("Files organized successfully.")

def setup_wikisql():
    """Main function to set up the WikiSQL dataset"""
    print("Starting WikiSQL dataset setup...")
    print(f"Script directory: {SCRIPT_DIR}")
    print(f"Server directory: {SERVER_DIR}")
    
    # Create the directory structure
    create_directory_structure()
    
    # Create a temporary directory for download and extraction
    with tempfile.TemporaryDirectory() as temp_dir:
        # Define file paths
        tar_file = os.path.join(temp_dir, "data.tar.bz2")
        extract_dir = os.path.join(temp_dir, "extract")
        os.makedirs(extract_dir, exist_ok=True)
        
        # Download the dataset
        if not download_wikisql_data(WIKISQL_DATA_URL, tar_file):
            print("Failed to download the WikiSQL dataset.")
            return False
        
        # Extract the tar file
        if not extract_tar_file(tar_file, extract_dir):
            print("Failed to extract the WikiSQL dataset.")
            return False
        
        # Organize the files
        organize_files(extract_dir)
    
    print("\nWikiSQL dataset setup complete!")
    print(f"Dev dataset located at: {DEV_DATASET_DIR}")
    print(f"Test dataset located at: {TEST_DATASET_DIR}")
    
    # Copy the dataset creator script to the preprocess/wikisql directory
    dataset_creator_path = os.path.join(SCRIPT_DIR, "wikisql_creator.py")
    with open(dataset_creator_path, "w") as f:
        f.write(DATASET_CREATOR_SCRIPT)
    print(f"\nDataset creator script saved to: {dataset_creator_path}")
    
    # Print instructions for running the dataset creator
    print("\nTo create the final JSON datasets, run:")
    print("python preprocess/wikisql/wikisql_creator.py --dataset dev")
    print("python preprocess/wikisql/wikisql_creator.py --dataset test")
    
    return True

# Dataset creator script content
DATASET_CREATOR_SCRIPT = """#!/usr/bin/env python
\"\"\"
WikiSQL Dataset Creator for Server Structure

This script processes WikiSQL data in the new folder structure and creates
properly formatted JSON dataset files.

Usage:
    python preprocess/wikisql/wikisql_creator.py --dataset dev
    python preprocess/wikisql/wikisql_creator.py --dataset test
\"\"\"
import json
import os
import sys
import re
import sqlite3
from collections import defaultdict

class WikiSQLProcessor:
    \"\"\"Process WikiSQL queries and convert them to executable SQL\"\"\"
    
    # Define constants copied from the original WikiSQL codebase
    AGG_OPS = ['', 'MAX', 'MIN', 'COUNT', 'SUM', 'AVG']
    COND_OPS = ['=', '>', '<', 'OP']
    
    def __init__(self, db_path):
        \"\"\"Initialize with SQLite database path\"\"\"
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        
        # Set up regex patterns
        self.schema_re = re.compile(r'\\((.+)\\)')
        self.num_re = re.compile(r'[-+]?\\d*\\.\\d+|\\d+')
    
    def close(self):
        \"\"\"Close the database connection\"\"\"
        if self.conn:
            self.conn.close()
    
    def get_table_schema(self, table_id):
        \"\"\"Get the schema for a table\"\"\"
        # Convert table_id to SQLite table name
        table_name = f"table_{table_id.replace('-', '_')}"
        
        # Query SQLite schema
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT sql FROM sqlite_master WHERE tbl_name = ?", (table_name,))
        sql_create = cursor.fetchone()
        
        if not sql_create:
            return None
        
        # Extract column definitions
        schema_match = self.schema_re.search(sql_create[0])
        if not schema_match:
            return None
        
        schema_str = schema_match.group(1)
        schema = {}
        
        for column_def in schema_str.split(", "):
            parts = column_def.split()
            if len(parts) >= 2:
                col_name, col_type = parts[0], parts[1]
                schema[col_name] = col_type
        
        return {"name": table_name, "schema": schema}
    
    def convert_query(self, wikisql_query, table_id):
        \"\"\"Convert WikiSQL query format to SQL\"\"\"
        # Get the table schema
        table_info = self.get_table_schema(table_id)
        if not table_info:
            return None, None, "Table not found"
        
        table_name = table_info["name"]
        schema = table_info["schema"]
        
        # Extract query components
        sel_index = wikisql_query.get('sel', 0)
        agg_index = wikisql_query.get('agg', 0)
        conditions = wikisql_query.get('conds', [])
        
        # Build the SELECT part
        select_col = f"col{sel_index}"
        agg_op = self.AGG_OPS[agg_index] if 0 <= agg_index < len(self.AGG_OPS) else ''
        
        if agg_op:
            select_clause = f"{agg_op}({select_col})"
        else:
            select_clause = select_col
        
        # Build the WHERE part
        where_clauses = []
        params = {}
        
        for col_index, op_index, val in conditions:
            col_name = f"col{col_index}"
            
            # Get the correct operator
            if 0 <= op_index < len(self.COND_OPS):
                op = self.COND_OPS[op_index]
            else:
                op = '='
            
            # Handle value based on column type
            param_name = f"val{len(params)}"
            
            # Convert to lowercase for comparison as WikiSQL does
            if isinstance(val, str):
                val = val.lower()
            
            # Check if it needs numeric conversion
            col_type = schema.get(col_name, 'text')
            if col_type == 'real' and not isinstance(val, (int, float)):
                try:
                    # Try to convert to float if it's numeric
                    val = float(val)
                except (ValueError, TypeError):
                    # Try to extract number using regex
                    matches = self.num_re.findall(str(val))
                    if matches:
                        try:
                            val = float(matches[0])
                        except (ValueError, TypeError):
                            pass
            
            where_clauses.append(f"{col_name} {op} :{param_name}")
            params[param_name] = val
        
        # Build the complete query
        sql = f"SELECT {select_clause} AS result FROM {table_name}"
        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)
        
        return sql, params, None
    
    def format_query_with_params(self, sql, params):
        \"\"\"Format SQL query by substituting parameters\"\"\"
        formatted = sql
        for name, value in params.items():
            if isinstance(value, str):
                formatted = formatted.replace(f":{name}", f"'{value}'")
            else:
                formatted = formatted.replace(f":{name}", str(value))
        return formatted
    
    def execute_query(self, sql, params):
        \"\"\"Execute SQL query with parameters and return results\"\"\"
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql, params)
            results = [dict(row) for row in cursor.fetchall()]
            
            # Extract just the result values
            if results:
                return [row['result'] for row in results], None
            else:
                return [], None
        
        except Exception as e:
            return None, str(e)
    
    def process_wikisql_query(self, question_id, question, table_id, wikisql_query):
        \"\"\"Process a complete WikiSQL query and return formatted results\"\"\"
        # Convert to SQL
        sql, params, error = self.convert_query(wikisql_query, table_id)
        
        if error:
            return {
                'question_id': question_id,
                'success': False,
                'error': error,
                'formatted_sql': None,
                'execution_result': None
            }
        
        # Format SQL for display
        formatted_sql = self.format_query_with_params(sql, params)
        
        # Execute query to get results
        execution_result, exec_error = self.execute_query(sql, params)
        
        if exec_error:
            return {
                'question_id': question_id,
                'success': False,
                'error': exec_error,
                'formatted_sql': formatted_sql,
                'execution_result': None
            }
        
        return {
            'question_id': question_id,
            'success': True,
            'error': None,
            'formatted_sql': formatted_sql,
            'execution_result': execution_result
        }

def process_dataset(dataset_type):
    \"\"\"Process a dataset with the new folder structure\"\"\"
    # Define paths based on the new structure
    script_dir = os.path.dirname(os.path.abspath(__file__))
    server_dir = os.path.abspath(os.path.join(script_dir, "../.."))
    
    dataset_dir = os.path.join(server_dir, "data", "wikisql", f"{dataset_type}_dataset")
    db_dir = os.path.join(dataset_dir, dataset_type)
    
    jsonl_path = os.path.join(dataset_dir, f"{dataset_type}.jsonl")
    tables_jsonl_path = os.path.join(dataset_dir, f"{dataset_type}.tables.jsonl")
    db_path = os.path.join(db_dir, f"{dataset_type}.db")
    output_path = os.path.join(dataset_dir, f"{dataset_type}.json")
    
    # Check if files exist
    for path, desc in [
        (jsonl_path, f"{dataset_type}.jsonl file"),
        (tables_jsonl_path, f"{dataset_type}.tables.jsonl file"),
        (db_path, f"{dataset_type}.db file")
    ]:
        if not os.path.exists(path):
            print(f"Error: {desc} not found: {path}")
            print(f"Make sure you've set up the {dataset_type} dataset properly.")
            return False
    
    # Load tables for lookup
    print(f"Loading tables from {tables_jsonl_path}...")
    tables = {}
    with open(tables_jsonl_path, 'r') as f:
        for line in f:
            table = json.loads(line)
            tables[table['id']] = table
    
    # Initialize the processor
    print(f"Connecting to database {db_path}...")
    processor = WikiSQLProcessor(db_path)
    
    # Process each query
    print(f"Processing queries from {jsonl_path}...")
    dataset = []
    success_count = 0
    error_count = 0
    
    with open(jsonl_path, 'r') as f:
        lines = list(f)
        total = len(lines)
        
        for idx, line in enumerate(lines):
            if (idx + 1) % 100 == 0 or (idx + 1) == total:
                print(f"Progress: {idx + 1}/{total}")
            
            entry = json.loads(line)
            
            question_id = idx + 1
            question = entry['question']
            table_id = entry['table_id']
            wikisql_query = entry['sql']
            
            # Process the query
            result = processor.process_wikisql_query(
                question_id, question, table_id, wikisql_query
            )
            
            if result['success']:
                success_count += 1
                # Create entry in the required format
                dataset_entry = {
                    "question_id": question_id,
                    "db_id": dataset_type,
                    "question": question,
                    "evidence": "",
                    "SQL": result['formatted_sql'],
                    "execution_result": result['execution_result'],
                    "difficulty": ""
                }
                dataset.append(dataset_entry)
            else:
                error_count += 1
                print(f"Error processing query {question_id}: {result['error']}")
    
    # Close the processor
    processor.close()
    
    # Save dataset
    print(f"Saving dataset to {output_path}...")
    with open(output_path, 'w') as f:
        json.dump(dataset, f, indent=2)
    
    # Print summary
    print(f"\\nProcessing Summary for {dataset_type}:")
    print(f"Total queries:      {total}")
    print(f"Successful:         {success_count} ({success_count/total:.2%})")
    print(f"Errors:             {error_count} ({error_count/total:.2%})")
    print(f"Dataset saved with {len(dataset)} entries")
    
    return True

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='WikiSQL Dataset Creator for New Folder Structure')
    parser.add_argument('--dataset', required=True, choices=['dev', 'test'], 
                        help='Which dataset to process (dev or test)')
    
    args = parser.parse_args()
    
    # Process the specified dataset
    process_dataset(args.dataset)
"""

if __name__ == "__main__":
    setup_wikisql()