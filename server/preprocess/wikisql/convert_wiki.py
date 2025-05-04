import json
import os
import sys
import re
import sqlite3
from collections import defaultdict

class WikiSQLProcessor:
    """Process WikiSQL queries and convert them to executable SQL"""
    
    # Define constants copied from the original WikiSQL codebase
    AGG_OPS = ['', 'MAX', 'MIN', 'COUNT', 'SUM', 'AVG']
    COND_OPS = ['=', '>', '<', 'OP']
    
    def __init__(self, db_path):
        """Initialize with SQLite database path"""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        
        # Set up regex patterns
        self.schema_re = re.compile(r'\((.+)\)')
        self.num_re = re.compile(r'[-+]?\d*\.\d+|\d+')
    
    def close(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()
    
    def get_table_schema(self, table_id):
        """Get the schema for a table"""
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
        """Convert WikiSQL query format to SQL"""
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
        """Format SQL query by substituting parameters"""
        formatted = sql
        for name, value in params.items():
            if isinstance(value, str):
                formatted = formatted.replace(f":{name}", f"'{value}'")
            else:
                formatted = formatted.replace(f":{name}", str(value))
        return formatted
    
    def execute_query(self, sql, params):
        """Execute SQL query with parameters and return results"""
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
        """Process a complete WikiSQL query and return formatted results"""
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
    """Process a dataset with the new folder structure"""
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
    print(f"\nProcessing Summary for {dataset_type}:")
    print(f"Total queries:      {total}")
    print(f"Successful:         {success_count} ({success_count/total:.2%})")
    print(f"Errors:             {error_count} ({error_count/total:.2%})")
    print(f"Dataset saved with {len(dataset)} entries")
    
    return True

if __name__ == "__main__":
    """
    WikiSQL Converter for SQLs

    This script processes WikiSQL data in the new folder structure and creates
    properly formatted JSON dataset files that converts it into a conventional format for SQLs and processing.

    Before: (jsonl file)
    "sql": {"sel": 3, "conds": [[5, 0, "Butler CC (KS)"]], "agg": 0}

    After: (json file)
    "SQL": "SELECT col2 AS result FROM table_1_10015132_9 WHERE col5 = 'fresno state'",


    Usage:
        python3 -m preprocess.wikisql.convert_wiki --dataset dev
        python3 -m preprocess.wikisql.convert_wiki --dataset test
        python3 -m preprocess.wikisql.convert_wiki --dataset all
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='WikiSQL Dataset Creator for New Folder Structure')
    parser.add_argument('--dataset', required=True, choices=['dev', 'test', 'all'], 
                        help='Which dataset to process (dev, test, or all)')
    
    args = parser.parse_args()
    
    # Process the specified dataset(s)
    if args.dataset == 'all':
        print("Processing dev dataset...")
        process_dataset('dev')
        print("\nProcessing test dataset...")
        process_dataset('test')
    else:
        process_dataset(args.dataset)

    # Print instructions for running the next script
    print("\nRun the next script to preprocess the wikisql dataset and prepare it for the text2sql solution:")
    print("python3 -m preprocess.wikisql.prepare_wiki --dataset dev")
    print("python3 -m preprocess.wikisql.prepare_wiki --dataset test")
    print("python3 -m preprocess.wikisql.prepare_wiki --dataset all")
