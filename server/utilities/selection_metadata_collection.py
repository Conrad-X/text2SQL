import os
import threading
from pathlib import Path

import pandas as pd
from utilities.config import PATH_CONFIG
from utilities.logging_utils import setup_logger
from utilities.utility_functions import execute_sql_timeout

logger = setup_logger(__name__)

def get_dict(database: str, file_path: Path, columns: list) -> dict:
    """
    Load a CSV file into a nested dictionary keyed by database, initializing missing entries with zeros.
    """

    if os.path.exists(file_path):
        # Load the CSV file (tab-separated) into a DataFrame
        df = pd.read_csv(file_path, sep='\t', index_col=False)

        # Convert the DataFrame into a dictionary with 'database' as the key
        df_dict = df.set_index("database").to_dict(orient='index')

        # If the specified database is not present, initialize it with zero values for each column
        if database not in df_dict:
            df_dict[database] = {str(i): 0 for i in columns}
    else:
        # If the file doesn't exist, create a new dictionary for the given database with zero values
        df_dict = {database: {str(i): 0 for i in columns}}

    return df_dict


def save_df(data_dict: dict, file_path: Path) -> None:
    """
    Save a nested dictionary to a TSV file with 'database' as the index column.
    """

    df = pd.DataFrame.from_dict(data_dict, orient='index')

    # Reset index and rename columns
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'database'}, inplace=True)

    # Save to CSV
    df.to_csv(file_path, sep='\t', index=False)

def compare_sql_result(gold_sql: str, candidate_sql: str, database: str, gold_res = None) -> tuple:
    """
    Compare the results of the gold SQL query and the candidate SQL query.
    """

    if gold_res is None:
        try:
            gold_res = execute_sql_timeout(database, sql_query=gold_sql)
        except Exception as e:
            logger.critical(f"Error in Gold SQL: {e}")

    try:
        res = execute_sql_timeout(database, candidate_sql)
        if set(res) == set(gold_res):
            return True, gold_res
    except Exception as e:
        logger.error(f"Error in Candidate SQL {e}")
        
    return False, gold_res

class SelectionMetadata:
    """
    A class to manage and update selection metadata for SQL queries.

    Attributes:
        run_config (list): Configuration for the run.
        database (str): The name of the current database.
        correct_gen_dict (dict): Dictionary to store correct generation metadata.
        config_sel_dict (dict): Dictionary to store configuration selection metadata.
        correct_sel_dict (dict): Dictionary to store correct selection metadata.
        lock (threading.Lock): A lock to ensure thread safety when updating metadata.

    Methods:
        __init__(self, run_config, database):
            Initializes the SelectionMetadata object with the given run configuration and database.
            
        change_database(self, database):
            Changes the database and reloads the metadata dictionaries for the new database.

        update_selection_metadata(self, candidates, gold_sql, database, selected_config):
            Updates the metadata based on the comparison of candidate SQL queries with the gold SQL query.
            This method is thread-safe.

        save_metadata(self):
            Saves the current metadata dictionaries to their respective files.
            This method is thread-safe.
    """

    def __init__(self, run_config: list, database: str) -> None:
        self.run_config = run_config
        self.database_name = database
        self.config_sel_dict, self.correct_gen_dict, self.correct_sel_dict = None, None, None
        self.load_metadata(database)
        self.lock = threading.Lock()
    
    def load_metadata(self, database: str) -> None:
        self.database_name = database
        self.correct_gen_dict = get_dict(database, PATH_CONFIG.correct_generated_file(), [i+1 for i in range(len(self.run_config))])
        self.config_sel_dict = get_dict(database, PATH_CONFIG.config_selected_file(), [i+1 for i in range(len(self.run_config))])
        self.correct_sel_dict = get_dict(database, PATH_CONFIG.correct_selected_file(), ['correct_selected', 'correct_generated'])

    def update_selection_metadata(self, candidates: list, gold_sql: str, database: str, selected_config: int) -> None:
        with self.lock:
            if database != self.database_name:
                self.load_metadata(database)

            gold_res = None
            correct_gen = []

            for  sql, id in candidates:
                results_match, gold_res = compare_sql_result(gold_sql, sql, database, gold_res)
                if results_match:
                    self.correct_gen_dict[database][str(id)]+=1
                    correct_gen.append(id)

            self.config_sel_dict[database][str(selected_config)]+=1
            if len(correct_gen) > 0:
                self.correct_sel_dict[database]['correct_generated']+=1

            if selected_config in correct_gen:
                self.correct_sel_dict[database]['correct_selected']+=1

    def save_metadata(self) -> None:
        with self.lock:
            save_df(self.correct_sel_dict, PATH_CONFIG.correct_selected_file())
            save_df(self.config_sel_dict, PATH_CONFIG.config_selected_file())
            save_df(self.correct_gen_dict, PATH_CONFIG.correct_generated_file())
