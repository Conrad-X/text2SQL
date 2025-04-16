from utilities.utility_functions import (
    execute_sql_timeout
)
from utilities.logging_utils import setup_logger
import os
import pandas as pd
from utilities.config import PATH_CONFIG
import threading

logger = setup_logger(__name__)

def get_dict(database, file_path, columns):
    if os.path.exists(file_path):
        df = pd.read_csv(file_path, sep='\t', index_col=False)
        df_dict = df.set_index("database").to_dict(orient='index')
        if database not in list(df_dict.keys()):
            df_dict[database] = {str(i): 0 for i in columns}
    else:
        df_dict={database : {str(i): 0 for i in columns}}
    return df_dict

def save_df(data_dict, file_path):
    df = pd.DataFrame.from_dict(data_dict, orient='index')

    # Reset index and rename columns
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'database'}, inplace=True)

    # Save to CSV
    df.to_csv(file_path, sep='\t', index=False)

def compare_sql_result(gold_sql: str, candidate_sql: str, database: str, gold_res = None) -> tuple:
    """
    Compare the results of the gold SQL query and the candidate SQL query.
    
    Args:
        gold_sql (str): The gold SQL query.
        candidate_sql (str): The candidate SQL query.
        database (str): The database name.
        gold_res (list, optional): The result of the gold SQL query. Defaults to None.

    Returns:
        tuple: A tuple containing a boolean indicating if the results are the same and the result of the gold SQL query.
    """

    if not gold_res:
        try:
            gold_res = execute_sql_timeout(database, sql_query=gold_sql)
        except Exception as e:
            logger.critical(f"ERROR IN GOLD SQL: {e}")

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

    def __init__(self, run_config, database):
        self.run_config = run_config
        self.database = database
        self.correct_gen_dict = get_dict(database, PATH_CONFIG.correct_generated_file(), [i+1 for i in range(len(run_config))])
        self.config_sel_dict = get_dict(database, PATH_CONFIG.config_selected_file(), [i+1 for i in range(len(run_config))])
        self.correct_sel_dict = get_dict(database, PATH_CONFIG.correct_selected_file(), ['correct_selected', 'correct_generated'])
        self.lock = threading.Lock()
    
    def change_database(self, database):
        self.database = database
        self.correct_gen_dict = get_dict(database, PATH_CONFIG.correct_generated_file(), [i+1 for i in range(len(self.run_config))])
        self.config_sel_dict = get_dict(database, PATH_CONFIG.config_selected_file(), [i+1 for i in range(len(self.run_config))])
        self.correct_sel_dict = get_dict(database, PATH_CONFIG.correct_selected_file(), ['correct_selected', 'correct_generated'])

    def update_selection_metadata(self, candidates, gold_sql, database, selected_config):
        with self.lock:
            if database != self.database:
                self.change_database(database)

            gold_res = None
            correct_gen = []

            for  sql, id in candidates:
                same, gold_res = compare_sql_result(gold_sql, sql, self.database, gold_res)
                if same:
                    self.correct_gen_dict[database][str(id)]+=1
                    correct_gen.append(id)

            self.config_sel_dict[database][str(selected_config)]+=1
            if len(correct_gen) > 0:
                self.correct_sel_dict[database]['correct_generated']+=1

            if selected_config in correct_gen:
                self.correct_sel_dict[database]['correct_selected']+=1

    def save_metadata(self):
        with self.lock:
            save_df(self.correct_sel_dict, PATH_CONFIG.correct_selected_file())
            save_df(self.config_sel_dict, PATH_CONFIG.config_selected_file())
            save_df(self.correct_gen_dict, PATH_CONFIG.correct_generated_file())
