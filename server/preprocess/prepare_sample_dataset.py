"""
Prepare and process sample datasets for text-to-SQL tasks.

This module handles the preparation of training datasets, primarily for the BIRD dataset.
It includes functionality for creating train files, adding question IDs, and enriching
data with schema information needed for text-to-SQL models.
"""

import os
import shutil
from turtle import pd

from preprocess.add_descriptions_bird_dataset import add_database_descriptions
from tqdm import tqdm
from utilities.bird_utils import (add_sequential_ids_to_questions,
                                  load_json_from_file, save_json_to_file)
from utilities.config import PATH_CONFIG
from utilities.connections.common import close_connection
from utilities.connections.sqlite import make_sqlite_connection
from utilities.constants.bird_utils.indexing_constants import (DB_ID_KEY,
                                                               QUESTION_ID_KEY,
                                                               SCHEMA_USED,
                                                               SQL)
from utilities.constants.common.error_messages import (FILE_NOT_FOUND,
                                                       IO_ERROR,
                                                       UNEXPECTED_ERROR)
from utilities.constants.database_enums import DatasetType
from utilities.constants.LLM_enums import LLMType, ModelType
from utilities.constants.preprocess.prepare_sample_dataset.response_messages import (
    ERROR_INVALID_TRAIN_FILE, ERROR_INVALID_TRAIN_FILE_CONTENTS,
    ERROR_USER_KEYBOARD_INTERRUPTION, INFO_SKIPPING_PROCESSED_ITEM,
    INFO_TRAIN_DATA_PROGRESS_SAVED)
from utilities.generate_schema_used import get_sql_columns_dict
from utilities.logging_utils import setup_logger

logger = setup_logger(__name__)

connection = None

# String literals For Fish Bar
ADDING_SCHEMA_USED_FIELD = "Adding Schema Used Field On Each Data Item"

def get_train_file_path():
    """
    Get the path to the train file, creating it if it doesn't exist.

    Returns:
        str: The path to the processed train file.
    """
    train_file = PATH_CONFIG.processed_train_path()
    if not os.path.exists(train_file):
        create_train_file(train_file)
    
    return train_file

def create_train_file(train_file: str) -> None:
    """
    Create a new train file at the specified path.

    Creates necessary directories, copies content from source files, and adds
    question IDs for BIRD train datasets if required.

    Args:
        train_file: Path where the train file should be created.
    """
    try:
        # Ensure the directory exists
        os.makedirs(os.path.dirname(train_file), exist_ok=True)
        # Copy contents from files ( dev.json / train.json / test.json ) to processed_train_path
        copy_bird_train_file(train_file)
        # Add question id for bird train as train does not have that for each question
        if PATH_CONFIG.sample_dataset_type == DatasetType.BIRD_TRAIN:
            add_sequential_ids_to_questions(train_file)
    except FileNotFoundError as e:
        logger.error(FILE_NOT_FOUND.format(error=str(e)))
    except IOError as e:
        logger.error(IO_ERROR.format(error=str(e)))
    except Exception as e:
        logger.error(UNEXPECTED_ERROR.format(error=str(e)))

def copy_bird_train_file(train_file: str) -> None:
    """
    Copy the BIRD dataset file to the train file path.

    Args:
        train_file: Destination path for the copied file.
    """
    bird_file_path = PATH_CONFIG.bird_file_path(dataset_type=PATH_CONFIG.sample_dataset_type)
    shutil.copyfile(bird_file_path, train_file)
    
        
def create_database_connection(database_name: str, dataset_type: str) -> None:
    """
    Create a database connection using SQLite.
    
    Attempts to close any existing connection before creating a new one
    to the specified database.
    
    Args:
        database_name: Name of the database to connect to.
        dataset_type: Type of dataset being processed (e.g., BIRD_TRAIN).
        
    Returns:
        A SQLite database connection object.
        
    Raises:
        Exception: If connection cannot be established.
    """
    global connection
    try:
        close_connection(connection)
        connection = make_sqlite_connection(
            PATH_CONFIG.sqlite_path(database_name=database_name, dataset_type=dataset_type)
        )
    except Exception as exception:
        logger.error(UNEXPECTED_ERROR.format(error=str(exception)))
        raise exception
    
    return connection

def fetch_database(train_file: str) -> None:
    """Fetch database ID from the first item in the training data file.
    
    This function attempts to load a JSON file and extract the database ID
    from the first item in the training data. If the file doesn't exist
    or contains invalid data, an error is logged and None is returned.
    
    Args:
        train_file: Path to the JSON file containing training data.
        
    Returns:
        str: The database ID from the first item in the train data if successful,
             None otherwise.
    """
    if os.path.exists(train_file):
        train_data = load_json_from_file(train_file)
        if train_data:
            return train_data[0][DB_ID_KEY]
        else:
            logger.error(ERROR_INVALID_TRAIN_FILE_CONTENTS)
            return None
    else:
        logger.error(ERROR_INVALID_TRAIN_FILE)
        return None
    

def add_schema_used(train_data: pd.DataFrame, dataset_type: str) -> None:
    """
    Add schema_used field to each item in the train file.

    Processes each item in the train data, adding schema information based on the
    SQL queries. Handles database connections and updates the train file with 
    the enriched data.

    Args:
        train_data: train data as a json
        dataset_type: Type of dataset being processed.
    """
    global connection
    try:
        for item in tqdm(train_data, desc=ADDING_SCHEMA_USED_FIELD):
            if SCHEMA_USED in item:
                logger.info(INFO_SKIPPING_PROCESSED_ITEM.format(question_id=item[QUESTION_ID_KEY]))
            else:
                # if the db changes then delete previous connection and connect to new one
                if current_db != item[DB_ID_KEY]:
                    connection = create_database_connection(database_name=item[DB_ID_KEY], dataset_type=dataset_type)
                    current_db = item[DB_ID_KEY]

                item[SCHEMA_USED] = get_sql_columns_dict(
                    PATH_CONFIG.sqlite_path(database_name=item[DB_ID_KEY], dataset_type=dataset_type),
                    item[SQL],
                )
    except KeyboardInterrupt:
        logger.error(ERROR_USER_KEYBOARD_INTERRUPTION)
        
    finally:
        close_connection(connection)
        save_json_to_file(train_file, train_data)
        logger.info(INFO_TRAIN_DATA_PROGRESS_SAVED)

if __name__ == '__main__':
    """
    To run this script:

    1. Ensure you have set the correct SAMPLE_DATASET_TYPE in .env:
       - Set SAMPLE_DATASET_TYPE to DatasetType.BIRD_TRAIN for training data.
       - Set SAMPLE_DATASET_TYPE to DatasetType.BIRD_DEV for development data.

    4. Expected Outputs:
       - The test data JSON file(s) will be updated in-place with an added "schema_used" field for each processed item.
       - Progress and any errors will be logged to the console.
       - Generates a {database_name}_tables.csv file for each database with table descriptions. 
       - Updated {table_name}.csv with improved column descriptions for each schema.
       - Detailed logs for each database processed, including errors (if any).
    """
    train_file = get_train_file_path()
    dataset_type = PATH_CONFIG.sample_dataset_type
    train_data = fetch_database(train_file)
    
    if train_data:
        database = train_data[0][DB_ID_KEY]
        connection = create_database_connection(
            database_name=database, 
            dataset_type=PATH_CONFIG.sample_dataset_type
        )
        
        if connection:
            add_schema_used(train_data, dataset_type)
            # TODO 
            # 1. Convert the arguments into LLMConfig dataclass
            # 2. The `add_Description_bird_dataset.py`` needs to be changed to accommodate this change.
            # 3. Need to Updated add_database__descriptions to work for sample datasets
            add_database_descriptions(
                dataset_type=dataset_type,
                llm_type=LLMType.GOOGLE_AI,
                model=ModelType.GOOGLEAI_GEMINI_2_0_FLASH,
                temperature=0.7,
                max_tokens=8192,
            )
    else:
        logger.error(ERROR_INVALID_TRAIN_FILE_CONTENTS)
    