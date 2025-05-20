"""Prepare and process sample datasets for text-to-SQL tasks.

This module handles the preparation of training datasets, primarily for the BIRD dataset.
It includes functionality for creating train files, adding question IDs, and enriching
data with schema information needed for text-to-SQL models.
"""

import json
import os
import shutil

from alive_progress import alive_bar
from preprocess.add_descriptions_bird_dataset import add_database_descriptions
from utilities.config import PATH_CONFIG
from utilities.constants.database_enums import DatasetType
from utilities.constants.LLM_enums import LLMType, ModelType
from utilities.generate_schema_used import get_sql_columns_dict

from text2SQL.server.utilities.bird_utils import \
    add_sequential_ids_to_questions
from text2SQL.server.utilities.connections.common import close_connection
from text2SQL.server.utilities.connections.sqlite import make_sqlite_connection
from text2SQL.server.utilities.constants.common.error_messages import (
    FILE_NOT_FOUND, IO_ERROR, UNEXPECTED_ERROR)
from text2SQL.server.utilities.constants.preprocess.prepare_sample_dataset.indexing_constants import (
    DB_ID, QUESTION_ID, SCHEMA_USED, SQL)
from text2SQL.server.utilities.constants.preprocess.prepare_sample_dataset.response_messages import (
    FINAL_CLEANUP_COMPLETED, SKIPPING_PROCESSED_ITEM,
    TRAIN_DATA_PROGRESS_SAVED, USER_KEYBOARD_INTERRUPION)
from text2SQL.server.utilities.logging_utils import setup_logger

logger = setup_logger(__name__)

# String literals For Fish Bar
BAR_TYPE="fish"
BAR_SPINNER="fish2"
BAR_TITLE="Adding Schema Used in items"

def get_train_file_path():
    """Get the path to the train file, creating it if it doesn't exist.

    Returns:
        str: The path to the processed train file.
    """
    train_file = PATH_CONFIG.processed_train_path()
    if not os.path.exists(train_file):
        create_train_file(train_file)
    
    return train_file

def create_train_file(train_file):
    """Create a new train file at the specified path.

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

def copy_bird_train_file(train_file):
    """Copy the BIRD dataset file to the train file path.

    Args:
        train_file: Destination path for the copied file.
    """
    bird_file_path = PATH_CONFIG.bird_file_path(dataset_type=PATH_CONFIG.sample_dataset_type)
    shutil.copyfile(bird_file_path, train_file)

def add_schema_used(train_file, dataset_type):
    """Add schema_used field to each item in the train file.

    Processes each item in the train data, adding schema information based on the
    SQL queries. Handles database connections and updates the train file with 
    the enriched data.

    Args:
        train_file: Path to the train file to process.
        dataset_type: Type of dataset being processed.
    """
    with open(train_file, "r") as file:
        train_data = json.load(file)

    current_db = train_data[0][DB_ID]
    connection = make_sqlite_connection(
        PATH_CONFIG.sqlite_path(database_name=current_db, dataset_type=dataset_type)
    )

    with alive_bar(
        len(train_data),
        bar=BAR_TYPE,
        spinner=BAR_SPINNER,
        title=BAR_TITLE,
    ) as bar:

        try:
            for item in train_data:
                if SCHEMA_USED in item:
                    logger.info(SKIPPING_PROCESSED_ITEM.format(question_id=item[QUESTION_ID]))
                    bar()
                else:
                    # if the db changes then delete previous connection and connect to new one
                    if current_db != item[DB_ID]:
                        close_connection(connection)
                        connection = make_sqlite_connection(
                            PATH_CONFIG.sqlite_path(database_name=item[DB_ID], dataset_type=dataset_type)
                        )
                        current_db = item[DB_ID]

                    item[SCHEMA_USED] = get_sql_columns_dict(
                        PATH_CONFIG.sqlite_path(database_name=item[DB_ID], dataset_type=dataset_type),
                        item[SQL],
                    )
                    bar()
        except KeyboardInterrupt:
            logger.error(USER_KEYBOARD_INTERRUPION)
            
        finally:
            close_connection(connection)
            write_train_data_to_file(train_file, train_data)
            logger.info(TRAIN_DATA_PROGRESS_SAVED)

def write_train_data_to_file(train_file, train_data):
    """Write processed train data to the specified file.

    Args:
        train_file: Path to the file where train data should be written.
        train_data: The processed data to be written to the file.
    """
    with open(train_file, "w") as file:
        json.dump(train_data, file, indent=4)

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

    add_schema_used(get_train_file_path(), PATH_CONFIG.sample_dataset_type)

    # TODO - convert the arguments into LLMConfig dataclass
    # the `add_Description_bird_dataset.py`` needs to be changed to accommodate this change.
    add_database_descriptions(
        dataset_type=PATH_CONFIG.sample_dataset_type,
        llm_type=LLMType.GOOGLE_AI,
        model=ModelType.GOOGLEAI_GEMINI_2_0_FLASH,
        temperature=0.7,
        max_tokens=8192,
    )