"""
Prepare and process sample datasets for text-to-SQL tasks.

This module handles the preparation and enrichment of training datasets, primarily focusing
on the BIRD dataset format. It provides functionality for:
- Creating and managing training data files
- Adding unique question IDs to training data
- Enriching data with schema information for SQL queries
- Adding database descriptions using LLM processing
- Managing file paths and dataset types through PATH_CONFIG

The module supports both training and development dataset types and ensures proper
data structure for text-to-SQL model training.
"""

import os
import shutil
from pathlib import Path

from preprocess.add_descriptions_bird_dataset import add_database_descriptions
from tqdm import tqdm
from utilities.bird_utils import (add_sequential_ids_to_questions,
                                  load_json_from_file, save_json_to_file)
from utilities.config import PATH_CONFIG
from utilities.constants.bird_utils.indexing_constants import (DB_ID_KEY,
                                                               QUESTION_ID_KEY,
                                                               SCHEMA_USED,
                                                               SQL)
from utilities.constants.common.error_messages import (ERROR_FILE_NOT_FOUND,
                                                       ERROR_IO,
                                                       UNEXPECTED_ERROR)
from utilities.constants.database_enums import DatasetType
from utilities.constants.preprocess.prepare_sample_dataset.response_messages import (
    ERROR_INVALID_TRAIN_FILE, ERROR_INVALID_TRAIN_FILE_CONTENTS,
    ERROR_USER_KEYBOARD_INTERRUPTION, INFO_SKIPPING_PROCESSED_ITEM,
    INFO_TRAIN_DATA_PROGRESS_SAVED, WARNING_FAILED_TO_ADD_SCHEMA_USED)
from utilities.constants.services.llm_enums import (LLMConfig, LLMType,
                                                    ModelType)
from utilities.generate_schema_used import get_sql_columns_dict
from utilities.logging_utils import setup_logger

logger = setup_logger(__name__)

# String literals For Fish Bar
ADDING_SCHEMA_USED_FIELD = "Adding Schema Used Field On Each Data Item"

def get_train_file_path() -> str:
    """
    Get the path to the train file, creating it if it doesn't exist.

    Uses PATH_CONFIG to determine the correct processed train path based on the
    configured dataset type. If the file doesn't exist at the specified location,
    it will be created automatically.

    Returns:
        str: The absolute path to the processed train file.
    """
    train_file = PATH_CONFIG.processed_train_path()
    if not os.path.exists(train_file):
        create_train_file(train_file)
    
    return train_file

def create_train_file(train_file: str) -> None:
    """
    Create a new train file at the specified path.

    Creates necessary directories and processes the BIRD dataset according to
    PATH_CONFIG.sample_dataset_type. For BIRD_TRAIN datasets, automatically adds
    sequential question IDs to each entry.

    Args:
        train_file: Absolute path where the train file should be created.

    Raises:
        FileNotFoundError: If source dataset files cannot be found
        IOError: If there are issues with file operations
        Exception: For unexpected errors during processing
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
        if isinstance(e, FileNotFoundError):
            raise
        logger.error(ERROR_FILE_NOT_FOUND.format(error=str(e)))
    except IOError as e:
        if isinstance(e, IOError):
            raise
        logger.error(ERROR_IO.format(error=str(e)))
    except Exception as e:
        if isinstance(e, PermissionError):
            raise
        logger.error(UNEXPECTED_ERROR.format(error=str(e)))

def copy_bird_train_file(train_file: str) -> None:
    """
    Copy the BIRD dataset file to the train file path.

    Uses PATH_CONFIG.bird_file_path to locate the source file based on the
    configured dataset type (train/dev/test) and copies it to the specified
    destination.

    Args:
        train_file: Absolute path where the file should be copied to.
    """
    bird_file_path = PATH_CONFIG.bird_file_path(dataset_type=PATH_CONFIG.sample_dataset_type)
    shutil.copyfile(bird_file_path, train_file)
        
def get_train_data(train_file: str) -> list:
    """
    Load and return the complete training data from the specified JSON file.

    Attempts to load the training data JSON file and validate its contents.
    The function expects the file to contain a list of training items with
    appropriate database IDs and other required fields.

    Args:
        train_file: Path to the JSON file containing training data.

    Returns:
        list: Complete list of training data items if successful, None if the
             file is invalid or empty.
    """
    if os.path.exists(train_file):
        train_data = load_json_from_file(train_file)
        if train_data:
            return train_data
        else:
            logger.error(ERROR_INVALID_TRAIN_FILE_CONTENTS)
            return None
    else:
        logger.error(ERROR_INVALID_TRAIN_FILE)
        return None
    
def add_schema_used(train_data: list, dataset_type: str, train_file: Path) -> None:
    """
    Add schema_used field to each item in the train data.

    Processes each training item to add schema information based on its SQL query.
    The schema information is extracted from the corresponding SQLite database
    and includes relevant table and column information.

    Args:
        train_data: List of training data items to be processed
        dataset_type: Type of dataset being processed (e.g., BIRD_TRAIN, BIRD_DEV)

    Notes:
        - Progress is saved after processing each item
        - Skips items that already have schema_used field
        - Uses tqdm for progress tracking
        - Handles keyboard interruptions gracefully
    """
    try:
        for item in tqdm(train_data, desc=ADDING_SCHEMA_USED_FIELD):
            if SCHEMA_USED in item:
                logger.info(INFO_SKIPPING_PROCESSED_ITEM.format(question_id=item[QUESTION_ID_KEY]))
            else:
                try:
                    item[SCHEMA_USED] = get_sql_columns_dict(
                        PATH_CONFIG.sqlite_path(database_name=item[DB_ID_KEY], dataset_type=dataset_type),
                        item[SQL],
                    )
                except Exception as e:
                    logger.warning(WARNING_FAILED_TO_ADD_SCHEMA_USED.format(question_id=item[QUESTION_ID_KEY]))
                    item[SCHEMA_USED] = None
    except KeyboardInterrupt as e:
        if isinstance(e, KeyboardInterrupt):
            raise
        logger.error(ERROR_USER_KEYBOARD_INTERRUPTION)

    finally:
        save_json_to_file(train_file, train_data)
        logger.info(INFO_TRAIN_DATA_PROGRESS_SAVED)

if __name__ == '__main__':
    """
    Prepare and enrich BIRD dataset files with schema information and descriptions.

    To run this script:

    1. Environment Configuration:
       - Set SAMPLE_DATASET_TYPE in .env to either:
         * DatasetType.BIRD_TRAIN for training data
         * DatasetType.BIRD_DEV for development data

    2. Processing Steps:
       - Creates or loads the training file
       - Adds schema information to each query
       - Enriches database descriptions using Google AI (Gemini)

    3. Outputs:
       - Updated JSON file with added "schema_used" field
       - {database_name}_tables.csv with table descriptions
       - Updated {table_name}.csv files with column descriptions
       - Processing logs and error reports

    Notes:
       - Progress is automatically saved during processing
       - Existing schema information is preserved
       - Uses Google AI Gemini 2.0 Flash for description generation
    """
    train_file = get_train_file_path()
    dataset_type = PATH_CONFIG.sample_dataset_type
    train_data = get_train_data(Path(train_file))
    
    if train_data:  
        add_schema_used(train_data, dataset_type, Path(train_file))
        # Need to Updated `add_database_descriptions` to work for sample datasets
        add_database_descriptions(
            dataset_type=dataset_type,
            llm_config=LLMConfig(
                llm_type=LLMType.GOOGLE_AI,
                model_type=ModelType.GOOGLEAI_GEMINI_2_0_FLASH,
                temperature=0.7,
                max_tokens=8192,
            )
        )
    else:
        logger.error(ERROR_INVALID_TRAIN_FILE_CONTENTS)
    