import json
import shutil
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List

from utilities.config import PATH_CONFIG
from utilities.constants.bird_utils.indexing_constants import (DB_ID_KEY,
                                                               QUESTION_ID_KEY)
from utilities.constants.bird_utils.response_messages import (
    ERROR_FILE_DECODE, ERROR_FILE_NOT_FOUND, ERROR_FILE_READ, ERROR_FILE_SAVE,
    ERROR_JSON_DECODE, ERROR_MISSING_DB_ID, ERROR_PATH_NOT_DIRECTORY,
    ERROR_PATH_NOT_EXIST, ERROR_EMPTY_BIRD_ITEMS_LIST)
from utilities.constants.database_enums import DatasetType

# Constants
JSON_FILE_ENCODING = 'utf-8'


def load_json_from_file(file_path: Path) -> List[Dict]:
    """
    Load JSON data from the provided file.

    Args:
        file_path (Path): The path to the JSON file.

    Returns:
        List[Dict]: The parsed list of questions from the JSON file.

    Raises:
        ValueError: If the file content is not valid JSON.
        FileNotFoundError: If the file doesn't exist.
        UnicodeDecodeError: If an encoding issue occurs.
    """
    try:
        return json.loads(file_path.read_text(encoding=JSON_FILE_ENCODING))
    except json.JSONDecodeError:
        raise ValueError(ERROR_JSON_DECODE.format(file_path=file_path))
    except FileNotFoundError:
        raise FileNotFoundError(ERROR_FILE_NOT_FOUND.format(file_path=file_path))
    except UnicodeDecodeError as e:
        raise UnicodeDecodeError(ERROR_FILE_DECODE.format(file_path=file_path, error=str(e)))
    except Exception as e:
        raise Exception(ERROR_FILE_READ.format(file_path=file_path, error=str(e)))


def save_json_to_file(file_path: Path, data: List[Dict]) -> None:
    """
    Save the annotated JSON data back to the file.

    Args:
        file_path (Path): The path to the file.
        data (List[Dict]): The data to save.
    """
    try:
        file_path.write_text(json.dumps(data, indent=4), encoding=JSON_FILE_ENCODING)
    except Exception as e:
        raise Exception(ERROR_FILE_SAVE.format(file_path=file_path, error=str(e)))


def add_sequential_ids_to_questions(file_path: Path) -> None:
    """
    Add sequential question_id fields for BIRD training data.

    Args:
        file_path (Path): Path to the JSON file to be annotated.

    Raises:
        ValueError: If the loaded bird items list is empty.
    """
    # Load the data from the file
    bird_items = load_json_from_file(file_path)
    if not bird_items:
        raise ValueError(ERROR_EMPTY_BIRD_ITEMS_LIST)

    # Annotate the questions with sequential IDs
    annotated_questions = [
        {QUESTION_ID_KEY: index, **item}
        for index, item in enumerate(bird_items)
    ]

    # Save the modified data back to the file
    save_json_to_file(file_path, annotated_questions)


def group_bird_items_by_database_name(bird_items: List[Dict[str, Any]]) -> Dict[str, List[int]]:
    """
    Group BIRD dataset items by their associated database (db_id), returning a mapping from
    database name to list of item indices.

    Args:
        data (List[Dict[str, Any]]): List of BIRD data items. Each must include a 'db_id' key.

    Returns:
        Dict[str, List[int]]: Maps each 'db_id' to a list of indices where it appears.

    Raises:
        ValueError: If the input bird items list is empty.
        ValueError: If any item is missing the 'db_id' field.
    """
    if not bird_items:
        raise ValueError(ERROR_EMPTY_BIRD_ITEMS_LIST)

    indices_grouped_by_database_name: Dict[str, List[int]] = defaultdict(list)

    for index, item in enumerate(bird_items):
        if DB_ID_KEY not in item:
            raise ValueError(ERROR_MISSING_DB_ID.format(index=index))
        indices_grouped_by_database_name[item[DB_ID_KEY]].append((item))

    return dict(indices_grouped_by_database_name)


def get_database_list(dataset_directory: Path) -> List[str]:
    """
    Retrieve the names of all database directories within the given dataset directory.

    Args:
        dataset_directory (Path): Path to the root folder containing database directories.

    Returns:
        List[str]: List of database directory names.

    Raises:
        ValueError: If the provided path does not exist or is not a directory.
    """
    if not dataset_directory.exists():
        raise ValueError(ERROR_PATH_NOT_EXIST.format(dataset_directory=dataset_directory))
    if not dataset_directory.is_dir():
        raise ValueError(ERROR_PATH_NOT_DIRECTORY.format(dataset_directory=dataset_directory))

    return [
        database_path.name
        for database_path in dataset_directory.iterdir()
        if database_path.is_dir()
    ]


def get_global_bird_test_file(test_file: Path) -> Path:
    """
    Ensures that the specified test file exists.
    If it doesn't, it will copy it from the source and annotate it if needed.

    Args:
        test_file (Path): The path to the test file that needs to be ensured.

    Returns:
        Path: The path to the test file, ensuring its existence.
    """
    if test_file.exists():
        return test_file

    return create_and_copy_test_file(test_file)


def create_and_copy_test_file(test_file: Path) -> Path:
    """
    Helper function to create the test file by copying it from the source.

    Args:
        test_file (Path): The path to the test file to be created.

    Returns:
        Path: The path to the newly created test file.
    """
    source = PATH_CONFIG.bird_file_path()
    test_file.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(source, test_file)
    if PATH_CONFIG.sample_dataset_type == DatasetType.BIRD_TRAIN:
        add_sequential_ids_to_questions(test_file)

    return test_file
