import json
import shutil
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from utilities.config import PATH_CONFIG
from utilities.constants.bird_utils.indexing_constants import (DB_ID_KEY,
                                                               QUESTION_ID_KEY)
from utilities.constants.bird_utils.response_messages import (
    ERROR_EMPTY_BIRD_ITEMS_LIST, ERROR_FAILED_TO_READ_CSV, ERROR_FILE_DECODE,
    ERROR_FILE_NOT_FOUND, ERROR_FILE_READ, ERROR_FILE_SAVE, ERROR_JSON_DECODE,
    ERROR_MISSING_DB_ID, ERROR_PATH_NOT_DIRECTORY, ERROR_PATH_NOT_EXIST,
    WARNING_DATABASE_DESCRIPTION_FILE_NOT_FOUND, WARNING_ENCODING_FAILED,
    WARNING_TABLE_DESCRIPTION_FILE_NOT_FOUND)
from utilities.constants.common.indexing_constants import (
    COLUMN_DESCRIPTION_COL, COLUMNS_KEY, IMPROVED_COLUMN_DESCRIPTIONS_COL,
    ORIG_COLUMN_NAME_COL, TABLE_DESCRIPTION_STR, TABLE_NAME_COL)
from utilities.constants.database_enums import DatasetType
from utilities.logging_utils import setup_logger

logger = setup_logger(__name__)

# Constants
JSON_FILE_ENCODING = 'utf-8'
DESCRIPTION_FILE_EXTENSION = ".csv"
DESCRIPTION_PLACEHOLDER = ""


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
        raise ValueError(ERROR_FILE_DECODE.format(file_path=file_path, error=e.reason))
    except Exception as e:
        raise RuntimeError(ERROR_FILE_READ.format(file_path=file_path, error=str(e)))


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
        raise RuntimeError(ERROR_FILE_SAVE.format(file_path=file_path, error=str(e)))


def add_sequential_ids_to_questions(file_path: Path) -> None:
    """
    Add sequential question_id fields for BIRD training data.

    Args:
        file_path (Path): Path to the JSON file to be annotated.

    Raises:
        ValueError: If the loaded bird items list is empty.
    """
    # Load the bird items list from the file
    bird_items = load_json_from_file(file_path)
    if not bird_items:
        raise ValueError(ERROR_EMPTY_BIRD_ITEMS_LIST)

    # Annotate the questions with sequential IDs
    annotated_questions = [
        {QUESTION_ID_KEY: index, **item}
        for index, item in enumerate(bird_items)
    ]

    # Save the modified bird items list back to the file
    save_json_to_file(file_path, annotated_questions)


def group_bird_items_by_database_name(bird_items: List[Dict[str, Any]]) -> Dict[str, List[int]]:
    """
    Group BIRD dataset items by their associated database (db_id), returning a mapping from database name to list of item indices.

    Args:
        bird_items (List[Dict[str, Any]]): List of BIRD question items. Each must include a 'db_id' key.

    Returns:
        Dict[str, List[int]]: Maps each 'db_id' to a list of indices where it appears.

    Raises:
        ValueError: If the input bird items list is empty.
        ValueError: If any item is missing the 'db_id' field.
    """
    if not bird_items:
        raise ValueError(ERROR_EMPTY_BIRD_ITEMS_LIST)

    items_grouped_by_database_name: Dict[str, List[dict]] = defaultdict(list)

    for index, item in enumerate(bird_items):
        if DB_ID_KEY not in item:
            raise ValueError(ERROR_MISSING_DB_ID.format(index=index))
        items_grouped_by_database_name[item[DB_ID_KEY]].append((item))

    return dict(items_grouped_by_database_name)


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


def ensure_global_bird_test_file_path(test_file: Path) -> Path:
    """
    Ensure that the specified test file exists.

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
    Create the test file by copying it from the source.

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

def read_csv(
    file_path: str, encodings: list = ["utf-8-sig", "ISO-8859-1"]
) -> pd.DataFrame:
    """
    Attempts to read a CSV file using a list of potential encodings.

    This function tries to load a CSV file by iterating through the provided list of encodings.
    If reading with a particular encoding fails (e.g., due to a UnicodeDecodeError), it logs a
    warning and tries the next encoding. If none of the encodings succeed, it raises a ValueError.

    Args:
        file_path (str): The path to the CSV file.
        encodings (list, optional): A list of text encodings to try. Defaults to ["utf-8-sig", "ISO-8859-1"].

    Returns:
        pd.DataFrame: The data loaded from the CSV file.

    Raises:
        ValueError: If the file could not be read using any of the specified encodings.
    """

    for encoding in encodings:
        try:
            return pd.read_csv(file_path, encoding=encoding)
        except UnicodeDecodeError as e:
            logger.warning(WARNING_ENCODING_FAILED.format(
                encoding=encoding, e=e))
        except FileNotFoundError:
            raise FileNotFoundError(
                ERROR_FILE_NOT_FOUND.format(file_path=file_path))

    raise ValueError(ERROR_FAILED_TO_READ_CSV.format(file_path=file_path))


def load_table_description_file(
    description_dir: Path, table_name: str
) -> Optional[pd.DataFrame]:
    """
    Load a table description file from the specified directory.

    Args:
        description_dir (Path): The directory where the description files are stored.
        table_name (str): The name of the table for which the description file is to be loaded.

    Returns:
        Union[None, pd.DataFrame]: A DataFrame containing the table description if the file is found,
        otherwise None.

    Notes:
        - The function first checks for a file named exactly as the table name with the description file extension.
        - If the file is not found, it checks for a file named as the table name in lowercase with the description file extension.
        - If neither file is found, a warning is logged and the function returns None.
    """
    filenames = [table_name, table_name.lower()]

    for name in filenames:
        file_path = description_dir / f"{name}{DESCRIPTION_FILE_EXTENSION}"
        if file_path.exists():
            return pd.read_csv(file_path)

    logger.warning(
        WARNING_TABLE_DESCRIPTION_FILE_NOT_FOUND.format(
            file_path=f"{description_dir}/{table_name}{DESCRIPTION_FILE_EXTENSION}"
        )
    )
    return None


def get_longest_description(row: pd.Series) -> str:
    """
    Return the longest description between the original and improved descriptions.

    Parameters:
    row (pd.Series): A row from the DataFrame containing the descriptions.

    Returns:
    str: The longest description.
    """
    original_description = str(row[COLUMN_DESCRIPTION_COL] or "")
    improved_description = str(row[IMPROVED_COLUMN_DESCRIPTIONS_COL] or "")
    return max(original_description, improved_description, key=len)


def get_longest_description_series(table_description_df: Optional[pd.DataFrame]) -> pd.Series:
    """
    Return a pandas Series containing the longest description for each column in the input DataFrame.

    Parameters:
    table_description_df (Union[pd.DataFrame, None]): A DataFrame containing column descriptions.
                                                     If None, an empty Series is returned.

    Returns:
    pd.Series: A Series where the index is the column names and the values are the longest descriptions.
    """
    if table_description_df is None:
        return pd.Series()

    # Isolate the description columns
    description_df = table_description_df.dropna(
        subset=[COLUMN_DESCRIPTION_COL, IMPROVED_COLUMN_DESCRIPTIONS_COL],
        how="all",
    )

    # From the isolated the series choose the longest description
    longest_description_series = description_df.apply(
        get_longest_description, axis=1)

    # Set the index to the column names
    longest_description_series.index = description_df[ORIG_COLUMN_NAME_COL].values

    return longest_description_series


def generate_description_dict(
    database_name: str, dataset_type: DatasetType, schema_dict: Dict[str, List]
) -> Dict[str, Dict]:
    """
    Generate a dictionary containing descriptions for tables and their columns based on the provided schema.

    Returns a dictionary as follows:

        {
            table_name:{
                "table_description": table description,
                "columns":
                    {
                        "column_name": column description,
                        "column_name": column description,
                        ...
                    }
            ...
        }

    Args:
        database_name (str): The name of the database.
        dataset_type (DatasetType): The type of the dataset.
        schema_dict (Dict[str, List]): A dictionary where keys are table names and values are lists of column names.

    Returns:
        Dict[str, Dict]: A dictionary where keys are table names and values are dictionaries containing:
            - 'table_description': A string describing the table.
            - 'columns': A dictionary where keys are column names and values are strings describing the columns.

    The function reads table and column descriptions from CSV files located in a specified directory. If the description
    files are not found, it uses placeholder descriptions. The descriptions are formatted to replace newline characters
    with spaces.
    """
    description_directory = PATH_CONFIG.description_dir(
        database_name=database_name, dataset_type=dataset_type
    )

    # Load table descriptions
    db_descriptions_path = PATH_CONFIG.table_description_file(
        database_name=database_name, dataset_type=dataset_type)

    try:
        tables_df = read_csv(file_path=db_descriptions_path)
    except FileNotFoundError:
        logger.warning(
            WARNING_DATABASE_DESCRIPTION_FILE_NOT_FOUND.format(
                file_path=db_descriptions_path
            )
        )
        tables_df = None

    description_dict = {}
    for table in schema_dict.keys():

        table_description_df = load_table_description_file(
            description_dir=description_directory, table_name=table
        )
        longest_description_series = get_longest_description_series(
            table_description_df=table_description_df)

        # if table description file is not read, replace table description with placeholder
        if tables_df is not None:
            table_description = tables_df.loc[
                    tables_df[TABLE_NAME_COL].str.lower() == table.lower(),
                    TABLE_DESCRIPTION_STR,
                ].values[0]
            description_dict[table] = {
                TABLE_DESCRIPTION_STR: table_description.replace("\n", " "),
                COLUMNS_KEY: {},
            }
        else:
            description_dict[table] = {
                TABLE_DESCRIPTION_STR: DESCRIPTION_PLACEHOLDER,
                COLUMNS_KEY: {},
            }

        for column in schema_dict[table]:
            description_dict[table][COLUMNS_KEY][column] = longest_description_series.get(
                column, DESCRIPTION_PLACEHOLDER).replace("\n", " ")

    return description_dict