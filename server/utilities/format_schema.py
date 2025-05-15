import os
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Union

import pandas as pd
import yaml
from utilities.config import PATH_CONFIG
from utilities.constants.common.indexing_constants import (
    COLUMN_DESCRIPTION_COL, IMPROVED_COLUMN_DESCRIPTIONS_COL,
    ORIG_COLUMN_NAME_COL, TABLE_DESCRIPTION_COL, TABLE_NAME_COL)
from utilities.constants.database_enums import DatasetType
from utilities.constants.prompts_enums import FormatType
from utilities.constants.utilities.format_schema.indexing_constants import (
    COLUMN_DESCRIPTION_KEY, COLUMN_EXAMPLES_KEY, COLUMN_NAME_KEY,
    COLUMN_PRIMARY_KEY_KEY, COLUMN_TYPE_KEY, COLUMNS_KEY,
    FOREIGN_KEY_FROM_COLUMN_KEY, FOREIGN_KEY_TO_COLUMN_KEY,
    FOREIGN_KEY_TO_TABLE_KEY, TABLE_DESCRIPTION_KEY, TABLE_FOREIGN_KEYS_KEY)
from utilities.constants.utilities.format_schema.response_messages import (
    ERROR_UNSUPPORTED_FORMAT_TYPE, WARNING_DATABASE_DESCRIPTION_FILE_NOT_FOUND,
    WARNING_TABLE_DESCRIPTION_FILE_NOT_FOUND)
from utilities.constants.utilities.format_schema.schema_templates import (
    BASIC_SCHEMA_LINE_ENTRY, CODE_REPR_SCHEMA_MISSING_SQL_ENTRY,
    M_SCHEMA_COLUMN_ENTRY, M_SCHEMA_DB_LINE, M_SCHEMA_FOREIGN_KEY_ENTRY,
    M_SCHEMA_FOREIGN_KEY_LINE, M_SCHEMA_PRIMARY_KEY_FLAG, M_SCHEMA_SCHEMA_LINE,
    M_SCHEMA_TABLE_ENTRY, OPENAI_SCHEMA_LINE_ENTRY, SEMANTIC_COLUMN_ENTRY,
    SEMANTIC_SCHEMA_COLUMNS_KEY, SEMANTIC_SCHEMA_DESCRIPTION_KEY,
    SEMANTIC_SCHEMA_TABLE_KEY, TEXT_SCHEMA_LINE_ENTRY)
from utilities.logging_utils import setup_logger
from utilities.utility_functions import (examples_to_str, get_column_values,
                                         get_primary_keys, get_schema_dict,
                                         get_table_column_types, get_table_ddl,
                                         get_table_foreign_keys)

logger = setup_logger(__name__)

NUM_COLUMN_EXAMPLES = 5

TABLE_DESCRIPION_FILE = "{database_name}_tables.csv"
DESCRIPTION_FILE_EXTENSION = ".csv"
DESCRIPTION_PLACEHOLDER = ""


def get_table_foreign_key_list(
    table_name: str, columns: List[str], connection: sqlite3.Connection
) -> List[Dict[str, str]]:
    """
    Retrieves a list of foreign keys for a specified table that are associated with given columns.
    Returns a list of dictionaries as follows:
    [
        {
            "to_table": table_name,
            "to_column": column_name,
            "from_column": column_name
        },
        ...
    ]

    Args:
        table_name (str): The name of the table to retrieve foreign keys for.
        columns (List[str]): A list of column names to filter the foreign keys.
        connection (sqlite3.Connection): The database connection object.

    Returns:
        List[Dict]: A list of dictionaries representing the foreign keys that are associated with the specified columns.
    """

    table_foreign_keys = get_table_foreign_keys(
        connection=connection, table_name=table_name
    )

    foreign_keys_mentioned = [
        foreign_key
        for foreign_key in table_foreign_keys
        if foreign_key[FOREIGN_KEY_FROM_COLUMN_KEY] in columns
    ]

    return foreign_keys_mentioned


def create_column_schema_object(
    column_name: str,
    table_name: str,
    description_dict: Dict[str, Dict],
    table_column_types: Dict[str, str],
    primary_key_dict: Dict[str, List[str]],
    connection: sqlite3.Connection,
) -> Dict[str, Union[str, List, bool]]:
    """
    Creates a schema object for a given column in a table.
    Returns a dictionary as follows:

        {
            "column_name": column_name,
            "column_description": column_description
            "column_type": column_type,
            "column_examples": [ ... ],
            "primary_key": True|False,

        }

    Args:
        column_name (str): The name of the column.
        table_name (str): The name of the table.
        description_dict (Dict[str, Dict]): A dictionary containing descriptions for tables and columns.
        table_column_types (Dict[str, str]): A dictionary mapping column names to their data types.
        primary_key_dict (Dict[str, List[str]]): A dictionary mapping table names to their primary key columns.
        connection (sqlite3.Connection): A connection to the SQLite database.

    Returns:
        Dict[str, Union[str, List, bool]]: A dictionary representing the column schema object.
    """

    column_description = description_dict[table_name][COLUMNS_KEY][column_name]
    column_type = table_column_types[column_name]
    primary_key = True if column_name in primary_key_dict[table_name] else False
    column_examples = examples_to_str(get_column_values(
        column_name=column_name,
        table_name=table_name,
        num_values=NUM_COLUMN_EXAMPLES,
        connection=connection,
    )
    )

    return {
        COLUMN_NAME_KEY: column_name,
        COLUMN_DESCRIPTION_KEY: column_description,
        COLUMN_TYPE_KEY: column_type,
        COLUMN_EXAMPLES_KEY: column_examples,
        COLUMN_PRIMARY_KEY_KEY: primary_key,
    }


def create_schema_config_dict(schema_dict: Dict, description_dict, connection) -> Dict:
    """
    Generates a configuration dictionary for the schema based on the provided schema dictionary,
    description dictionary, and database connection.

    Returns a Dictionary as follows:

        {
        table_name:{
            "table_description": table_description,
            "columns":
                    [
                        {
                        "column_name": column_name,
                        "column_description": column_description
                        "column_type": column_type,
                        "column_examples": [],
                        "primary_key": True|False,

                        },
                        ...
                    ]
        "foreign_keys":
            [
                {
                    "to_table": table_name,
                    "to_column": column_name,
                    "from_column": column_name
                },
                ...
            ]

        }
        ...
        }

    Args:
        schema_dict (Dict): A dictionary representing the schema of the database.
        description_dict (Dict): A dictionary containing descriptions for tables and columns.
        connection: The database connection object.

    Returns:
        Dict: A dictionary containing the configuration for each table in the schema.
    """

    schema_config_dict = {}

    primary_keys = get_primary_keys(connection)

    for table_name in schema_dict:

        table_column_types = get_table_column_types(
            connection=connection, table_name=table_name
        )
        table_description = description_dict[table_name][TABLE_DESCRIPTION_KEY]

        table_foreign_key_list = get_table_foreign_key_list(
            connection=connection,
            table_name=table_name,
            columns=schema_dict[table_name],
        )

        column_objects = []
        for column_name in schema_dict[table_name]:

            column_objects.append(
                create_column_schema_object(
                    column_name=column_name,
                    table_name=table_name,
                    description_dict=description_dict,
                    table_column_types=table_column_types,
                    primary_key_dict=primary_keys,
                    connection=connection,
                )
            )

        schema_config_dict[table_name] = {
            TABLE_DESCRIPTION_KEY: table_description,
            COLUMNS_KEY: column_objects,
            TABLE_FOREIGN_KEYS_KEY: table_foreign_key_list,
        }

    return schema_config_dict


def load_table_description_file(
    description_dir: Path, table_name: str
) -> Union[None, pd.DataFrame]:
    """
    Loads a table description file from the specified directory.

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

    if os.path.exists(f"{description_dir}/{table_name}{DESCRIPTION_FILE_EXTENSION}"):
        table_description_df = pd.read_csv(
            f"{description_dir}/{table_name}{DESCRIPTION_FILE_EXTENSION}"
        )
    elif os.path.exists(
        f"{description_dir}/{table_name.lower()}{DESCRIPTION_FILE_EXTENSION}"
    ):
        table_description_df = pd.read_csv(
            f"{description_dir}/{table_name.lower()}{DESCRIPTION_FILE_EXTENSION}"
        )
    else:
        logger.warning(
            WARNING_TABLE_DESCRIPTION_FILE_NOT_FOUND.format(
                file_path=f"{description_dir}/{table_name}{DESCRIPTION_FILE_EXTENSION}"
            )
        )
        table_description_df = None

    return table_description_df


def get_longest_description_series(table_description_df: Union[pd.DataFrame, None]) -> pd.Series:
    """
    Returns a pandas Series containing the longest description for each column in the input DataFrame.

    Parameters:
    table_description_df (Union[pd.DataFrame, None]): A DataFrame containing column descriptions.
                                                     If None, an empty Series is returned.

    Returns:
    pd.Series: A Series where the index is the column names and the values are the longest descriptions.
    """

    if table_description_df is None:
        return pd.Series()

    # Isolate the description columns
    desc_df = table_description_df.dropna(
        subset=[COLUMN_DESCRIPTION_COL, IMPROVED_COLUMN_DESCRIPTIONS_COL],
        how="all",
    )

    # From the isolated the series choose the longest description
    longest_description_series = desc_df.apply(
        lambda row: max(
            (
                str(row[COLUMN_DESCRIPTION_COL] or ""),
                str(row[IMPROVED_COLUMN_DESCRIPTIONS_COL] or ""),
            ),
            key=len,
        ),
        axis=1,
    )

    # Set the index to the column names
    longest_description_series.index = desc_df[ORIG_COLUMN_NAME_COL].values

    return longest_description_series


def get_description_dict(
    database_name: str, dataset_type: DatasetType, schema_dict: Dict[str, List]
) -> Dict[str, Dict]:
    """
    Generates a dictionary containing descriptions for tables and their columns based on the provided schema.
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

    description_dir = PATH_CONFIG.description_dir(
        database_name=database_name, dataset_type=dataset_type
    )

    # Load table descriptions
    db_descriptions_path = (
        f"{description_dir}/{TABLE_DESCRIPION_FILE.format(database_name=database_name)}"
    )
    try:
        tables_df = pd.read_csv(db_descriptions_path)
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
            description_dir=description_dir, table_name=table
        )
        longest_description_series = get_longest_description_series(
            table_description_df=table_description_df)

        # if table description file is not read, replace table description with placeholder
        if tables_df is not None:
            description_dict[table] = {
                TABLE_DESCRIPTION_KEY: tables_df.loc[
                    tables_df[TABLE_NAME_COL].str.lower() == table.lower(),
                    TABLE_DESCRIPTION_COL,
                ].values[0].replace("\n", " "),
                COLUMNS_KEY: {},
            }
        else:
            description_dict[table] = {
                TABLE_DESCRIPTION_KEY: DESCRIPTION_PLACEHOLDER,
                COLUMNS_KEY: {},
            }

        for column in schema_dict[table]:
            description_dict[table][COLUMNS_KEY][column] = longest_description_series.get(
                column, DESCRIPTION_PLACEHOLDER).replace("\n", " ")

    return description_dict


def remove_errors_from_linked_schema(linked_schema: Dict[str, List], schema_dict: Dict[str, List]) -> Dict[str, List]:
    """
    Removes columns and tables in the linked schema that do not exist in the DB.
    The comparison is case-insensitive.

    Parameters:
    linked_schema (Dict[str, List]): A dictionary where keys are table names and
                                    values are lists of column names.
    schema_dict (Dict[str, List]): A dictionary where keys are table names and
                                   values are lists of column names.

    Returns:
    Dict[str, List]: A corrected linked schema where only valid tables and columns
                     are retained. The keys and values are in the same case as
                     the original linked schema.
    """

    lowered_schema_dict = {
        table.lower(): [column.lower() for column in schema_dict[table]]
        for table in schema_dict.keys()
    }

    corrected_linked_schema = {}
    for table in linked_schema.keys():
        if table.lower() in lowered_schema_dict:
            corrected_linked_schema[table] = []
            for column in linked_schema[table]:
                if column.lower() in lowered_schema_dict[table.lower()]:
                    corrected_linked_schema[table].append(column)

    return corrected_linked_schema


def map_linked_schema_to_original_schema(
    linked_schema: Dict[str, List[str]], schema_dict: Dict[str, List[str]]
) -> Dict[str, List[str]]:
    """
    Maps a linked schema to the original schema using case-insensitive comparison.

    This function takes a linked schema and an original schema dictionary, and maps
    the linked schema to the original schema using case-insensitive comparison for
    both table names and column names. This ensures that the linked schema can be
    correctly aligned with the original schema, even if there are differences in
    casing.

    Args:
        linked_schema (Dict[str, List[str]]): A dictionary representing the linked schema,
            where keys are table names and values are lists of column names.
        schema_dict (Dict[str, List[str]]): A dictionary representing the original schema,
            where keys are table names and values are lists of column names.

    Returns:
        Dict[str, List[str]]: A dictionary representing the mapped schema, where keys are
            the original table names and values are lists of the original column names,
            mapped from the linked schema.
    """

    # Create mappings for case-insensitive comparison for all tables and columns
    table_lower_case_mapping = {
        table.lower(): table for table in schema_dict.keys()}
    column_lower_case_mapping = {
        table.lower(): {column.lower(): column for column in schema_dict[table]}
        for table in schema_dict.keys()
    }

    # Normalize the linked schema to lower case for comparison
    linked_schema = {
        table.lower(): [column.lower() for column in linked_schema[table]]
        for table in linked_schema
    }

    # Map the linked schema to the original schema using the mappings
    mapped_schema_dict = {
        table_lower_case_mapping[table.lower()]: [
            column_lower_case_mapping[table.lower()][column.lower()]
            for column in linked_schema[table.lower()]
        ]
        for table in linked_schema.keys()
    }

    return mapped_schema_dict


def basic_schema(schema_config_dict: Dict) -> str:
    """
    Generates a basic schema representation from the given schema configuration dictionary.

    Args:
        schema_config_dict (Dict): A dictionary containing schema configuration details.

    Returns:
        str: A string representing the basic schema.
    """

    schema = []
    for table in schema_config_dict:
        columns = [
            column[COLUMN_NAME_KEY] for column in schema_config_dict[table][COLUMNS_KEY]
        ]
        schema.append(BASIC_SCHEMA_LINE_ENTRY.format(
            table=table, columns=', '.join(columns)))
    return "\n".join(schema)


def text_schema(schema_config_dict: Dict) -> str:
    """
    Generates a text schema representation from the given schema configuration dictionary.

    Args:
        schema_config_dict (Dict): A dictionary containing schema configuration details.

    Returns:
        str: A string representing the text schema.
    """

    schema = []
    for table in schema_config_dict:
        columns = [
            column[COLUMN_NAME_KEY] for column in schema_config_dict[table][COLUMNS_KEY]
        ]
        schema.append(TEXT_SCHEMA_LINE_ENTRY.format(
            table=table, columns=', '.join(columns)))
    return "\n".join(schema)


def code_repr_schema(schema_config_dict: Dict, connection: sqlite3.Connection) -> str:
    """
    Generates a code representation schema from the given schema configuration dictionary and database connection.

    Args:
        schema_config_dict (Dict): A dictionary containing schema configuration details.
        connection (sqlite3.Connection): A connection to the SQLite database.

    Returns:
        str: A string representing the code representation schema.
    """

    schema = []
    for table in schema_config_dict:
        table_ddl = get_table_ddl(connection=connection, table_name=table)
        schema.append(
            table_ddl if table_ddl else CODE_REPR_SCHEMA_MISSING_SQL_ENTRY.format(table=table))
    return "\n".join(schema)


def openai_schema(schema_config_dict: Dict) -> str:
    """
    Generates an OpenAI schema representation from the given schema configuration dictionary.

    Args:
        schema_config_dict (Dict): A dictionary containing schema configuration details.

    Returns:
        str: A string representing the OpenAI schema.
    """

    schema = []
    for table in schema_config_dict:
        columns = [
            column[COLUMN_NAME_KEY] for column in schema_config_dict[table][COLUMNS_KEY]
        ]
        schema.append(OPENAI_SCHEMA_LINE_ENTRY.format(
            table=table, columns=', '.join(columns)))
    return "\n".join(schema)


def semantic_schema(schema_config_dict: Dict) -> str:
    """
    Generates a semantic schema representation from the given schema configuration dictionary.

    Args:
        schema_config_dict (Dict): A dictionary containing schema configuration details.

    Returns:
        str: A string representing the semantic schema in YAML format.
    """

    schema = []
    for table in schema_config_dict:
        table_entry = {
            SEMANTIC_SCHEMA_TABLE_KEY: table,
            SEMANTIC_SCHEMA_DESCRIPTION_KEY: schema_config_dict[table][TABLE_DESCRIPTION_KEY],
        }
        columns = [
            SEMANTIC_COLUMN_ENTRY.format(
                column_name=column[COLUMN_NAME_KEY],
                column_type=column[COLUMN_TYPE_KEY],
                column_description=column[COLUMN_DESCRIPTION_KEY],
                example_value=column[COLUMN_EXAMPLES_KEY][0],
            )
            for column in schema_config_dict[table][COLUMNS_KEY]
        ]
        table_entry[SEMANTIC_SCHEMA_COLUMNS_KEY] = columns
        schema.append(table_entry)

    return yaml.dump(schema, sort_keys=False, default_flow_style=False)


def m_schema(schema_config_dict: Dict, database_name: str) -> str:
    """
    Generates an M schema representation from the given schema configuration dictionary and database name.

    Args:
        schema_config_dict (Dict): A dictionary containing schema configuration details.
        database_name (str): The name of the database.

    Returns:
        str: A string representing the M schema.
    """

    schema = [
        M_SCHEMA_DB_LINE.format(database_name=database_name),
        M_SCHEMA_SCHEMA_LINE,
    ]

    foreign_keys = []
    for table in schema_config_dict:
        columns = [
            M_SCHEMA_COLUMN_ENTRY.format(
                column_name=column[COLUMN_NAME_KEY],
                column_type=column[COLUMN_TYPE_KEY],
                column_description=column[COLUMN_DESCRIPTION_KEY],
                examples_list=str(column[COLUMN_EXAMPLES_KEY]),
                primary_key=(
                    M_SCHEMA_PRIMARY_KEY_FLAG if column[COLUMN_PRIMARY_KEY_KEY] else ""
                ),
            )
            for column in schema_config_dict[table][COLUMNS_KEY]
        ]

        table_entry = M_SCHEMA_TABLE_ENTRY.format(
            table_name=table,
            table_description=schema_config_dict[table][TABLE_DESCRIPTION_KEY],
            columns="\n".join(columns),
        )
        schema.append(table_entry)

        for foreign_key in schema_config_dict[table][TABLE_FOREIGN_KEYS_KEY]:
            foreign_keys.append(
                M_SCHEMA_FOREIGN_KEY_ENTRY.format(
                    from_table=table,
                    from_column=foreign_key[FOREIGN_KEY_FROM_COLUMN_KEY],
                    to_table=foreign_key[FOREIGN_KEY_TO_TABLE_KEY],
                    to_column=foreign_key[FOREIGN_KEY_TO_COLUMN_KEY],
                )
            )

    if len(foreign_keys) > 0:
        schema.append(M_SCHEMA_FOREIGN_KEY_LINE)
        schema.extend(foreign_keys)

    return "\n".join(schema)


def format_schema(
    format_type: FormatType,
    database_name: str,
    linked_schema: Optional[Dict[str, List]] = None,
    dataset_type: Optional[DatasetType] = None,
) -> str:
    """
    Formats the schema of a given database according to the specified format type.

    Args:
        format_type (FormatType): The type of format to apply to the schema.
        database_name (str): The name of the database.
        linked_schema (Optional[Dict[str, List]]): An optional linked schema to use instead of the schema from the database.
        dataset_type (Optional[DatasetType]): The type of dataset. If not provided, the default dataset type is used.

    Returns:
        str: The formatted schema as a string.

    Raises:
        ValueError: If the provided format type is unsupported.
    """

    db_path = PATH_CONFIG.sqlite_path(
        database_name=database_name,
        dataset_type=dataset_type if dataset_type else PATH_CONFIG.dataset_type,
    )

    schema_dict = get_schema_dict(database_path=db_path)

    # if linked schema is provided, use it instead of the schema from the database, sometimes linked schema is in lower case hence map it to original column names
    if linked_schema:
        corrected_linked_schema = remove_errors_from_linked_schema(
            linked_schema=linked_schema, schema_dict=schema_dict)
        schema_dict = map_linked_schema_to_original_schema(
            linked_schema=corrected_linked_schema, schema_dict=schema_dict
        )

    description_dict = get_description_dict(
        database_name=database_name, dataset_type=dataset_type, schema_dict=schema_dict
    )

    with sqlite3.connect(
        PATH_CONFIG.sqlite_path(
            database_name=database_name, dataset_type=dataset_type)
    ) as conn:

        schema_config_dict = create_schema_config_dict(
            schema_dict=schema_dict, description_dict=description_dict, connection=conn
        )

        if format_type == FormatType.BASIC:
            return basic_schema(schema_config_dict=schema_config_dict)
        elif format_type == FormatType.TEXT:
            return text_schema(schema_config_dict=schema_config_dict)
        elif format_type == FormatType.CODE:
            return code_repr_schema(schema_config_dict=schema_config_dict, connection=conn)
        elif format_type == FormatType.OPENAI:
            return openai_schema(schema_config_dict=schema_config_dict)
        elif format_type == FormatType.SEMANTIC:
            return semantic_schema(schema_config_dict=schema_config_dict)
        elif format_type == FormatType.M_SCHEMA:
            return m_schema(
                schema_config_dict=schema_config_dict, database_name=database_name
            )
        else:
            raise ValueError(
                (ERROR_UNSUPPORTED_FORMAT_TYPE.format(format_type=format_type))
            )
