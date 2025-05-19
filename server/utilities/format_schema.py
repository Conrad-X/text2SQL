import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Union

import pandas as pd
import yaml
from utilities.bird_utils import generate_description_dict
from utilities.config import PATH_CONFIG
from utilities.constants.common.indexing_constants import (
    COLUMNS_KEY, TABLE_DESCRIPTION_STR)
from utilities.constants.database_enums import DatasetType
from utilities.constants.prompts_enums import FormatType
from utilities.constants.utilities.format_schema.indexing_constants import (
    COLUMN_DESCRIPTION_KEY, COLUMN_EXAMPLES_KEY, COLUMN_NAME_KEY,
    COLUMN_PRIMARY_KEY, COLUMN_TYPE_KEY, FOREIGN_KEY_FROM_COLUMN_KEY,
    FOREIGN_KEY_TO_COLUMN_KEY, FOREIGN_KEY_TO_TABLE_KEY, TABLE_FOREIGN_KEY)
from utilities.constants.utilities.format_schema.response_messages import \
    ERROR_UNSUPPORTED_FORMAT_TYPE
from utilities.constants.utilities.format_schema.schema_templates import (
    BASIC_SCHEMA_LINE_ENTRY, CODE_REPR_SCHEMA_MISSING_SQL_ENTRY,
    M_SCHEMA_COLUMN_ENTRY, M_SCHEMA_DB_LINE, M_SCHEMA_FOREIGN_KEY_ENTRY,
    M_SCHEMA_FOREIGN_KEY_LINE, M_SCHEMA_PRIMARY_KEY_FLAG, M_SCHEMA_SCHEMA_LINE,
    M_SCHEMA_TABLE_ENTRY, OPENAI_SCHEMA_LINE_ENTRY, SEMANTIC_COLUMN_ENTRY,
    SEMANTIC_SCHEMA_COLUMNS_KEY, SEMANTIC_SCHEMA_DESCRIPTION_KEY,
    SEMANTIC_SCHEMA_TABLE_KEY, TEXT_SCHEMA_LINE_ENTRY)
from utilities.logging_utils import setup_logger
from utilities.utility_functions import (format_example_values,
                                         get_column_values, get_primary_keys,
                                         get_schema_dict,
                                         get_table_column_types, get_table_ddl,
                                         get_table_foreign_keys)

logger = setup_logger(__name__)

NUM_COLUMN_EXAMPLES = 5
DESCRIPTION_FILE_EXTENSION = ".csv"
DESCRIPTION_PLACEHOLDER = ""
UNKNOWN_COLUMN_TYPE = "UNKNOWN"


def filter_table_foreign_keys_by_columns(
    table_name: str, columns: List[str], connection: sqlite3.Connection
) -> List[Dict[str, str]]:
    """
    Retrieve a list of foreign keys for a specified table that are associated with given columns.

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
    all_foreign_keys = get_table_foreign_keys(
        connection=connection, table_name=table_name
    )

    filtered_foreign_keys = []

    if len(all_foreign_keys) > 0:
        filtered_foreign_keys = [
            foreign_key
            for foreign_key in all_foreign_keys
            if foreign_key[FOREIGN_KEY_FROM_COLUMN_KEY] in columns
        ]

    return filtered_foreign_keys


def create_column_schema_object(
    column_name: str,
    table_name: str,
    description_dict: Dict[str, Dict],
    table_column_types: Dict[str, str],
    primary_key_dict: Dict[str, List[str]],
    connection: sqlite3.Connection,
) -> Dict[str, Union[str, List, bool]]:
    """
    Create a schema object for a given column in a table.

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
    column_type = table_column_types.get(column_name, UNKNOWN_COLUMN_TYPE)
    primary_key = column_name in primary_key_dict[table_name]
    column_values = get_column_values(
        column_name=column_name,
        table_name=table_name,
        num_values=NUM_COLUMN_EXAMPLES,
        connection=connection,
    )
    column_examples = format_example_values(column_values)

    return {
        COLUMN_NAME_KEY: column_name,
        COLUMN_DESCRIPTION_KEY: column_description,
        COLUMN_TYPE_KEY: column_type,
        COLUMN_EXAMPLES_KEY: column_examples,
        COLUMN_PRIMARY_KEY: primary_key,
    }


def construct_schema_config(schema_dict: Dict, description_dict: Dict[str, Dict], connection: sqlite3.Connection) -> Dict[str, Dict[str, any]]:
    """
    Generate a configuration dictionary for the schema based on the provided schema dictionary, description dictionary, and database connection.

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

    for table_name, column_names in schema_dict.items():

        table_column_types = get_table_column_types(
            connection=connection, table_name=table_name
        )
        table_description = description_dict[table_name][TABLE_DESCRIPTION_STR]

        table_foreign_key_list = filter_table_foreign_keys_by_columns(
            connection=connection,
            table_name=table_name,
            columns=schema_dict[table_name],
        )

        column_objects = [
            create_column_schema_object(
                column_name=column_name,
                table_name=table_name,
                description_dict=description_dict,
                table_column_types=table_column_types,
                primary_key_dict=primary_keys,
                connection=connection,
            )
            for column_name in column_names
        ]

        schema_config_dict[table_name] = {
            TABLE_DESCRIPTION_STR: table_description,
            COLUMNS_KEY: column_objects,
            TABLE_FOREIGN_KEY: table_foreign_key_list,
        }

    return schema_config_dict


def remove_errors_from_linked_schema(linked_schema: Dict[str, List], schema_dict: Dict[str, List]) -> Dict[str, List]:
    """
    Remove columns and tables in the linked schema that do not exist in the DB.

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
    Map a linked schema to the original schema using case-insensitive comparison.

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
    Generate a basic schema representation from the given schema configuration dictionary.

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
    Generate a text schema representation from the given schema configuration dictionary.

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
    Generate a code representation schema from the given schema configuration dictionary and database connection.

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
    Generate an OpenAI schema representation from the given schema configuration dictionary.

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
    Generate a semantic schema representation from the given schema configuration dictionary.

    Args:
        schema_config_dict (Dict): A dictionary containing schema configuration details.

    Returns:
        str: A string representing the semantic schema in YAML format.
    """
    schema = []
    for table in schema_config_dict:
        table_entry = {
            SEMANTIC_SCHEMA_TABLE_KEY: table,
            SEMANTIC_SCHEMA_DESCRIPTION_KEY: schema_config_dict[table][TABLE_DESCRIPTION_STR],
        }
        columns = [
            SEMANTIC_COLUMN_ENTRY.format(
                column_name=column[COLUMN_NAME_KEY],
                column_type=column[COLUMN_TYPE_KEY],
                column_description=column[COLUMN_DESCRIPTION_KEY],
                example_value=column[COLUMN_EXAMPLES_KEY][0] if len(
                    column[COLUMN_EXAMPLES_KEY]) > 0 else "",
            )
            for column in schema_config_dict[table][COLUMNS_KEY]
        ]
        table_entry[SEMANTIC_SCHEMA_COLUMNS_KEY] = columns
        schema.append(table_entry)

    return yaml.dump(schema, sort_keys=False, default_flow_style=False)


def m_schema(schema_config_dict: Dict, database_name: str) -> str:
    """
    Generate an M schema representation from the given schema configuration dictionary and database name.

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
                    M_SCHEMA_PRIMARY_KEY_FLAG if column[COLUMN_PRIMARY_KEY] else ""
                ),
            )
            for column in schema_config_dict[table][COLUMNS_KEY]
        ]

        table_entry = M_SCHEMA_TABLE_ENTRY.format(
            table_name=table,
            table_description=schema_config_dict[table][TABLE_DESCRIPTION_STR],
            columns="\n".join(columns),
        )
        schema.append(table_entry)

        for foreign_key in schema_config_dict[table][TABLE_FOREIGN_KEY]:
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
    Format the schema of a given database according to the specified format type.

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

    description_dict = generate_description_dict(
        database_name=database_name, dataset_type=dataset_type, schema_dict=schema_dict
    )

    with sqlite3.connect(
        PATH_CONFIG.sqlite_path(
            database_name=database_name, dataset_type=dataset_type)
    ) as conn:

        schema_config_dict = construct_schema_config(
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
