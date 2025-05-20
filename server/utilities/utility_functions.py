import concurrent.futures
import datetime
import decimal
import re
import sqlite3
from enum import Enum
from typing import Dict, List, Union

from nltk import pos_tag, word_tokenize
from nltk.corpus import wordnet
from utilities.config import PATH_CONFIG
from utilities.constants.LLM_enums import VALID_LLM_MODELS, LLMType, ModelType
from utilities.constants.response_messages import (
    ERROR_DATABASE_QUERY_FAILURE, ERROR_FAILED_FECTHING_PRIMARY_KEYS,
    ERROR_FAILED_FETCH_COLUMN_NAMES, ERROR_FAILED_FETCH_COLUMN_TYPES,
    ERROR_FAILED_FETCH_COLUMN_VALUES, ERROR_FAILED_FETCH_FOREIGN_KEYS,
    ERROR_FAILED_FETCH_SCHEMA, ERROR_FAILED_FETCH_TABLE_NAMES,
    ERROR_INVALID_MODEL_FOR_TYPE, ERROR_SQL_MASKING_FAILED,
    ERROR_SQL_QUERY_REQUIRED, ERROR_SQLITE_EXECUTION_ERROR,
    ERROR_UNSUPPORTED_CLIENT_TYPE)
from utilities.m_schema.schema_engine import SchemaEngine

SQL_GET_TABLE_DDL = (
    'SELECT sql FROM sqlite_master WHERE type="table" AND name="{table_name}"'
)
PRAGMA_COLUMN_NAME_INDEX = 1
PRAGMA_PRIMARY_KEY_INDEX = 5
PRAGMA_COLUMN_TYPE_INDEX = 2

PRAGMA_TABLE_INFO_SQL = 'PRAGMA table_info("{table_name}")'
GET_TABLE_COLUMN_VALUES_SQL = 'SELECT "{column_name}" FROM "{table_name}" LIMIT {num_values}'


def execute_sql_query(connection: sqlite3.Connection, sql_query: str):
    """
    Executes a SQL query and returns the results as a list of dictionaries.
    """
    if not sql_query:
        raise ValueError(ERROR_SQL_QUERY_REQUIRED)

    try:
        cursor = connection.cursor()
        cursor.execute(sql_query)
        columns = [
            description[0].split(
            )[-1] if " " in description[0] else description[0]
            for description in cursor.description
        ]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return rows
    except Exception as e:
        raise RuntimeError(ERROR_DATABASE_QUERY_FAILURE.format(error=str(e)))
    finally:
        cursor.close()


def execute_sql_timeout(database, sql_query: str, timeout=30):
    """
    Executes a SQL query and returns the results as a list of dictionaries.
    Times out if the query takes longer than the specified timeout.
    """
    if not sql_query:
        raise ValueError(ERROR_SQL_QUERY_REQUIRED)

    def run_query():
        try:
            connection = sqlite3.connect(
                PATH_CONFIG.sqlite_path(database_name=database), timeout=timeout
            )  # Set SQLite connection timeout
            cursor = connection.cursor()
            cursor.execute(sql_query)
            res = cursor.fetchall()
            return res
        except Exception as e:
            raise RuntimeError(
                ERROR_DATABASE_QUERY_FAILURE.format(error=str(e)))
        finally:
            cursor.close()
            connection.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(run_query)
        try:
            return future.result(timeout=timeout)
        except concurrent.futures.TimeoutError:
            raise TimeoutError(f"Query execution exceeded {timeout} seconds")


def validate_llm_and_model(llm_type: LLMType, model: ModelType):
    """
    Validates that the model corresponds to the LLM type.
    """
    if llm_type not in VALID_LLM_MODELS:
        raise ValueError(ERROR_UNSUPPORTED_CLIENT_TYPE)

    if model not in VALID_LLM_MODELS[llm_type]:
        raise ValueError(
            ERROR_INVALID_MODEL_FOR_TYPE.format(
                model=model.value, llm_type=llm_type.value
            )
        )


def get_table_names(connection: sqlite3.Connection):
    """
    Retrieves the names of all tables in the SQLite database.
    """
    query = "SELECT name FROM sqlite_master WHERE type='table';"
    try:
        result = execute_sql_query(connection, query)
        return [row["name"] for row in result]
    except Exception as e:
        raise RuntimeError(
            (ERROR_FAILED_FETCH_TABLE_NAMES.format(error=str(e))))


def get_table_columns(connection: sqlite3.Connection, table_name: str):
    """
    Fetches the column names for a given table in the SQLite database.
    """
    try:
        query = f"PRAGMA table_info('{table_name}')"
        cursor = connection.cursor()
        cursor.execute(query)
        return [row[1] for row in cursor.fetchall()]
    except Exception as e:
        raise RuntimeError(
            (ERROR_FAILED_FETCH_COLUMN_NAMES.format(error=str(e))))


def get_array_of_table_and_column_name(database_path: str):
    try:
        connection = sqlite3.connect(database_path)
        connection.row_factory = sqlite3.Row

        table_names = get_table_names(connection)
        column_names = []
        for table_name in table_names:
            column_names.extend(get_table_columns(connection, table_name))

        return table_names + column_names
    finally:
        connection.close()


def prune_code(ddl, columns, connection, table):
    """
    Filters the given DDL statement to retain only the specified columns and
    remove any that are not present in the database table.
    """
    try:
        ddl = ddl.split("(\n")
        create_table = ddl[0]
        ddl = ddl[1].split(",\n")

        cols_from_conn = [i.lower()
                          for i in get_table_columns(connection, table)]
        stripped_cols = [i.lstrip("\n ").split(" ")[0].lower() for i in ddl]
        pruned_ddl = []
        for idx, i in enumerate(stripped_cols):
            if i not in cols_from_conn:
                pruned_ddl.append(ddl[idx])
            elif i in columns:
                pruned_ddl.append(ddl[idx])

        pruned_ddl = ",\n".join(pruned_ddl)
        pruned_ddl = f"{create_table}(\n{pruned_ddl}"
        if pruned_ddl[-2:] != "\n)":
            pruned_ddl += "\n)"

        return pruned_ddl
    except Exception as e:
        print("Exception in prune code: ", e)
        return ddl


def convert_word_to_singular_form(word) -> str:
    singular_word = wordnet.morphy(word)
    return singular_word if singular_word else word


def mask_question(
    question: str,
    table_and_column_names: list,
    mask_tag: str = "<mask>",
    value_tag: str = "<unk>",
) -> str:
    """
    Masks specified table and column names with mask tag and other values with value tag in a question.
    """
    tokens = word_tokenize(question)
    pos_tags = pos_tag(tokens)

    table_and_column_names = set(
        convert_word_to_singular_form(name.lower()) for name in table_and_column_names
    )

    masked_question = []

    for word, tag in pos_tags:
        word_lower = word.lower()

        if tag in ["NNS", "NNPS"]:
            word_lower = convert_word_to_singular_form(word_lower)

        is_table_or_column = any(
            word_lower in tab for tab in table_and_column_names)
        is_numeric_value = word.isdigit() or word.replace(".", "", 1).isdigit()
        is_noun_or_adjective = tag in ["NN", "NNS", "NNP", "NNPS", "JJ"]
        is_stop_word = tag in ["DT", "CC", "IN", "WP", "PRP$"]

        if is_table_or_column and not is_stop_word:
            masked_question.append(mask_tag)
        elif is_noun_or_adjective and not is_table_or_column or is_numeric_value:
            masked_question.append(value_tag)
        else:
            masked_question.append(word)

    return " ".join(masked_question)


def mask_sql_query(
    sql_query: str, mask_tag: str = "<mask>", value_tag: str = "<unk>"
) -> str:
    """
    Masks table names, column names, and values/integers in a SQL query.
    """
    sql_keywords = (
        r"\b(select|from|where|and|or|insert|update|delete|set|values|"
        r"join|on|group by|order by|having|limit|distinct|as|inner|"
        r"left|right|full|cross|natural|outer|with|concat|"
        r"sum|avg|count|min|max|group_concat|like)\b"
    )

    table_column_pattern = rf"\b(?!{sql_keywords})\w+\b"
    value_pattern = r"\'[^\']*\'|\d+(\.\d+)?"
    combined_pattern = rf"({table_column_pattern})|({value_pattern})"

    def apply_mask(matched_text):
        matched_string = matched_text.group(0)
        return mask_tag if re.match(table_column_pattern, matched_string) else value_tag

    try:
        masked_query = re.sub(
            combined_pattern, apply_mask, sql_query, flags=re.IGNORECASE
        )
        return masked_query
    except Exception as e:
        raise ValueError(ERROR_SQL_MASKING_FAILED.format(error=e))


def format_sql_response(sql: str) -> str:
    """
    Cleans up and formats the raw SQL response returned by the LLM.
    """
    sql = re.sub(r"```\s*$", "", sql)
    sql = re.sub(r"^```sqlite\s*", "", sql, flags=re.IGNORECASE)
    sql = re.sub(r"^```sql\s*", "", sql, flags=re.IGNORECASE)
    sql = sql.replace("\n", " ").replace("\\n", " ")
    sql = sql.rstrip().lstrip()
    sql = sql.strip("`")
    if sql.startswith("SELECT"):
        return sql[:5000]
    sql = "SELECT " + sql
    return sql[:5000]


def convert_enums_to_string(enum_object):
    """
    This function takes in a object and converts Enums to their string values. The function recursively calls itself for every value of a dict or a list until it reaches an Enum, if it does not reach an enum then return as it is.
    """

    if isinstance(enum_object, dict):
        return {
            key: convert_enums_to_string(value) for key, value in enum_object.items()
        }
    elif isinstance(enum_object, list):
        return [convert_enums_to_string(item) for item in enum_object]
    elif isinstance(enum_object, Enum):
        return enum_object.value
    else:
        return enum_object


def format_chat(chat, translate_dict):
    """
    Formats a chat conversation into a structured format using a translation dictionary.
    """
    formatted_chat = []
    for i in chat:
        if len(i[1]) >= 1:
            formatted_chat.append(
                {"role": translate_dict[i[0]], translate_dict["content"]: i[1]}
            )

    return formatted_chat


def normalize_execution_results(
    results, result_len=50000, value_len=10000, fetchall=False
):
    if fetchall:
        if len(str(results)) > result_len:
            return_list = []
            for row in results:
                value_len_row = int(value_len / len(row))
                return_list.append([str(i)[:value_len_row] for i in row])
            results = return_list
    if (not fetchall) and len(str(results)) > result_len:
        for row in results:
            for key, value in row.items():
                row[key] = str(value)[:value_len]
    return results


def check_config_types(
    input_config: dict, gold_config: dict, path: Union[str, None] = ""
) -> list:
    """
    Recursively checks whether the structure and types of an input configuration dictionary
    match the expected structure and types defined in a gold configuration dictionary.

    Special Cases:
        - For the key "improve_config", the value is allowed to be either a dict or None.
        - If a value is a list, its length and element types must match the structure of the gold_config list.
    """

    errors = []
    for key, expected_type in gold_config.items():
        full_key = f"{path}.{key}" if path else key

        if key not in input_config:
            errors.append(f"Missing key: {full_key}")
            continue

        value = input_config[key]

        if isinstance(expected_type, dict):
            if not isinstance(value, dict):
                # if checking improve_config it can be None as well
                if key == "improve_config" and value is not None:
                    errors.append(
                        f"Key '{full_key}' should be a dict or None.")
                elif key != "improve_config":
                    errors.append(f"Key '{full_key}' should be a dict")
            else:
                errors += check_config_types(value,
                                             expected_type, path=full_key)

        elif isinstance(expected_type, list):
            if len(expected_type) != len(input_config[key]):
                errors.append(
                    f"Key '{full_key}' should be a list of length {len(expected_type)}"
                    f", got length {len(input_config[key])}"
                )
            else:
                for i, (sub_value, sub_type) in enumerate(zip(value, expected_type)):
                    if not isinstance(sub_value, sub_type):
                        errors.append(
                            f"Key '{full_key}[{i}]' should be of type {sub_type.__name__}"
                        )
        else:
            if not isinstance(value, expected_type):
                errors.append(
                    f"Key '{full_key}' should be of type {expected_type.__name__}"
                )

    return errors


def get_table_ddl(connection: sqlite3.Connection, table_name: str) -> any:
    """
    Retrieves the Data Definition Language (DDL) statement for a specified SQLite table.

    Executes a SQL query to fetch the CREATE TABLE statement from the SQLite internal schema.
    This is useful for inspecting the structure of the table as originally defined.

    Args:
        connection (sqlite3.Connection): An active SQLite database connection.
        table_name (str): The name of the table whose DDL is to be retrieved.

    Returns:
        any: The DDL statement as a string, or None if not found.
    """

    cursor = connection.cursor()

    try:
        cursor.execute(SQL_GET_TABLE_DDL.format(table_name=table_name))
        results = cursor.fetchone()[0]
    except sqlite3.Error as e:
        raise RuntimeError(ERROR_SQLITE_EXECUTION_ERROR.format(
            sql=SQL_GET_TABLE_DDL.format(table_name=table_name, error=str(e))))
    finally:
        cursor.close()

    return results


def get_table_foreign_keys(connection: sqlite3.Connection, table_name: str):
    """
    Retrieves foreign key information for a given table.
    """
    try:
        query = f'PRAGMA foreign_key_list("{table_name}");'
        cursor = connection.execute(query)
        foreign_keys = cursor.fetchall()

        return [
            {
                "from_column": row[3],  # column in current table
                "to_column": row[4],  # referenced column in foreign table
                "to_table": row[2],  # referenced table
            }
            for row in foreign_keys
        ]

    except Exception as e:
        raise RuntimeError(
            ERROR_FAILED_FETCH_FOREIGN_KEYS.format(
                table_name=table_name, error=str(e))
        )


def get_primary_keys(connection: sqlite3.Connection) -> Dict[str, List[str]]:
    """
    Returns a dictionary mapping each table name to its list of primary key columns.
    """
    try:
        cursor = connection.cursor()
        primary_key_dict = {}
        # Get all table names
        tables = get_table_names(connection)
        if "sqlite_sequence" in tables:
            tables.remove("sqlite_sequence")

        for table_name in tables:
            cursor.execute(f'PRAGMA table_info("{table_name}");')
            columns = cursor.fetchall()

            # PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
            primary_key_columns = [
                col[PRAGMA_COLUMN_NAME_INDEX]
                for col in columns
                if col[PRAGMA_PRIMARY_KEY_INDEX] > 0
            ]  # col[1] is column name, col[5] is pk flag
            primary_key_dict[table_name] = primary_key_columns

        return primary_key_dict
    except Exception as e:
        raise RuntimeError(
            ERROR_FAILED_FECTHING_PRIMARY_KEYS.format(error=str(e)))


def get_schema_dict(database_path: str) -> Dict[str, List[str]]:
    """
    Retrieves schema dictionary from the SQLite database in the format {table_name: [column1, column2, ...]}.
    """

    try:
        with sqlite3.connect(database_path) as connection:
            connection.row_factory = sqlite3.Row
            table_names = get_table_names(connection)
            schema = {
                table_name: get_table_columns(connection, table_name)
                for table_name in table_names
            }
            if "sqlite_sequence" in schema:
                del schema["sqlite_sequence"]

            return schema

    except Exception as e:

        raise RuntimeError(ERROR_FAILED_FETCH_SCHEMA.format(error=str(e)))


def get_table_column_types(table_name: str, connection: sqlite3.Connection) -> Dict[str, str]:
    """
    Get the column types for a given table in a SQLite database.
    """
    try:
        cursor = connection.cursor()
        cursor.execute(PRAGMA_TABLE_INFO_SQL.format(table_name=table_name))
        columns = cursor.fetchall()
        column_types = {column[PRAGMA_COLUMN_NAME_INDEX]                        : column[PRAGMA_COLUMN_TYPE_INDEX] for column in columns}
        return column_types
    except Exception as e:
        raise RuntimeError(ERROR_FAILED_FETCH_COLUMN_TYPES.format(
            table_name=table_name, error=str(e)))


def get_column_values(column_name: str, table_name: str, num_values: int, connection: sqlite3.Connection) -> List:
    """
    Get the values for a given column in a SQLite database.
    """
    try:
        cursor = connection.cursor()
        cursor.execute(
            GET_TABLE_COLUMN_VALUES_SQL.format(column_name=column_name, table_name=table_name, num_values=num_values))
        values = cursor.fetchall()
        return [value[0] for value in values]
    except Exception as e:
        raise RuntimeError(ERROR_FAILED_FETCH_COLUMN_VALUES.format(
            column_name=column_name, table_name=table_name, error=str(e)))


def is_email(string: str) -> bool:
    """
    Check if the given string is a valid email address.

    Args:
        string (str): The string to be checked.

    Returns:
        bool: True if the string is a valid email address, False otherwise.
    """
    pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
    match = re.match(pattern, string)
    if match:
        return True
    else:
        return False


def format_example_values(examples: list[any]) -> list[str]:
    """
    Converts a list of example values into strings, filtering out dates, URLs, emails, and other non-stringifiable values.

    This function processes a list of example values and returns a list of strings. It filters out values that are
    instances of `datetime.date` or `datetime.datetime`, converts `decimal.Decimal` instances to floats, and excludes
    values that are emails, URLs, or otherwise non-stringifiable. The function also removes any `None` values and
    values that are empty strings after stripping whitespace.

    Args:
        examples (list[Any]): A list of example values to be formatted.

    Returns:
        list[str]: A list of formatted string values.
    """
    if not examples:
        return []

    for value in examples:
        if isinstance(value, (datetime.date, datetime.datetime)):
            return [str(value)]
        if isinstance(value, decimal.Decimal):
            value = float(value)
        if is_email(str(value)) or ('http://' in str(value)) or ('https://' in str(value)):
            return []

    return [str(v) for v in examples if v is not None and str(v).strip()]