import sqlite3
from nltk import word_tokenize, pos_tag
from nltk.corpus import wordnet
import re
import os
from enum import Enum
import concurrent.futures
import pandas as pd
import yaml

from utilities.constants.response_messages import (
    ERROR_DATABASE_QUERY_FAILURE,
    ERROR_SQL_QUERY_REQUIRED,
    ERROR_INVALID_MODEL_FOR_TYPE,
    ERROR_UNSUPPORTED_CLIENT_TYPE,
    ERROR_SQL_MASKING_FAILED,
    ERROR_UNSUPPORTED_FORMAT_TYPE,
    ERROR_FAILED_FETCH_COLUMN_NAMES,
    ERROR_FAILED_FETCH_TABLE_NAMES,
)

from utilities.constants.LLM_enums import LLMType, ModelType, VALID_LLM_MODELS
from utilities.constants.prompts_enums import FormatType
from utilities.config import PATH_CONFIG
from utilities.m_schema.schema_engine import SchemaEngine
from sqlalchemy import create_engine


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
            description[0].split()[-1] if " " in description[0] else description[0]
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
            raise RuntimeError(ERROR_DATABASE_QUERY_FAILURE.format(error=str(e)))
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
        raise RuntimeError((ERROR_FAILED_FETCH_TABLE_NAMES.format(error=str(e))))


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
        raise RuntimeError((ERROR_FAILED_FETCH_COLUMN_NAMES.format(error=str(e))))


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

        cols_from_conn = [i.lower() for i in get_table_columns(connection, table)]
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


def format_schema(
    format_type: FormatType, database_name: str = None, matches=None, dataset_type=None
):
    """
    Formats the database schema based on the specified format type.
    Pass matches as None to return the full schema and matches as a dict as table: [columns...] to return pruned schema
    """
    database_name = database_name if database_name else PATH_CONFIG.database_name
    db_path = PATH_CONFIG.sqlite_path(
        database_name=database_name, dataset_type=dataset_type
    )

    connection = sqlite3.connect(db_path)
    if matches:
        matches = {
            key.lower(): [item.lower() for item in value]
            for key, value in matches.items()
        }
    try:
        table_names = get_table_names(connection)
        filtered_table_names = [
            name
            for name in table_names
            if "alembic" not in name.lower() and "index" not in name.lower()
        ]
        formatted_schema = []

        if format_type == FormatType.SEMANTIC:
            cursor = connection.cursor()
            base_path = PATH_CONFIG.description_dir(
                database_name=database_name, dataset_type=dataset_type
            )
            table_description_csv_path = os.path.join(
                base_path, f"{database_name}_tables.csv"
            )
            table_description_df = pd.read_csv(table_description_csv_path)

            schema_yaml = []

            for table_csv in os.listdir(base_path):

                if table_csv.endswith(".csv") and not table_csv.startswith(
                    database_name
                ):
                    if (
                        matches
                        and table_csv.removesuffix(".csv").lower()
                        in list(matches.keys())
                    ) or not matches:
                        table_name = table_csv.split(".csv")[0]
                        table_df = pd.read_csv(os.path.join(base_path, table_csv))
                        table_description = (
                            table_description_df.loc[
                                table_description_df["table_name"] == table_name,
                                "table_description",
                            ].values[0]
                            if len(table_description_df) > 0
                            else "No description available."
                        )

                        # Initialize the table entry
                        table_entry = {
                            "Table": table_name,
                            "Description": table_description.strip(),
                            "Columns": [],
                        }

                        for _, row in table_df.iterrows():
                            column_name = str(row["original_column_name"]).strip()
                            if not matches or (
                                matches
                                and column_name.lower()
                                in matches[table_csv.removesuffix(".csv").lower()]
                            ):
                                column_type = (
                                    row["data_format"].upper()
                                    if pd.notna(row["data_format"])
                                    else ""
                                )

                                column_description = row[
                                    "improved_column_description"
                                ].strip("\n")

                                # Get first row as example
                                query = f'SELECT "{column_name}" FROM "{table_name}" LIMIT 1'
                                cursor.execute(query)
                                result = cursor.fetchone()
                                example_value = result[0] if result else "N/A"

                                # Add column entry
                                column_entry = f"{column_name}: {column_type}, Description: {column_description}, Example: {example_value}"
                                table_entry["Columns"].append(column_entry)

                        schema_yaml.append(table_entry)

            return yaml.dump(schema_yaml, sort_keys=False, default_flow_style=False)

        elif format_type == FormatType.M_SCHEMA:
            db_engine = create_engine(f"sqlite:///{db_path}")
            schema_engine = SchemaEngine(
                engine=db_engine,
                db_name=database_name,
                dataset_type=dataset_type,
                matches=matches,
                db_path=db_path,
            )
            mschema = schema_engine.mschema
            mschema_str = mschema.to_mschema()
            return mschema_str

        for table in filtered_table_names:
            if (matches and table.lower() in list(matches.keys())) or not matches:
                columns = get_table_columns(connection, table)
                if matches:
                    columns = [
                        col for col in columns if col.lower() in matches[table.lower()]
                    ]

                if format_type == FormatType.BASIC:
                    # Format: Table table_name, columns = [ col1, col2, col3 ]
                    formatted_schema.append(
                        f"Table {table}, columns = [ {', '.join(columns)} ]"
                    )
                elif format_type == FormatType.TEXT:
                    # Format: table_name: col1, col2, col3
                    formatted_schema.append(f"{table}: {', '.join(columns)}")
                elif format_type == FormatType.CODE:
                    # Format in SQL create table form
                    cursor = connection.cursor()
                    cursor.execute(
                        f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table}';"
                    )
                    create_table_sql = cursor.fetchone()
                    if matches:
                        res = prune_code(
                            create_table_sql[0],
                            matches[table.lower()],
                            connection,
                            table,
                        )
                    else:
                        res = (
                            create_table_sql[0]
                            if create_table_sql
                            else f"-- Missing SQL for {table}"
                        )
                    formatted_schema.append(res)
                elif format_type == FormatType.OPENAI:
                    # Format in OpenAI demo style: # table_name ( col1, col2, col3 )
                    formatted_schema.append(f"# {table} ( {', '.join(columns)} )")
                else:
                    raise ValueError(
                        (ERROR_UNSUPPORTED_FORMAT_TYPE.format(format_type=format_type))
                    )
        return "\n".join(formatted_schema)
    finally:
        connection.close()


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

        is_table_or_column = any(word_lower in tab for tab in table_and_column_names)
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
    input_config: dict, gold_config: dict, path: str | None = ""
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
                    errors.append(f"Key '{full_key}' should be a dict or None.")
                elif key != "improve_config":
                    errors.append(f"Key '{full_key}' should be a dict")
            else:
                errors += check_config_types(value, expected_type, path=full_key)

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