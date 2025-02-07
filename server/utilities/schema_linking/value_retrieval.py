import os
import pickle
import random
import sqlite3
import logging
from alive_progress import alive_bar
from typing import Dict, Tuple, List, Union

import concurrent.futures
from datasketch import MinHash, MinHashLSH

from utilities.constants.response_messages import (
    ERROR_INVALID_FETCH_ARGUMENT,
    ERROR_SQL_QUERY_TIMEOUT,
    ERROR_LSH_CREATION,
    ERROR_LSH_LOADING,
    ERROR_DATABASE_PROCESSING,
)
from utilities.config import (
    DATABASE_SQLITE_PATH,
    UNIQUE_VALUES_PATH,
    LSH_PATH,
    MINHASHES_PATH,
    PREPROCESSED_DIR_PATH,
    DATASET_DIR,
)

# Constants
SIGNATURE_SIZE = 100
N_GRAM = 3
THRESHOLD = 0.1


def execute_sql(
    db_path: str, sql: str, fetch: Union[str, int] = "all", timeout: int = 60
):
    """
    Executes an SQL query
    """

    with sqlite3.connect(db_path, timeout=timeout) as conn:
        cursor = conn.cursor()
        cursor.execute(sql)

        if fetch == "all":
            return cursor.fetchall()
        if fetch == "one":
            return cursor.fetchone()
        if fetch == "random":
            samples = cursor.fetchmany(10)
            return random.choice(samples) if samples else []
        if isinstance(fetch, int):
            return cursor.fetchmany(fetch)

        raise ValueError(ERROR_INVALID_FETCH_ARGUMENT)


def _get_unique_values(db_path: str):
    """
    Retrieves unique text values from the database excluding primary keys.
    """

    table_names = [
        table[0]
        for table in execute_sql(
            db_path, "SELECT name FROM sqlite_master WHERE type='table';", fetch="all"
        )
    ]
    primary_keys = []

    for table_name in table_names:
        columns = execute_sql(
            db_path, f"PRAGMA table_info('{table_name}')", fetch="all"
        )
        for column in columns:
            if column[5] > 0:  # Check if it's a primary key
                column_name = column[1]
                if column_name.lower() not in [c.lower() for c in primary_keys]:
                    primary_keys.append(column_name)

    unique_values: Dict[str, Dict[str, List[str]]] = {}
    for table_name in table_names:
        if table_name == "sqlite_sequence":
            continue

        columns = [
            col[1]
            for col in execute_sql(
                db_path, f"PRAGMA table_info('{table_name}')", fetch="all"
            )
            if (
                "TEXT" in col[2]
                and col[1].lower() not in [c.lower() for c in primary_keys]
            )
        ]

        table_values: Dict[str, List[str]] = {}

        for column in columns:
            if any(
                keyword in column.lower()
                for keyword in [
                    "_id",
                    " id",
                    "url",
                    "email",
                    "web",
                    "time",
                    "phone",
                    "date",
                    "address",
                ]
            ) or column.endswith("Id"):
                continue

            try:
                result = execute_sql(
                    db_path,
                    f"""
                    SELECT SUM(LENGTH(unique_values)), COUNT(unique_values)
                    FROM (
                        SELECT DISTINCT `{column}` AS unique_values
                        FROM `{table_name}`
                        WHERE `{column}` IS NOT NULL
                    ) AS subquery
                """,
                    fetch="one",
                    timeout=480,
                )
            except Exception:
                result = 0, 0

            sum_of_lengths, count_distinct = result
            if sum_of_lengths is None or count_distinct == 0:
                continue

            average_length = sum_of_lengths / count_distinct

            if (
                ("name" in column.lower() and sum_of_lengths < 5000000)
                or (sum_of_lengths < 2000000 and average_length < 25)
                or count_distinct < 100
            ):
                try:
                    values = [
                        str(value[0])
                        for value in execute_sql(
                            db_path,
                            f"SELECT DISTINCT `{column}` FROM `{table_name}` WHERE `{column}` IS NOT NULL",
                            fetch="all",
                            timeout=480,
                        )
                    ]
                except Exception:
                    values = []

                table_values[column] = values

        unique_values[table_name] = table_values

    return unique_values


def _create_minhash(string: str):
    """
    Creates a MinHash object for a given string.
    """
    m = MinHash(num_perm=SIGNATURE_SIZE)
    for d in [string[i : i + N_GRAM] for i in range(len(string) - N_GRAM + 1)]:
        m.update(d.encode("utf8"))
    return m


def skip_column(column_name: str, column_values: List[str]):
    """
    Determines whether to skip processing a column based on its values.
    """

    if "name" in column_name.lower():
        return False

    sum_of_lengths = sum(len(value) for value in column_values)
    average_length = sum_of_lengths / len(column_values)
    return (sum_of_lengths > 50000) and (average_length > 20)


def make_lsh(unique_values: Dict[str, Dict[str, List[str]]]):
    """
    Creates a MinHash LSH from unique values.
    """

    lsh = MinHashLSH(threshold=THRESHOLD, num_perm=SIGNATURE_SIZE)

    minhashes: Dict[str, Tuple[MinHash, str, str, str]] = {}

    try:
        for table_name, table_values in unique_values.items():
            for column_name, column_values in table_values.items():
                for idx, value in enumerate(column_values):
                    minhash = _create_minhash(value)
                    minhash_key = f"{table_name}_{column_name}_{idx}"
                    minhashes[minhash_key] = (minhash, table_name, column_name, value)
                    lsh.insert(minhash_key, minhash)

    except Exception as e:
        logging.error(ERROR_LSH_CREATION.format(error=e))

    return lsh, minhashes


def make_db_lsh(database_name: str):
    """
    Creates a MinHash LSH for the database and saves the results.
    If the unique values, LSH, and minhashes pickle files already exist, then it skips creation.
    """

    # Create the preprocessed directory
    preprocessed_dir = PREPROCESSED_DIR_PATH.format(database_name=database_name)
    os.makedirs(preprocessed_dir, exist_ok=True)

    # Build file paths
    unique_values_file = UNIQUE_VALUES_PATH.format(database_name=database_name)
    lsh_file = LSH_PATH.format(database_name=database_name)
    minhashes_file = MINHASHES_PATH.format(database_name=database_name)

    # Load unique values if they exist, otherwise create and save them
    if os.path.exists(unique_values_file):
        with open(unique_values_file, "rb") as file:
            unique_values = pickle.load(file)
    else:
        unique_values = _get_unique_values(
            DATABASE_SQLITE_PATH.format(database_name=database_name)
        )
        with open(unique_values_file, "wb") as file:
            pickle.dump(unique_values, file)

    lsh, minhashes = make_lsh(unique_values)

    with open(lsh_file, "wb") as file:
        pickle.dump(lsh, file)

    with open(minhashes_file, "wb") as file:
        pickle.dump(minhashes, file)


def load_db_lsh(database_name: str):
    """
    Loads the LSH and MinHashes from the preprocessed files.
    If the files do not exist, it creates them and then loads them.
    """

    lsh_file = LSH_PATH.format(database_name=database_name)
    minhashes_file = MINHASHES_PATH.format(database_name=database_name)

    # Check if the required files exist
    if not os.path.exists(lsh_file) or not os.path.exists(minhashes_file):
        make_db_lsh(database_name)

    # Load the LSH and MinHashes
    try:
        with open(lsh_file, "rb") as file:
            lsh = pickle.load(file)
        with open(minhashes_file, "rb") as file:
            minhashes = pickle.load(file)
        return lsh, minhashes
    except Exception as e:
        logging.error(ERROR_LSH_LOADING.format(database_name=database_name, error=e))
        raise e


def query_lsh(
    lsh: MinHashLSH,
    minhashes: Dict[str, Tuple[MinHash, str, str, str]],
    keyword: str,
    top_n: int = 10,
) -> Dict[str, Dict[str, List[str]]]:
    """
    Queries the LSH for similar values to the given keyword and returns the top results.
    """

    schema = {}

    # Create the query minhash
    query_minhash = _create_minhash(keyword)

    # Query the LSH and get the top results
    results = lsh.query(query_minhash)

    # Calculate Jaccard similarity for the top results
    similarities = [
        (result, query_minhash.jaccard(minhashes[result][0])) for result in results
    ]
    similarities = sorted(similarities, key=lambda x: x[1], reverse=True)[
        :top_n
    ]  # Get top N results

    # Convert results to schema format {table1: [col2, col2...], table2: [col1, col2...]}
    for result, _ in similarities:
        table_name, column_name, _ = minhashes[result][1:]

        if table_name not in schema:
            schema[table_name] = []

        if column_name not in schema[table_name]:
            schema[table_name].append(column_name)

    return schema


def get_table_column_of_similar_values(
    keywords: list,
    top_n: int,
    database_name: str,
):
    """
    Retrieves the table and column names of similar values to the given keyword.
    """

    # Load the LSH and MinHashes
    lsh, minhashes = load_db_lsh(database_name=database_name)

    def process_keyword(keyword: str):
        return query_lsh(lsh, minhashes, keyword, top_n=top_n)

    final_schema = {}
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(executor.map(process_keyword, keywords))

    # Merge the individual schema dictionaries into one final schema
    for schema in results:
        for table, columns in schema.items():
            if table not in final_schema:
                final_schema[table] = []
            for col in columns:
                if col not in final_schema[table]:
                    final_schema[table].append(col)

    return final_schema


def create_lsh_for_all_databases(dataset_dir: str = DATASET_DIR):
    """
    Creates MinHash LSH for all databases using threads.
    """

    databases = [
        d
        for d in os.listdir(dataset_dir)
        if os.path.isdir(os.path.join(dataset_dir, d))
    ]

    with alive_bar(len(databases)) as bar:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {executor.submit(make_db_lsh, db): db for db in databases}
            for future in concurrent.futures.as_completed(futures):
                bar()
                try:
                    future.result()
                except Exception as e:
                    logging.error(
                        ERROR_DATABASE_PROCESSING.format(
                            database=futures[future], error=e
                        )
                    )
