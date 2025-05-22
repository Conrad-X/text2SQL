"""Process and split the BIRD dataset for training and testing.

This module provides functionality to process the BIRD dataset, apply masking to
questions, and split the data into training and testing sets. It supports both
random and deterministic splitting methods.
"""

import json
import os
import sqlite3
from pathlib import Path

from alive_progress import alive_bar
from sklearn.model_selection import train_test_split
from utilities.config import PATH_CONFIG
from utilities.constants.database_enums import DatasetType
from utilities.generate_schema_used import get_sql_columns_dict
from utilities.masking.linking_process import SpiderEncoderV2Preproc
from utilities.masking.linking_utils.utils import (
    load_tables, mask_single_question_with_schema_linking)
from utilities.masking.pretrained_embeddings import GloVe

INTERMEDIARY_FILE = Path(__file__).parent.parent / "cache.json"

def read_json_file(path):
    """Read and parse a JSON file.
    
    Args:
        path: Path to the JSON file to read.
        
    Returns:
        The parsed JSON content as a Python object.
    """
    with open(path, "r") as file:
        json_file = json.load(file)
        file.close()
    return json_file


def write_json_file(path, content):
    """Write content to a JSON file.
    
    Args:
        path: Path where the JSON file should be written.
        content: Python object to be serialized to JSON.
    """
    with open(path, "w") as file:
        json.dump(content, file, indent=4)
        file.close()


def json_preprocess(data_jsons, with_evidence):
    """Preprocess JSON data for text-to-SQL tasks.
    
    Processes each question in the dataset, handling evidence integration
    if requested, tokenizing questions, and preparing the data structure
    for further processing.
    
    Args:
        data_jsons: List of question data dictionaries to process.
        with_evidence: Whether to include evidence text in the question.
        
    Returns:
        A list of preprocessed data dictionaries.
    """
    new_datas = []
    for data_json in data_jsons:
        if with_evidence and len(data_json["evidence"]) > 0:
            data_json["question"] = (
                data_json["question"] + " " + data_json["evidence"]
            ).strip()
        question = data_json["question"]
        tokens = []
        for token in question.split(" "):
            if len(token) == 0:
                continue
            if token[-1] in ["?", ".", ":", ";", ","] and len(token) > 1:
                tokens.extend([token[:-1], token[-1:]])
            else:
                tokens.append(token)
        data_json["question_toks"] = tokens
        data_json["query"] = data_json["SQL"]
        new_datas.append(data_json)
    return new_datas

def mask_all_questions(json_file_path, with_evidence, dataset_type):
    """Mask all questions in the dataset for schema linking.
    
    Processes a JSON file containing questions, applies masking to entities
    that match database schema elements, and enriches the data with
    schema linking information.
    
    Args:
        json_file_path: Path to the JSON file containing questions.
        with_evidence: Whether to include evidence text in the questions.
        dataset_type: Type of dataset being processed (train, dev, etc.).
        
    Returns:
        A list of processed question data with masking applied.
    """
    # load data
    data = read_json_file(json_file_path)

    # Adding indexes if not present
    if dataset_type == DatasetType.BIRD_TRAIN:
        for idx, item in enumerate(data):
            item["question_id"] = idx

    preprocessed_data = json_preprocess(data, with_evidence)
    preprocessed_data = sorted(preprocessed_data, key=lambda x: x["db_id"])

    schemas, _ = load_tables([PATH_CONFIG.bird_schema_file_path(dataset_type=dataset_type)])

    # load cache
    if os.path.exists(INTERMEDIARY_FILE):
        cache = read_json_file(INTERMEDIARY_FILE)
    else:
        cache = {}
        write_json_file(INTERMEDIARY_FILE, cache)

    # loading tokenizer
    word_emb = GloVe(kind="42B", lemmatize=True)

    # loading linking processor
    linking_processor = SpiderEncoderV2Preproc(
        PATH_CONFIG.dataset_dir(dataset_type=dataset_type),
        min_freq=4,
        max_count=5000,
        include_table_name_in_column=False,
        word_emb=word_emb,
        fix_issue_16_primary_keys=True,
        compute_sc_link=True,
        compute_cv_link=False,
    )

    # getting original table and column names
    tables, columns = {}, {}
    for database in schemas:
        tables[database] = []
        columns[database] = []
        for table in schemas[database].tables:
            tables[database].append(table.orig_name)
        for column in schemas[database].columns:
            columns[database].append(column.orig_name)

    current_db = preprocessed_data[0]["db_id"]
    connection = make_sqlite_connection(
        PATH_CONFIG.sqlite_path(database_name=current_db, dataset_type=dataset_type)
    )

    with alive_bar(
        len(preprocessed_data),
        bar="fish",
        spinner="fish2",
        title="Processing all Questions",
    ) as bar:

        for idx, question in enumerate(preprocessed_data):
            if str(question["question_id"]) in list(cache.keys()):
                print("Skipping already processed question: ", question["question_id"])
                preprocessed_data[idx] = cache[str(question["question_id"])]
                bar()
                continue

            # if the db changes then delete previous connection and connect to new one
            if current_db != question["db_id"]:
                connection.close()
                connection = make_sqlite_connection(
                    PATH_CONFIG.sqlite_path(database_name=question["db_id"], dataset_type=dataset_type)
                )
            current_db = question["db_id"]
            database = question["db_id"]
            schema = schemas[database]

            linking_info = linking_processor.add_item(question, schema, connection)
            masked_question = mask_single_question_with_schema_linking(
                linking_info, "<mask>", "<unk>"
            )
            question["masked_question"] = masked_question
            question["linking_info"] = {
                key: value
                for key, value in linking_info.items()
                if key in ["sc_link", "cv_link", "question"]
            }
            matched_columns_idx, matched_tables_idx = (
                linking_info["matched_columns"],
                linking_info["matched_tables"],
            )

            orig_table_names = tables[database]
            orig_column_names = columns[database]

            # getting original names of matched column and table names
            matched_tables = [orig_table_names[idx] for idx in matched_tables_idx]
            matched_columns = [orig_column_names[idx] for idx in matched_columns_idx]
            question["matched_tables"] = matched_tables
            question["matched_columns"] = matched_columns
            question["schema_used"] = get_sql_columns_dict(
                PATH_CONFIG.sqlite_path(database_name=question["db_id"], dataset_type=dataset_type),
                question["SQL"],
            )

            cache[question["question_id"]] = question

            # saving cache, comment the line below for faster processing saving cache increases time exponentially
            write_json_file(INTERMEDIARY_FILE, cache)

            bar()

    return preprocessed_data


def split_questions(questions, random, test_size):
    """Split a list of questions into training and testing sets.
    
    Args:
        questions: List of question data dictionaries to split.
        random: Whether to use random splitting (True) or deterministic splitting (False).
        test_size: Proportion of the dataset to include in the test split.
        
    Returns:
        A tuple containing (training_set, testing_set).
    """
    if random:
        sample_set, test_set = train_test_split(
            questions, test_size=test_size, random_state=42
        )
    else:
        # Sort the entries by a criterion (e.g., "question_id" or any other attribute you want)
        sorted_entries = sorted(
            questions, key=lambda x: x["question_id"]
        )  # Or any other key you choose

        # Select the top test size for the test set
        test_size = int(len(sorted_entries) * test_size)
        test_set = sorted_entries[:test_size]

        # The remaining goes into the sample set
        sample_set = sorted_entries[test_size:]

    return sample_set, test_set


def split_database_data(data, random_split, test_size):
    """Split data into training and testing sets by database.
    
    Groups data by database ID, then splits each database's questions into
    training and testing sets according to the specified parameters.
    Writes the resulting data to separate files for each database.
    
    Args:
        data: List of question data dictionaries to split.
        random_split: Whether to use random splitting (True) or deterministic splitting (False).
        test_size: Proportion of the dataset to include in the test split.
    """
    dbs = {}
    for entry in data:
        db_id = entry["db_id"]
        if db_id not in dbs:
            dbs[db_id] = []
        dbs[db_id].append(entry)

    # Creating indexes if dataset is train as train dataset is missing indexes
    if PATH_CONFIG.dataset_type == DatasetType.BIRD_TRAIN:
        for idx, item in enumerate(data):
            item["question_id"] = idx

    # Split data and save it to files
    for db_id, entries in dbs.items():
        sample_set, test_set = split_questions(entries, random_split, test_size)

        if test_size != 1.0:
            sample_dir = os.path.dirname(PATH_CONFIG.processed_train_path(database_name=db_id))
            os.makedirs(sample_dir, exist_ok=True)

            with open(PATH_CONFIG.processed_train_path(database_name=db_id), "w") as file:
                json.dump(sample_set, file, indent=4)

        if test_size != 0.0:
            test_dir = os.path.dirname(PATH_CONFIG.processed_test_path(database_name=db_id))
            os.makedirs(test_dir, exist_ok=True)

            with open(PATH_CONFIG.processed_test_path(database_name=db_id), "w") as file:
                json.dump(test_set, file, indent=4)


if __name__ == "__main__":
    """
    To run this script:

    1. Ensure you have set the correct `PATH_CONFIG.dataset_type` and `PATH_CONFIG.sample_dataset_type` in `utilities.config`:
       - Set `PATH_CONFIG.dataset_type` to DatasetType.BIRD_TRAIN for training data.
       - Set `PATH_CONFIG.dataset_type` to DatasetType.BIRD_DEV for development data.
       - Set `PATH_CONFIG.sample_dataset_type` to DatasetType.BIRD_DEV or DatasetType.BIRD_TRAIN.

    2. Download the dataset if not already available:
       - For training data, download from: https://bird-bench.oss-cn-beijing.aliyuncs.com/train.zip
       - For development data, download from: https://bird-bench.oss-cn-beijing.aliyuncs.com/dev.zip
       - Place the unzipped contents in the `data/bird` directory as follows:
         `./data/bird/train/train.json` or `./data/bird/dev/dev.json`

    3. Run the script:
       - In the terminal, run `python3 -m scripts.train_test_split_bird`.
       - The script will check if the dataset exists in the correct directory. If not, it will prompt you to download and place it.

    Expected Output:
       - Training data saved to files in the directory specified by `PATH_CONFIG.processed_train_path()`.
       - Testing data saved to files in the directory specified by `PATH_CONFIG.processed_test_path()`.
    """
    if not os.path.isdir(PATH_CONFIG.dataset_dir()):
        url = (
            "https://bird-bench.oss-cn-beijing.aliyuncs.com/train.zip"
            if PATH_CONFIG.dataset_type == DatasetType.BIRD_TRAIN
            else "https://bird-bench.oss-cn-beijing.aliyuncs.com/dev.zip"
        )

        print(
            "Dataset not found. Please download the dataset from the following URL:", url,
            "\nAfter downloading, ensure to extract the contents of the ZIP file.",
            "\nCopy and paste the unzipped bird data into the 'server/data/bird' directory.",
            "\nOpen the folder and make sure to unzip the train_databases/dev_databases directly into this folder.",
            "\nThe folder structure should be as follows:",
            "\n - 'train' folder containing 'train.json' or 'dev' folder containing 'dev.json'",
            "\n - Ensure that 'train_databases/dev_databases' are located directly inside the 'train/dev_20240627' directory."
        )

    if PATH_CONFIG.dataset_type == PATH_CONFIG.sample_dataset_type:
        file_path = PATH_CONFIG.bird_file_path()

        # Inputs
        with_evidence = False
        random_split = True
        test_size = 0.5

        # Test size should be a range in [0.0, 1.0] for train_test_split to work so they cannot be random
        if test_size == 0.0 or test_size == 1.0:
            random_split = False

        all_processed_questions = mask_all_questions(file_path, with_evidence, PATH_CONFIG.dataset_type)
        split_database_data(all_processed_questions, random_split, test_size)
        sample_set, test_set = split_questions(
            all_processed_questions, random_split, test_size
        )

        # Saving all questions in a single file
        write_json_file(PATH_CONFIG.processed_train_path(global_file=True), sample_set)
        write_json_file(PATH_CONFIG.processed_test_path(global_file=True), test_set)

        # clearing cache
        write_json_file(INTERMEDIARY_FILE, {})

    else:
        dataset_file_path = PATH_CONFIG.bird_file_path()
        sample_dataset_file_path = PATH_CONFIG.bird_file_path(PATH_CONFIG.sample_dataset_type)

        # Inputs
        with_evidence = False

        all_test_processed_questions = mask_all_questions(dataset_file_path, with_evidence, PATH_CONFIG.dataset_type)
        split_database_data(all_test_processed_questions, False, 1.0)
        write_json_file(PATH_CONFIG.processed_test_path(global_file=True), all_test_processed_questions)

        # clearing cache
        write_json_file(INTERMEDIARY_FILE, {})

        all_train_processed_questions = mask_all_questions(sample_dataset_file_path, with_evidence, PATH_CONFIG.sample_dataset_type)
        write_json_file(PATH_CONFIG.processed_train_path(), all_train_processed_questions)
        
        # clearing cache
        write_json_file(INTERMEDIARY_FILE, {})

    print("All files Successfully Made!")
    

