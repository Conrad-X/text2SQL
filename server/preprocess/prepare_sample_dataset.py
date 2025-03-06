
import json
import os
import shutil
import sqlite3
from alive_progress import alive_bar

from utilities.constants.database_enums import DatasetType
from utilities.constants.LLM_enums import LLMType, ModelType
from utilities.generate_schema_used import get_sql_columns_dict
from utilities.config import PATH_CONFIG
from preprocess.add_descriptions_bird_dataset import add_database_descriptions


def get_train_file_path():
    """ Checks if the processed_test_path exists and if not then create it """

    train_file = PATH_CONFIG.processed_train_path()

    if not os.path.exists(train_file):
        # Copy contents from files ( dev.json / train.json / test.json ) to processed_train_path
        open(train_file, 'a').close()
        bird_file_path = PATH_CONFIG.bird_file_path(dataset_type=PATH_CONFIG.sample_dataset_type)
        shutil.copy(bird_file_path, train_file)

        # Add question id for bird train as train does not have that for each quetsion
        if PATH_CONFIG.sample_dataset_type == DatasetType.BIRD_TRAIN:
            with open(train_file, "r") as file:
                data = json.load(file)

            for idx, item in enumerate(data):
                item["question_id"] = idx
            
            with open(train_file, "w") as file:
                json.dump(data, file, indent=4)

    return train_file

def make_sqlite_connection(path):
    source: sqlite3.Connection
    with sqlite3.connect(str(path)) as source:
        dest = sqlite3.connect(":memory:")
        dest.row_factory = sqlite3.Row
        source.backup(dest)
    return dest

def add_schema_used(train_file, dataset_type):
    """Add schema_used field to each item in the train file"""

    with open(train_file, "r") as file:
        train_data = json.load(file)

    current_db = train_data[0]["db_id"]
    connection = make_sqlite_connection(
        PATH_CONFIG.sqlite_path(database_name=current_db, dataset_type=dataset_type)
    )

    with alive_bar(
        len(train_data),
        bar="fish",
        spinner="fish2",
        title="Adding Schema Used in items",
    ) as bar:

        for item in train_data:
            if "schema_used" in item:
                print("Skipping already processed item: ", item["question_id"])
                bar()
                continue

            # if the db changes then delete previous connection and connect to new one
            if current_db != item["db_id"]:
                connection.close()
                connection = make_sqlite_connection(
                    PATH_CONFIG.sqlite_path(database_name=item["db_id"], dataset_type=dataset_type)
                )
                # Save Partial Processed Data
                with open(train_file, "w") as file:
                    json.dump(train_data, file, indent=4)

            current_db = item["db_id"]

            item["schema_used"] = get_sql_columns_dict(
                PATH_CONFIG.sqlite_path(database_name=item["db_id"], dataset_type=dataset_type),
                item["SQL"],
            )

            bar()

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

    # LLM Configurations
    llm_type = LLMType.GOOGLE_AI
    model = ModelType.GOOGLEAI_GEMINI_2_0_FLASH
    temperature = 0.7
    max_tokens = 8192

    add_database_descriptions(
        dataset_type=PATH_CONFIG.sample_dataset_type,
        llm_type=llm_type,
        model=model,
        temperature=temperature,
        max_tokens=max_tokens,
    )