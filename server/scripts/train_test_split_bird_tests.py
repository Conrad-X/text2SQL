import json
import os
from sklearn.model_selection import train_test_split

from utilities.config import UNMASKED_SAMPLE_DATA_FILE_PATH, TEST_DATA_FILE_PATH, BIRD_TRAIN_DATASET_DIR

TRAIN_FILE = "././data/bird/train/train.json"

def split_database_data(json_file_path):
    """ Reads a JSON file and splits the data into databases, samples, and tests. """
    
    with open(json_file_path, 'r') as file:
        data = json.load(file)

    # Get all database names
    dbs = {}
    for entry in data:
        db_id = entry['db_id']
        if db_id not in dbs:
            dbs[db_id] = []
        dbs[db_id].append(entry)

    # Split data and save it to files
    for db_id, entries in dbs.items():
        sample_set, test_set = train_test_split(entries, test_size=0.3, random_state=42)
        
        # Create directories if they don't exist
        sample_dir = os.path.dirname(UNMASKED_SAMPLE_DATA_FILE_PATH.format(database_name=db_id))
        os.makedirs(sample_dir, exist_ok=True)

        test_dir = os.path.dirname(TEST_DATA_FILE_PATH.format(database_name=db_id))
        os.makedirs(test_dir, exist_ok=True)

        # Format sample_set with "db_id", "question", and "answer" keys
        formatted_samples = [
            {
                "id": idx + 1,
                "question": entry["question"],
                "evidence": entry["evidence"],
                "answer": entry["SQL"]
            }
            for idx, entry in enumerate(sample_set)
        ]

        # Create files and save data
        with open(UNMASKED_SAMPLE_DATA_FILE_PATH.format(database_name=db_id), 'w') as file:
            json.dump(formatted_samples, file, indent=4)

        with open(TEST_DATA_FILE_PATH.format(database_name=db_id), 'w') as file:
            json.dump(test_set, file, indent=4)
        
        print(f"Saved training data to {UNMASKED_SAMPLE_DATA_FILE_PATH.format(database_name=db_id)}")
        print(f"Saved testing data to {TEST_DATA_FILE_PATH.format(database_name=db_id)}")

if __name__ == "__main__":
    # check if bird dataset is downloaded
    if not os.path.isdir(BIRD_TRAIN_DATASET_DIR):
        print(f"Dataset not found. Please download the dataset from this url: https://bird-bench.oss-cn-beijing.aliyuncs.com/train.zip")
        print("Copy paste the unzipped bird data in server/data/bird directory and then unzip the train_databases.zip inside the train dir into the same folder")
        # write print statements to see the project strcuture
    else:
        split_database_data(TRAIN_FILE)