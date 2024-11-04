import json
import os
from sklearn.model_selection import train_test_split

from utilities.config import DATASET_TYPE, UNMASKED_SAMPLE_DATA_FILE_PATH, TEST_DATA_FILE_PATH, DATASET_DIR, TEST_GOLD_DATA_FILE_PATH

DEV_FILE = "././data/bird/dev_20240627/dev.json"
TRAIN_FILE = "././data/bird/train/train.json"

def split_database_data(json_file_path):
    """ Reads a JSON file and splits the data into databases, samples, and tests. """
    
    with open(json_file_path, 'r') as file:
        data = json.load(file)

    # Creating indexes if dataset is train as train dataset is missing indexes
    if DATASET_TYPE == "bird_train":
        for idx, item in enumerate(data):
            item['question_id']=idx

    # Get all database names
    dbs = {}
    for entry in data:
        db_id = entry['db_id']
        if db_id not in dbs:
            dbs[db_id] = []
        dbs[db_id].append(entry)

    # Split data and save it to files
    for db_id, entries in dbs.items():
        sample_set, test_set = train_test_split(entries, test_size=0.7, random_state=42)
        
        # Create directories if they don't exist
        sample_dir = os.path.dirname(UNMASKED_SAMPLE_DATA_FILE_PATH.format(database_name=db_id))
        os.makedirs(sample_dir, exist_ok=True)

        test_dir = os.path.dirname(TEST_DATA_FILE_PATH.format(database_name=db_id))
        os.makedirs(test_dir, exist_ok=True)

        # Format sample_set with "id", "question", "evidence", and "answer" keys
        formatted_samples = [
            {
                "id": entry["question_id"],
                "question": entry["question"],
                "evidence": entry["evidence"],
                "answer": entry["SQL"]
            }
            for entry in sample_set
        ]

        # Create files and save data
        with open(UNMASKED_SAMPLE_DATA_FILE_PATH.format(database_name=db_id), 'w') as file:
            json.dump(formatted_samples, file, indent=4)

        with open(TEST_DATA_FILE_PATH.format(database_name=db_id), 'w') as file:
            json.dump(test_set, file, indent=4)
        
        with open(TEST_GOLD_DATA_FILE_PATH.format(database_name=db_id), 'w') as file:
            for entry in test_set:
                file.write(entry["SQL"] + "\n")
        
        print(f"Saved training data to {UNMASKED_SAMPLE_DATA_FILE_PATH.format(database_name=db_id)}")
        print(f"Saved testing data to {TEST_DATA_FILE_PATH.format(database_name=db_id)}")
        print(f"Saved test gold data to {TEST_GOLD_DATA_FILE_PATH.format(database_name=db_id)}\n")

if __name__ == "__main__":
    """
    To run this script:
    
    1. Ensure you have set the correct `DATASET_TYPE` in `utilities.config`:
       - Set `DATASET_TYPE` to "bird_train" for training data.
       - Set `DATASET_TYPE` to "bird_dev" for development data.

    2. Download the dataset if not already available:
       - For training data, download from: https://bird-bench.oss-cn-beijing.aliyuncs.com/train.zip
       - For development data, download from: https://bird-bench.oss-cn-beijing.aliyuncs.com/dev.zip
       - Place the unzipped contents in the `data/bird` directory as follows:
         `./data/bird/train/train.json` or `./data/bird/dev/dev.json`

    3. Run the script:
       - In the terminal, run `python3 -m scripts.train_test_split_bird`.
       - The script will check if the dataset exists in the correct directory. If not, it will prompt you to download and place it.

    Expected Output:
       - Training data saved to files in the directory specified by `UNMASKED_SAMPLE_DATA_FILE_PATH`.
       - Testing data saved to files in the directory specified by `TEST_DATA_FILE_PATH`.
       - Test gold data saved to files in the directory specified by `TEST_GOLD_DATA_FILE_PATH`.
    """

    # Determine file path based on dataset type
    file_path = TRAIN_FILE if DATASET_TYPE == "bird_train" else DEV_FILE if DATASET_TYPE == "bird_dev" else None
    
    if not file_path:
        print("Choose a bird dataset for this script")
        exit(1)

    # Check if bird dataset is downloaded
    if not os.path.isdir(DATASET_DIR):
        url = "https://bird-bench.oss-cn-beijing.aliyuncs.com/train.zip" if DATASET_TYPE == "bird_train" else "https://bird-bench.oss-cn-beijing.aliyuncs.com/dev.zip"

        print("Dataset not found. Please download the dataset from the following URL:", url)
        print("After downloading, ensure to extract the contents of the ZIP file.")
        print("Copy and paste the unzipped bird data into the 'server/data/bird' directory.")
        print("Open the folder and make sure to unzip the train_databases/dev_databases directly into this folder.")
        print("The folder structure should be as follows:")
        print(" - 'train' folder containing 'train.json' or 'dev' folder containing 'dev.json'")
        print(" - Ensure that 'train_databases/dev_databases' are located directly inside the 'train/dev_20240627' directory.")
    else:
        split_database_data(file_path)

