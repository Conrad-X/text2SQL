import json
import os
from sklearn.model_selection import train_test_split
import sqlite3
from alive_progress import alive_bar
from utilities.config import DATASET_TYPE, UNMASKED_SAMPLE_DATA_FILE_PATH, TEST_DATA_FILE_PATH, DATASET_DIR, TEST_GOLD_DATA_FILE_PATH
from utilities.constants.script_constants import(
    SCHEMA_PATH,
    PROCESSED_SAMPLE_DATA_FILE_PATH,
)
from utilities.constants.database_enums import DatasetType
from utilities.masking.pretrained_embeddings import (
    GloVe
)
from utilities.masking.linking_utils.utils import(
    load_tables,
    mask_question_with_schema_linking,
    mask_single_question_with_schema_linking,
)
from utilities.masking.linking_process import (
    SpiderEncoderV2Preproc
)
from utilities.config import (
    DATASET_DIR,
    DATABASE_SQLITE_PATH, 
    UNMASKED_SAMPLE_DATA_FILE_PATH,
    MASKED_SAMPLE_DATA_FILE_PATH,
    
)

DEV_FILE = "././data/bird/dev_20240627/dev.json"
TRAIN_FILE = "././data/bird/train/train.json"
INTERMEDIARY_FILE="./cache.json"


def read_json_file(path):
    with open(path, 'r') as file:
        json_file=json.load(file)
        file.close()
    return json_file

def write_json_file(path, content):
    with open(path, 'w') as file:
        json.dump(content, file, indent=4)
        file.close()

def json_preprocess(data_jsons, with_evidence):
    new_datas = []
    for data_json in data_jsons:
        if with_evidence and len(data_json["evidence"]) > 0:
            data_json['question'] = (data_json['question'] + " " + data_json["evidence"]).strip()
        question = data_json['question']
        tokens = []
        for token in question.split(' '):
            if len(token) == 0:
                continue
            if token[-1] in ['?', '.', ':', ';', ','] and len(token) > 1:
                tokens.extend([token[:-1], token[-1:]])
            else:
                tokens.append(token)
        data_json['question_toks'] = tokens
        data_json['query'] = data_json['SQL']
        new_datas.append(data_json)
    return new_datas

def make_sqlite_connection(path):
    source: sqlite3.Connection
    with sqlite3.connect(str(path)) as source:
        dest = sqlite3.connect(':memory:')
        dest.row_factory = sqlite3.Row
        source.backup(dest)
    return dest

def mask_all_questions(json_file_path, with_evidence):

    #load data
    data = read_json_file(json_file_path)

    #Adding indexes if not present
    if DATASET_TYPE == DatasetType.BIRD_TRAIN:
        for idx, item in enumerate(data):
            item['question_id']=idx
    
    preprocessed_data = json_preprocess(data, with_evidence)
    preprocessed_data=sorted(preprocessed_data, key=lambda x: x['db_id'])
    
    schemas, _ = load_tables([SCHEMA_PATH])

    #load cache
    if os.path.exists(INTERMEDIARY_FILE):
        cache=read_json_file(INTERMEDIARY_FILE)
    else:
        cache={}
        write_json_file(INTERMEDIARY_FILE, cache)

    #loading tokenizer
    word_emb = GloVe(kind='42B', lemmatize=True)

    #loading linking processor
    linking_processor = SpiderEncoderV2Preproc(DATASET_DIR,
            min_freq=4,
            max_count=5000,
            include_table_name_in_column=False,
            word_emb=word_emb,
            fix_issue_16_primary_keys=True,
            compute_sc_link=True,
            compute_cv_link=False)
    
    #getting original table and column names 
    tables, columns = {}, {}
    for database in schemas:
        tables[database]=[]
        columns[database]=[]
        for table in schemas[database].tables:
            tables[database].append(table.orig_name)
        for column in schemas[database].columns:
            columns[database].append(column.orig_name)

    current_db=preprocessed_data[0]['db_id']
    connection=make_sqlite_connection(DATABASE_SQLITE_PATH.format(database_name=current_db))

    with alive_bar(len(preprocessed_data), bar = 'fish', spinner = 'fish2', title='Processing all Questions') as bar:
        
        for idx, question in enumerate(preprocessed_data):
            if str(question['question_id']) in list(cache.keys()):
                print('Skipping already processed question: ', question['question_id'])
                preprocessed_data[idx]=cache[str(question['question_id'])]
                bar()
                continue

            #if the db changes then delete previous connection and connect to new one
            if current_db!=question['db_id']:
                connection.close()
                connection=make_sqlite_connection(DATABASE_SQLITE_PATH.format(database_name=question['db_id']))
            current_db=question['db_id']
            database=question['db_id']
            schema=schemas[database]
        

            linking_info=linking_processor.add_item(question, schema, connection)
            masked_question=mask_single_question_with_schema_linking(linking_info, '<mask>', '<unk>')
            question['masked_question']=masked_question
            question['linking_info']={key: value for key, value in linking_info.items() if key in ['sc_link', 'cv_link','question']}
            matched_columns_idx, matched_tables_idx = linking_info['matched_columns'], linking_info['matched_tables']

            orig_table_names=tables[database]
            orig_column_names=columns[database]

            #getting original names of matched column and table names
            matched_tables=[orig_table_names[idx] for idx in matched_tables_idx]
            matched_columns=[orig_column_names[idx] for idx in matched_columns_idx]
            question['matched_tables']=matched_tables
            question['matched_columns']=matched_columns

            cache[question['question_id']]=question

            #saving cache, comment the line below for faster processing saving cache increases time exponentially
            write_json_file(INTERMEDIARY_FILE, cache)

            bar()
    

    return preprocessed_data

def split_questions(questions, random, test_size):
    if random:
        sample_set, test_set = train_test_split(questions, test_size=test_size, random_state=42)
    else:
        # Sort the entries by a criterion (e.g., "question_id" or any other attribute you want)
        sorted_entries = sorted(questions, key=lambda x: x["question_id"])  # Or any other key you choose
        
        # Select the top 30% for the test set
        test_size = int(len(sorted_entries) * 0.3)
        test_set = sorted_entries[:test_size]
        
        # The remaining 70% goes into the sample set
        sample_set = sorted_entries[test_size:]
    
    return sample_set, test_set

def split_database_data(data, random_split, test_size):
    """ Reads a JSON file and splits the data into databases, samples, and tests. """

    dbs = {}
    for entry in data:
        db_id = entry['db_id']
        if db_id not in dbs:
            dbs[db_id] = []
        dbs[db_id].append(entry)

    # Creating indexes if dataset is train as train dataset is missing indexes
    if DATASET_TYPE == DatasetType.BIRD_TRAIN:
        for idx, item in enumerate(data):
            item['question_id']=idx

    # Split data and save it to files
    for db_id, entries in dbs.items():
        
        sample_set, test_set = split_questions(entries, random_split, test_size)
        
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
                "answer": entry["SQL"],
                'masked_question': entry['masked_question'],
                'linking_info': entry['linking_info'],
                'matched_tables': entry['matched_tables'],
                'matched_columns': entry['matched_columns']
            }
            for entry in sample_set
        ]

        # Create files and save data
        #TODO: samples are no longer unmasked need to change this variable name, will have to reflect across all scripts.
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
       - Set `DATASET_TYPE` to DatasetType.BIRD_TRAIN for training data.
       - Set `DATASET_TYPE` to DatasetType.BIRD_DEV for development data.

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
    file_path = TRAIN_FILE if DATASET_TYPE == DatasetType.BIRD_TRAIN else DEV_FILE if DATASET_TYPE == DatasetType.BIRD_DEV else None
    
    if not file_path:
        print("Choose a bird dataset for this script")
        exit(1)

    # Check if bird dataset is downloaded
    if not os.path.isdir(DATASET_DIR):
        url = "https://bird-bench.oss-cn-beijing.aliyuncs.com/train.zip" if DATASET_TYPE == DatasetType.BIRD_TRAIN else "https://bird-bench.oss-cn-beijing.aliyuncs.com/dev.zip"

        print("Dataset not found. Please download the dataset from the following URL:", url)
        print("After downloading, ensure to extract the contents of the ZIP file.")
        print("Copy and paste the unzipped bird data into the 'server/data/bird' directory.")
        print("Open the folder and make sure to unzip the train_databases/dev_databases directly into this folder.")
        print("The folder structure should be as follows:")
        print(" - 'train' folder containing 'train.json' or 'dev' folder containing 'dev.json'")
        print(" - Ensure that 'train_databases/dev_databases' are located directly inside the 'train/dev_20240627' directory.")
    else:

        # With Evidence
        with_evidence =  False

        # Random
        random_split = True

        # Test Size
        test_size = 0.5

        all_processed_questions=mask_all_questions(DEV_FILE, with_evidence)
        split_database_data(all_processed_questions, random_split, test_size)
        sample_set, test_set = split_questions(all_processed_questions, random_split, test_size)

        # Saving all questions in a single file
        write_json_file(f'{DATASET_DIR}/processed_train.json', sample_set)
        write_json_file(f'{DATASET_DIR}/processed_test.json', test_set)

        #clearing cache
        write_json_file(INTERMEDIARY_FILE, {})