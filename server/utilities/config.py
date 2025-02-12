from dotenv import load_dotenv
import os
import chromadb

from utilities.constants.response_messages import ERROR_API_KEY_MISSING
from utilities.constants.database_enums import DatabaseType, DATABASE_PATHS, DatasetType

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
GOOGLE_AI_API_KEY = os.getenv("GOOGLE_AI_API_KEY")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
ALL_GOOGLE_KEYS = os.getenv('ALL_GOOGLE_API_KEYS')
ALL_GOOGLE_KEYS = ALL_GOOGLE_KEYS.split(" ")

if not OPENAI_API_KEY:
    raise RuntimeError(ERROR_API_KEY_MISSING.format(api_key="OPENAI_API_KEY"))
if not ANTHROPIC_API_KEY:
    raise RuntimeError(ERROR_API_KEY_MISSING.format(api_key="ANTHROPIC_API_KEY"))
if not GOOGLE_AI_API_KEY:
    raise RuntimeError(ERROR_API_KEY_MISSING.format(api_key="GOOGLE_AI_API_KEY"))
if not DEEPSEEK_API_KEY:
    raise RuntimeError(ERROR_API_KEY_MISSING.format(api_key="DEEPSEEK_API_KEY"))

# File and Folder Paths configurations
DATASET_TYPE = DatasetType.BIRD_DEV

if DATASET_TYPE == DatasetType.BIRD_TRAIN:
    DATASET_DIR = './data/bird/train/train_databases'

    PREPROCESSED_DIR_PATH = "./data/bird/train/train_databases/{database_name}/preprocessed"

    DATABASE_SQLITE_PATH = "./data/bird/train/train_databases/{database_name}/{database_name}.sqlite"
    UNMASKED_SAMPLE_DATA_FILE_PATH = "./data/bird/train/train_databases/{database_name}/samples/unmasked_{database_name}.json"
    MASKED_SAMPLE_DATA_FILE_PATH = "./data/bird/train/train_databases/{database_name}/samples/masked_{database_name}.json"
    TEST_DATA_FILE_PATH = "./data/bird/train/train_databases/{database_name}/test_{database_name}.json"
    TEST_GOLD_DATA_FILE_PATH = "./data/bird/train/train_databases/{database_name}/test_gold_{database_name}.sql"
    BATCH_INPUT_FILE_PATH = "./data/bird/train/train_databases/{database_name}/batch_jobs/batch_job_input_{database_name}.jsonl"
    BATCH_OUTPUT_FILE_PATH = "./data/bird/train/train_databases/{database_name}/batch_jobs/batch_job_output_{database_name}.jsonl"
    
    DATASET_DESCRIPTION_PATH = "./data/bird/train/train_databases/{database_name}/database_description"

    # Paths for the output pickle files.
    UNIQUE_VALUES_PATH = "./data/bird/train/train_databases/{database_name}/preprocessed/{database_name}_unique_values.pkl"
    LSH_PATH = "./data/bird/train/train_databases/{database_name}/preprocessed/{database_name}_lsh.pkl"
    MINHASHES_PATH = "./data/bird/train/train_databases/{database_name}/preprocessed/{database_name}_minhashes.pkl"


elif DATASET_TYPE == DatasetType.BIRD_DEV:
    DATASET_DIR = './data/bird/dev_20240627/dev_databases'

    PREPROCESSED_DIR_PATH = "./data/bird/dev_20240627/dev_databases/{database_name}/preprocessed"

    DATABASE_SQLITE_PATH = "./data/bird/dev_20240627/dev_databases/{database_name}/{database_name}.sqlite"
    UNMASKED_SAMPLE_DATA_FILE_PATH = "./data/bird/dev_20240627/dev_databases/{database_name}/samples/unmasked_{database_name}.json"
    MASKED_SAMPLE_DATA_FILE_PATH = "./data/bird/dev_20240627/dev_databases/{database_name}/samples/masked_{database_name}.json"
    TEST_DATA_FILE_PATH = "./data/bird/dev_20240627/dev_databases/{database_name}/test_{database_name}.json"
    TEST_GOLD_DATA_FILE_PATH = "./data/bird/dev_20240627/dev_databases/{database_name}/test_gold_{database_name}.sql"
    BATCH_INPUT_FILE_PATH = "./data/bird/dev_20240627/dev_databases/{database_name}/batch_jobs/batch_job_input_{database_name}.jsonl"
    BATCH_OUTPUT_FILE_PATH = "./data/bird/dev_20240627/dev_databases/{database_name}/batch_jobs/batch_job_output_{database_name}.jsonl"
    
    DATASET_DESCRIPTION_PATH = "./data/bird/dev_20240627/dev_databases/{database_name}/database_description"

    # Paths for the output pickle files.
    UNIQUE_VALUES_PATH = "./data/bird/dev_20240627/dev_databases/{database_name}/preprocessed/{database_name}_unique_values.pkl"
    LSH_PATH = "./data/bird/dev_20240627/dev_databases/{database_name}/preprocessed/{database_name}_lsh.pkl"
    MINHASHES_PATH = "./data/bird/dev_20240627/dev_databases/{database_name}/preprocessed/{database_name}_minhashes.pkl"

# TO DO: Update the synthetic dataset to follow the same folder structure as the bird dataset (with database name subdirectories) for better code readability
elif DATASET_TYPE == DatasetType.SYNTHETIC:
    DATASET_DIR = "./databases"

    DATABASE_SQLITE_PATH = "./databases/{database_name}.db"
    UNMASKED_SAMPLE_DATA_FILE_PATH = "./data/sample_questions_and_queries/{database_name}_schema.json"
    MASKED_SAMPLE_DATA_FILE_PATH = "./data/masked_sample_questions_and_queries/{database_name}_schema.json"
    TEST_DATA_FILE_PATH = UNMASKED_SAMPLE_DATA_FILE_PATH # We did not create testing data for synthetic data hence using unmasked sample data
    TEST_GOLD_DATA_FILE_PATH = None
    BATCH_INPUT_FILE_PATH = "./data/batch_jobs/batch_input_files/{database_name}_batch_job_input.jsonl"
    BATCH_OUTPUT_FILE_PATH = "./data/batch_jobs/batch_input_files/{database_name}_batch_job_output.jsonl"
    
    DATASET_DESCRIPTION_PATH = None # This only exist for Bird Datasets

    # Paths for the output pickle files. 
    # TO DO: Create these paths for synthetic data
    UNIQUE_VALUES_PATH = None
    LSH_PATH = None
    MINHASHES_PATH = None


class DatabaseConfig:
    ACTIVE_DATABASE = "debit_card_specializing"
    DATABASE_URL = DATABASE_SQLITE_PATH.format(database_name=ACTIVE_DATABASE)

    @classmethod
    def set_database(cls, database_name):
        cls.ACTIVE_DATABASE = database_name
        cls.DATABASE_URL = DATABASE_SQLITE_PATH.format(database_name=database_name)

class ChromadbClient:
    CHROMADB_CLIENT=chromadb.Client()