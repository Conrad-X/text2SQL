from dotenv import load_dotenv
import os
import chromadb

from utilities.constants.response_messages import ERROR_API_KEY_MISSING
from utilities.constants.database_enums import DatabaseType, DATABASE_PATHS, DatasetType

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

if not OPENAI_API_KEY:
    raise RuntimeError(ERROR_API_KEY_MISSING.format(api_key="OPENAI_API_KEY"))
if not ANTHROPIC_API_KEY:
    raise RuntimeError(ERROR_API_KEY_MISSING.format(api_key="ANTHROPIC_API_KEY"))

# File and Folder Paths configurations
DATASET_TYPE = DatasetType.BIRD_DEV

if DATASET_TYPE == DatasetType.BIRD_TRAIN:
    DATASET_DIR = './data/bird/train/train_databases'

    DATABASE_SQLITE_PATH = "./data/bird/train/train_databases/{database_name}/{database_name}.sqlite"
    UNMASKED_SAMPLE_DATA_FILE_PATH = "./data/bird/train/train_databases/{database_name}/samples/unmasked_{database_name}.json"
    MASKED_SAMPLE_DATA_FILE_PATH = "./data/bird/train/train_databases/{database_name}/samples/masked_{database_name}.json"
    TEST_DATA_FILE_PATH = "./data/bird/train/train_databases/{database_name}/test_{database_name}.json"
    TEST_GOLD_DATA_FILE_PATH = "./data/bird/train/train_databases/{database_name}/test_gold_{database_name}.sql"
    BATCH_INPUT_FILE_PATH = "./data/bird/train/train_databases/{database_name}/batch_jobs/batch_job_input_{database_name}.jsonl"
    BATCH_OUTPUT_FILE_PATH = "./data/bird/train/train_databases/{database_name}/batch_jobs/batch_job_output_{database_name}.jsonl"

elif DATASET_TYPE == DatasetType.BIRD_DEV:
    DATASET_DIR = './data/bird/dev_20240627/dev_databases'

    DATABASE_SQLITE_PATH = "./data/bird/dev_20240627/dev_databases/{database_name}/{database_name}.sqlite"
    UNMASKED_SAMPLE_DATA_FILE_PATH = "./data/bird/dev_20240627/dev_databases/{database_name}/samples/unmasked_{database_name}.json"
    MASKED_SAMPLE_DATA_FILE_PATH = "./data/bird/dev_20240627/dev_databases/{database_name}/samples/masked_{database_name}.json"
    TEST_DATA_FILE_PATH = "./data/bird/dev_20240627/dev_databases/{database_name}/test_{database_name}.json"
    TEST_GOLD_DATA_FILE_PATH = "./data/bird/dev_20240627/dev_databases/{database_name}/test_gold_{database_name}.sql"
    BATCH_INPUT_FILE_PATH = "./data/bird/dev_20240627/dev_databases/{database_name}/batch_jobs/batch_job_input_{database_name}.jsonl"
    BATCH_OUTPUT_FILE_PATH = "./data/bird/dev_20240627/dev_databases/{database_name}/batch_jobs/batch_job_output_{database_name}.jsonl"

# TO DO: Update the synthetic dataset to follow the same folder structure as the bird dataset (with database name subdirectories) for better code readability
elif DATASET_TYPE == DatasetType.SYNTHETIC:
    DATASET_DIR = "./databases"

    DATABASE_SQLITE_PATH = "./databases/{database_name}.db"
    UNMASKED_SAMPLE_DATA_FILE_PATH = "./data/sample_questions_and_queries/{database_name}_schema.json"
    MASKED_SAMPLE_DATA_FILE_PATH = "./data/masked_sample_questions_and_queries/{database_name}_schema.json"
    TEST_DATA_FILE_PATH = UNMASKED_SAMPLE_DATA_FILE_PATH # We did not create testing data for synthetic data hence using unmasked sample data
    TEST_GOLD_DATA_FILE_PATH = None
    BATCH_INPUT_FILE_PATH = "./data//batch_jobs/batch_input_files/{database_name}_batch_job_input.jsonl"
    BATCH_OUTPUT_FILE_PATH = "./data//batch_jobs/batch_input_files/{database_name}_batch_job_output.jsonl"

class DatabaseConfig:
    ACTIVE_DATABASE = "hotel"
    DATABASE_URL = DATABASE_SQLITE_PATH.format(database_name=ACTIVE_DATABASE)

    @classmethod
    def set_database(cls, database_name):
        cls.ACTIVE_DATABASE = database_name
        cls.DATABASE_URL = DATABASE_SQLITE_PATH.format(database_name=database_name)

class ChromadbClient:
    CHROMADB_CLIENT=chromadb.Client()
    SAMPLE_QUESTIONS_PATH=f'./data/bird/train/train_databases/{DatabaseType.FORMULA1.value}/samples/unmasked_{DatabaseType.FORMULA1.value}.json'

    @classmethod
    def reset_chroma(cls,sample_question_path=None):
        cls.CHROMADB_CLIENT=chromadb.Client()
        cls.CHROMADB_CLIENT.reset()
        if sample_question_path==None:
            cls.SAMPLE_QUESTIONS_PATH=f'./data/bird/train/train_databases/{DatabaseType.FORMULA1.value}/samples/unmasked_{DatabaseType.FORMULA1.value}.json'
        else:
            cls.SAMPLE_QUESTIONS_PATH=sample_question_path


SAMPLE_QUESTIONS_AND_QUERIES_DIR = "./data/sample_questions_and_queries"
BATCH_OUTPUT_FILE_DIR = "./data/batch_jobs/batch_output_files"
BATCH_INPUT_FILE_DIR = "./data/batch_jobs/batch_input_files"

SAMPLE_QUESTIONS_AND_QUERIES_FILE_NAME  = "{database_name}_schema.json"
BATCH_INPUT_FILE_NAME = "{database_name}_batch_job_input.jsonl"
BATCH_OUTPUT_FILE_NAME = "{database_name}_batch_job_output.jsonl"


BIRD_TRAIN_DATASET_DIR = './data/bird/train/train_databases'

DATABASE_SQLITE_PATH = "./data/bird/train/train_databases/{database_name}/{database_name}.sqlite"
UNMASKED_SAMPLE_DATA_FILE_PATH = "./data/bird/train/train_databases/{database_name}/samples/unmasked_{database_name}.json"
MASKED_SAMPLE_DATA_FILE_PATH = "./data/bird/train/train_databases/{database_name}/samples/masked_{database_name}.json"
TEST_DATA_FILE_PATH = "./data/bird/train/train_databases/{database_name}/test_{database_name}.json"
BATCH_INPUT_FILE_PATH = "./data/bird/train/train_databases/{database_name}/batch_jobs/batch_job_input_{database_name}.jsonl"
BATCH_OUTPUT_FILE_PATH = "./data/bird/train/train_databases/{database_name}/batch_jobs/batch_job_output_{database_name}.jsonl"