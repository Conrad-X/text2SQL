from dotenv import load_dotenv
import os
import chromadb

from utilities.constants.response_messages import ERROR_API_KEY_MISSING
from utilities.constants.database_enums import DatabaseType, DATABASE_PATHS

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

if not OPENAI_API_KEY:
    raise RuntimeError(ERROR_API_KEY_MISSING.format(api_key="OPENAI_API_KEY"))
if not ANTHROPIC_API_KEY:
    raise RuntimeError(ERROR_API_KEY_MISSING.format(api_key="ANTHROPIC_API_KEY"))

# File and Folder Paths configurations
BIRD_TRAIN_DATASET_DIR = './data/bird/train/train_databases'

DATABASE_SQLITE_PATH = "./data/bird/train/train_databases/{database_name}/{database_name}.sqlite"
UNMASKED_SAMPLE_DATA_FILE_PATH = "./data/bird/train/train_databases/{database_name}/samples/unmasked_{database_name}.json"
MASKED_SAMPLE_DATA_FILE_PATH = "./data/bird/train/train_databases/{database_name}/samples/masked_{database_name}.json"
TEST_DATA_FILE_PATH = "./data/bird/train/train_databases/{database_name}/test_{database_name}.json"
BATCH_INPUT_FILE_PATH = "./data/bird/train/train_databases/{database_name}/batch_jobs/batch_job_input_{database_name}.jsonl"
BATCH_OUTPUT_FILE_PATH = "./data/bird/train/train_databases/{database_name}/batch_jobs/batch_job_output_{database_name}.jsonl"

class DatabaseConfig:
    ACTIVE_DATABASE = "address"
    DATABASE_URL = DATABASE_SQLITE_PATH.format(database_name=ACTIVE_DATABASE)

    @classmethod
    def set_database(cls, database_name):
        cls.ACTIVE_DATABASE = database_name
        cls.DATABASE_URL = DATABASE_SQLITE_PATH.format(database_name=database_name)

class ChromadbClient:
    CHROMADB_CLIENT=chromadb.Client()

    @classmethod
    def reset_chroma(cls):
        cls.CHROMADB_CLIENT=chromadb.Client()
        cls.CHROMADB_CLIENT.reset()
