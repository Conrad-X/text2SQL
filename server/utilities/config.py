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

class DatabaseConfig:
    ACTIVE_DATABASE = DatabaseType.FORMULA1
    DATABASE_URL = DATABASE_PATHS.get(ACTIVE_DATABASE)

    @classmethod
    def set_database(cls, database_type):
        cls.ACTIVE_DATABASE = database_type
        cls.DATABASE_URL = DATABASE_PATHS.get(database_type)

class ChromadbClient:
    CHROMADB_CLIENT=chromadb.Client()

    @classmethod
    def reset_chroma(cls):
        cls.CHROMADB_CLIENT=chromadb.Client()
        cls.CHROMADB_CLIENT.reset()


SAMPLE_QUESTIONS_AND_QUERIES_DIR = "./data/sample_questions_and_queries"
BATCH_OUTPUT_FILE_DIR = "./data/batch_jobs/batch_output_files"
BATCH_INPUT_FILE_DIR = "./data/batch_jobs/batch_input_files"

SAMPLE_QUESTIONS_AND_QUERIES_FILE_NAME  = "{database_name}_schema.json"
BATCH_INPUT_FILE_NAME = "{database_name}_batch_job_input.jsonl"
BATCH_OUTPUT_FILE_NAME = "{database_name}_batch_job_output.jsonl"