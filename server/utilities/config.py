from dotenv import load_dotenv
import os

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
    ACTIVE_DATABASE = DatabaseType.HOTEL
    DATABASE_URL = DATABASE_PATHS.get(ACTIVE_DATABASE)

    @classmethod
    def set_database(cls, database_type):
        cls.ACTIVE_DATABASE = database_type
        cls.DATABASE_URL = DATABASE_PATHS.get(database_type)

ENVIRONMENT = os.getenv("ENVIRONMENT", "development") 
if ENVIRONMENT == "testing":
    MASKED_FOLDER_PATH = os.getenv("MASKED_FOLDER_PATH", "/test_data/masked_sample_questions_and_queries")
    SAMPLE_FOLDER_PATH = os.getenv("SAMPLE_FOLDER_PATH", "/test_data/sample_questions_and_queries")
else:
    MASKED_FOLDER_PATH = os.getenv("MASKED_FOLDER_PATH", "/data/masked_sample_questions_and_queries")
    SAMPLE_FOLDER_PATH = os.getenv("SAMPLE_FOLDER_PATH", "/data/sample_questions_and_queries")

