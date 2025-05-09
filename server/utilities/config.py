import os

import chromadb
from dotenv import load_dotenv
from utilities.constants.database_enums import DatasetType
from utilities.constants.response_messages import ERROR_API_KEY_MISSING
from utilities.path_config import PathConfig

load_dotenv()

# Set Current Dataset Type and the Samples Dataset Type
PATH_CONFIG = PathConfig(
    dataset_type=DatasetType(os.getenv("DATASET_TYPE", "bird_dev")), 
    sample_dataset_type=DatasetType(os.getenv("SAMPLE_DATASET_TYPE", "bird_train"))
)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
GOOGLE_AI_API_KEY = os.getenv("GOOGLE_AI_API_KEY")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
DASHSCOPE_API_KEY = os.getenv("DASHSCOPE_API_KEY")
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
if not DASHSCOPE_API_KEY:
    raise RuntimeError(ERROR_API_KEY_MISSING.format(api_key="DASHSCOPE_API_KEY"))

class ChromadbClient:
    CHROMADB_CLIENT=chromadb.PersistentClient() # Default path is "./chroma"