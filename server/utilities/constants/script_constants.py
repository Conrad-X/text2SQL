from enum import Enum

MODEL='gpt-4o-2024-08-06'
MAX_TOKENS=1000
TEMPERATURE=0.7

GENERATE_BATCH_SCRIPT_PATH = "./data/bird/dev_20240627/dev_databases/"
GENERATE_BATCH_RELATIVE_PATH='./data/bird/dev_20240627/dev_databases/'
DB_CHANGE_ENPOINT="http://localhost:8000/database/change/"
PROMPT_GENERATE_ENDPOINT='http://localhost:8000/prompts/generate/'
PROMPT_TYPE='semantic_full_information'

NUM_SHOTS=5


SAMPLE_QUESTIONS_DIR='/samples/'
BATCH_DIR_SUFFIX="/batch_jobs/"

BATCHINPUT_FILE_PREFIX="batch_job_input"
BATCHOUTPUT_FILE_PREFIX="batch_job_output"
FORMATTED_PRED_FILE='formatted_predictions'
BATCH_JOB_METADATA_DIR='./batch_job_metadata/'

BIRD_EVAL_FOLDER="./bird_results/"

FINE_TUNE_NUMBER=350
FINE_TUNE_SYSTEM_MESSAGE="You are a Text to SQL bot. You are presented with a Natural Language Question, you have to return a SQL Script of the corresponding Natural Language Question."
FINE_TUNE_EXAMPLES_DIR="./fine_tune_examples/"
FINE_TUNE_EXAMPLES_FILE_PREFIX="ft_examples"

class BatchJobStatus(Enum):
    COMPLETED = "completed"
    INPROGRESS = "inprogress"


SCHEMA_PATH="./data/bird/dev_20240627/dev_tables.json"
PROCESSED_SAMPLE_DATA_FILE_PATH = "./data/bird/dev_20240627/dev_databases/{database_name}/samples/processed_{database_name}.json"

class APIStatusCode(Enum):
    SUCCESS = 200
    FAILURE = 404
    
class DatasetEvalStatus(Enum):
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"


UNKNOWN_COLUMN_DATA_TYPE_STR = "UNKNOWN"
GOOGLE_RESOURCE_EXHAUSTED_EXCEPTION_STR = "429"
