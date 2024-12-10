from enum import Enum

MODEL='ft:gpt-4o-mini-2024-07-18:conrad-labs:350-ex:AVeVYux8'
MAX_TOKENS=1000
TEMPERATURE=0.5

GENERATE_BATCH_SCRIPT_PATH = "./data/bird/train/train_databases/"
GENERATE_BATCH_RELATIVE_PATH='./data/bird/train/train_databases/'
DB_CHANGE_ENPOINT="http://localhost:8000/database/change/"
PROMPT_GENERATE_ENDPOINT='http://localhost:8000/prompts/generate/'
PROMPT_TYPE='dail_sql'
NUM_SHOTS=8

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

class APIStatusCode(Enum):
    SUCCESS = 200
    FAILURE = 404

class BatchFileStatus(Enum):
    UPLOADED = "uploaded"
    DOWNLOADED = "downloaded"
    PROCESSING_CANDIDATES = "processing_candidates"
    FORMATTED_PRED_FILE = "formatted_pred_file"
