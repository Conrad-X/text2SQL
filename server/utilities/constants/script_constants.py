from enum import Enum

MODEL='gpt-4o-mini'
MAX_TOKENS=1000
TEMPERATURE=0.7

GENERATE_BATCH_SCRIPT_PATH = "./data/bird/train/train_databases/"
GENERATE_BATCH_RELATIVE_PATH='./data/bird/train/train_databases/'
DB_CHANGE_ENPOINT="http://localhost:8000/database/change/"
PROMPT_GENERATE_ENDPOINT='http://localhost:8000/prompts/generate/'
PROMPT_TYPE='dail_sql'
NUM_SHOTS=3

SAMPLE_QUESTIONS_DIR='/samples/'
BATCH_DIR_SUFFIX="/batch_jobs/"

BATCHINPUT_FILE_PREFIX="batch_job_input"
BATCHOUTPUT_FILE_PREFIX="batch_job_output"


class BatchJobStatus(Enum):
    COMPLETED = "completed"
    INPROGRESS = "inprogress"

BATCH_JOB_METADATA_DIR='/batch_job_metadata/'