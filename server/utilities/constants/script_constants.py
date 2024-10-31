from enum import Enum

MODEL='gpt-4o-mini'
MAX_TOKENS=1000
TEMPERATURE=0.7

GENERATE_BATCH_SCRIPT_PATH = "./data/bird/"
GENERATE_BATCH_RELATIVE_PATH='./data/bird/'
DB_CHANGE_ENPOINT="http://localhost:8000/database/change/"
PROMPT_GENERATE_ENDPOINT='http://localhost:8000/prompts/generate/'
PROMPT_TYPE='dail_sql'
NUM_SHOTS=3

class BatchJobStatus(Enum):
    COMPLETED = "completed"
    INPROGRESS = "inprogress"
