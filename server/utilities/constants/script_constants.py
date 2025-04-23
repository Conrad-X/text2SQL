from enum import Enum

FINE_TUNE_NUMBER = 350
FINE_TUNE_SYSTEM_MESSAGE = "You are a Text to SQL bot. You are presented with a Natural Language Question, you have to return a SQL Script of the corresponding Natural Language Question."
FINE_TUNE_EXAMPLES_DIR = "./fine_tune_examples/"
FINE_TUNE_EXAMPLES_FILE_PREFIX = "ft_examples"

class BatchJobStatus(Enum):
    COMPLETED = "completed"
    INPROGRESS = "inprogress"

class APIStatusCode(Enum):
    SUCCESS = 200
    FAILURE = 404


class BatchFileStatus(Enum):
    UPLOADED = "uploaded"
    DOWNLOADED = "downloaded"
    JUDGING_CANDIDATES = "judging_candidates"
    FORMATTED_PRED_FILE = "formatted_pred_file"


class DatasetEvalStatus(Enum):
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"


UNKNOWN_COLUMN_DATA_TYPE_STR = "UNKNOWN"
