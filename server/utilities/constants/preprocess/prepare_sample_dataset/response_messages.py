"""Response message constants for dataset preparation operations.

This module defines standardized message strings used for logging and user feedback
during dataset preparation operations. Using constants ensures consistent messaging
throughout the application.
"""

INFO_SKIPPING_PROCESSED_ITEM = "Skipping already processed item: {question_id}"
INFO_TRAIN_DATA_PROGRESS_SAVED="Progress saved and connection closed."

ERROR_USER_KEYBOARD_INTERRUPTION = "Process interrupted by user."
ERROR_INVALID_TRAIN_FILE="Invalid train file"
ERROR_INVALID_TRAIN_FILE_CONTENTS="Invalid train file contents"
