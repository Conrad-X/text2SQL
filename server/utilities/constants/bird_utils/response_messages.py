"""
This module contains constant response messages used throughout the application.

These messages are used for error handling and logging purposes.
"""

ERROR_FILE_NOT_FOUND = "The file {file_path} was not found."
ERROR_FILE_DECODE = "Failed to decode file {file_path}: {error}"
ERROR_FILE_READ = "An unexpected error occurred while reading {file_path}: {error}"
ERROR_FILE_SAVE = "An unexpected error occurred while saving the file {file_path}: {error}"
ERROR_JSON_DECODE = "Failed to decode JSON from {file_path}."
ERROR_MISSING_DB_ID = "Missing 'db_id' in item at index {index}."
ERROR_PATH_NOT_EXIST = "Provided path does not exist: {dataset_directory}."
ERROR_PATH_NOT_DIRECTORY = "Provided path is not a directory: {dataset_directory}."
ERROR_EMPTY_BIRD_ITEMS_LIST = "The list of bird items is empty."

WARNING_DATABASE_DESCRIPTION_FILE_NOT_FOUND = "Database description file not found: {file_path}"
WARNING_TABLE_DESCRIPTION_FILE_NOT_FOUND = "Table description file not found: {file_path}"