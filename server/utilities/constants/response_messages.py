# ERROR MESSAGES

# Missing parameters Errors
ERROR_QUESTION_REQUIRED = "Question parameter is required"
ERROR_SQL_QUERY_REQUIRED = "Query parameter is required"

# Database-related Errors
ERROR_DATABASE_QUERY_FAILURE = "Database query error: {error}"
ERROR_DATABASE_DELETE_FAILURE = "Failed to delete existing records: {error}"
ERROR_DATABASE_ROLLBACK_FAILURE = "Error during database rollback: {error}"
ERROR_DATABASE_CLOSE_FAILURE = "Error closing the database session: {error}"

# LLM Client Errors
ERROR_INVALID_MODEL_FOR_TYPE = "Model {model} is not valid for {llm_type}."
ERROR_API_KEY_MISSING = "API key not found. Please set the {api_key} environment variable."
ERROR_API_FAILURE = "{llm_type} API error: {error}"
ERROR_UNSUPPORTED_CLIENT_TYPE = "Unsupported client type."

# Prompt-related Errors
ERROR_PROMPT_TYPE_NOT_FOUND = "Prompt type '{prompt_type}' not found."
ERROR_NO_EXAMPLES_PROVIDED = "Examples must be provided for `{prompt_type}`"
ERROR_SHOTS_REQUIRED = "Number of shots must be provided for the selected prompt type."
ERROR_NON_NEGATIVE_SHOTS_REQUIRED = "Shots must be a non-negative integer." 

# Utility Functions Errors
ERROR_SQL_MASKING_FAILED = "Error in masking SQL query: {error}"
ERROR_FILE_MASKING_FAILED = "Error in masking sample questions and queries file: {error}"
ERROR_UNSUPPORTED_FORMAT_TYPE = "Unsupported format type: {format_type}"
ERROR_FAILED_FETCH_TABLE_NAMES = "Failed to fetch table names: {error}"
ERROR_FAILED_FETCH_COLUMN_NAMES = "Failed to fetch column names: {error}"
ERROR_FAILED_FETCH_TABLE_AND_COLUMN_NAMES = "Failed to fetch table and column names: {error}"
ERROR_FAILED_FORMATING_SCHEMA = "Error in formating schema for given database: {error}"
ERROR_INVALID_DATABASE_PATH = "Invalid Database path"

UNKNOWN_ERROR = "An unknown error occurred`{error}`"

