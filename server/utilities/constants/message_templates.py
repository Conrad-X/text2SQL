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

UNKNOWN_ERROR = "An unknown error occurred"
