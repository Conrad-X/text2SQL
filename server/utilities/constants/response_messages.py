# ERROR MESSAGES

# Missing parameters Errors
ERROR_QUESTION_REQUIRED = "Question parameter is required"
ERROR_SQL_QUERY_REQUIRED = "Query parameter is required"

# Database related Errors
ERROR_DATABASE_QUERY_FAILURE = "Database query error: {error}"
ERROR_DATABASE_DELETE_FAILURE = "Failed to delete existing records: {error}"
ERROR_DATABASE_ROLLBACK_FAILURE = "Error during database rollback: {error}"
ERROR_DATABASE_CLOSE_FAILURE = "Error closing the database session: {error}"
ERROR_DATABASE_NOT_FOUND = "Database '{database_name}' not found."

# LLM Client Errors
ERROR_INVALID_MODEL_FOR_TYPE = "Model {model} is not valid for {llm_type}."
ERROR_API_KEY_MISSING = "API key not found. Please set the {api_key} environment variable."
ERROR_API_FAILURE = "{llm_type} API error: {error}"
ERROR_UNSUPPORTED_CLIENT_TYPE = "Unsupported client type."
ERROR_BATCH_JOB_CREATION = "Error creating batch job: {error}."
ERROR_UPLOAD_BATCH_INPUT_FILE = "Error uploading batch input file: {error}."
ERROR_GET_ALL_BATCHES = "Error getting all batches: {error}."
ERROR_GET_ALL_UPLOADED_FILES = "Error getting all uploaded files: {error}."
ERROR_DOWNLOAD_BATCH_FILE = "Error downloading batch output file: {error}."
ERROR_BATCH_INPUT_FILE_NOT_FOUND = "File '{file_name}_batch_job_input.jsonl' not found. Please create one."

# Batch Job related Errors
ERROR_BATCH_INPUT_FILE_CREATION = "Error creating batch input file '{file_path}'. Details: {error}"
ERROR_BATCH_INPUT_FILE_NOT_UPLOADED = "Batch input file not found for database type: {database_type}. Ensure the file is uploaded."
ERROR_BATCH_JOB_NOT_FOUND = "Batch job not found for input file ID: {input_file_id}."
ERROR_BATCH_JOB_STATUS_NOT_COMPLETED = "Batch job status is not completed. Current status: {status}"

# Prompt related Errors
ERROR_PROMPT_TYPE_NOT_FOUND = "Prompt type '{prompt_type}' not found."
ERROR_NO_EXAMPLES_PROVIDED = "Examples must be provided for `{prompt_type}`"
ERROR_SHOTS_REQUIRED = "Number of shots must be provided for the selected prompt type."
ERROR_ZERO_SHOTS_REQUIRED = "Number of shots must be 0 for the selected prompt type."
ERROR_NON_NEGATIVE_SHOTS_REQUIRED = "Shots must be a non-negative integer." 
ERROR_SCHEMA_FILE_NOT_FOUND = "Schema file not found: {error}"
ERROR_FETCHING_EXAMPLES = "Error fetching examples: {error}"
ERROR_SCHEMA_FORMAT_REQUIRED = "Schema format is required for {prompt_type}"

# Utility Functions Errors
ERROR_SQL_MASKING_FAILED = "Error in masking SQL query: {error}"
ERROR_FILE_MASKING_FAILED = "Error in masking sample questions and queries file: {error}"
ERROR_UNSUPPORTED_FORMAT_TYPE = "Unsupported format type: {format_type}"
ERROR_FAILED_FETCH_TABLE_NAMES = "Failed to fetch table names: {error}"
ERROR_FAILED_FETCH_COLUMN_NAMES = "Failed to fetch column names: {error}"
ERROR_SQLITE_EXECUTION_ERROR = "Excuting SQL: {sql} failed with error: {error}"
ERROR_FAILED_FETCH_SCHEMA = "Failed to fetch schema: {error}"
ERROR_FAILED_FETCH_FOREIGN_KEYS = "Failed to fetch foreign keys for table {table_name}: {error}"
ERROR_FAILED_FECTHING_PRIMARY_KEYS = "Failed to fetch primary keys error: {error}"
ERROR_FAILED_FETCH_COLUMN_TYPES = "Failed to fetch column types for table {table_name}: {error}"
ERROR_FAILED_FETCH_COLUMN_VALUES = "Failed to fetch column values for column {column_name} in table {table_name} error: {error}"

# Cost Estimation related Errors
ERROR_INVALID_MODEL_FOR_TOKEN_ESTIMATION = "Model {model} is not a valid OpenAI model. Only OpenAI models are supported."
ERROR_TOKEN_ESTIMATION_NOT_IMPLEMENTED_LLMTYPE = "Token estimation for llm type {llm_type} is not implemented."
ERROR_TOKEN_ESTIMATION_NOT_IMPLEMENTED = "Token estimation for model {model} is not implemented."
ERROR_PROCESSING_ITEM = "Error processing item: {item}\n{error}"
ERROR_FILE_NOT_FOUND = "File {file_path} does not exist. Please check that the file path is correct."
ERROR_PROCESSING_COST_ESTIMATION = "Error processing {database_name} for model {model} (Batched: {is_batched}): {error}"
ERROR_PRICING_INFORMATION_NOT_FOUND = "Pricing information not found for {llm_type} and {model}"

# Value Retrieval related Errors
ERROR_INVALID_FETCH_ARGUMENT = "Invalid fetch argument. Must be 'all', 'one', 'random', or an integer."
ERROR_SQL_QUERY_TIMEOUT = "SQL query execution exceeded the timeout of {timeout} seconds."
ERROR_LSH_CREATION = "Error creating LSH: {error}"
ERROR_LSH_LOADING = "Error loading LSH for {database_name}: {error}"
ERROR_DATABASE_PROCESSING = "Error processing database {database_name}: {error}"

UNKNOWN_ERROR = "An unknown error occurred`{error}`"

# WARNING MESSAGES

# Cost Estimation related Warnings
WARNING_MODEL_MAY_UPDATE = "Warning: {model} may update over time. Hence assuming {resolved_model}"
WARNING_MODEL_NOT_FOUND_FOR_ENCODING = "Warning: model {model} not found for encoding. Using o200k_base encoding."
WARNING_FILE_NOT_FOUND = "Warning: File not found for {database_name}"

# LLM Client Warnings
WARNING_ALL_API_KEYS_QUOTA_EXCEEDED = "All {llm_type} API keys quota-exhausted. Sleeping for 5s"

# SUCCESS MESSAGES

# Batch Job related Success Messages
SUCCESS_BATCH_INPUT_FILE_EXISTS = "Batch input file already exists"
SUCCESS_BATCH_INPUT_FILE_CREATED = "Batch input file created successfully"
SUCCESS_BATCH_JOB_EXISTS = "Batch job already exists"
SUCCESS_BATCH_JOB_CREATED = "Batch job created with ID: {job_id}"
SUCCESS_BATCH_OUTPUT_FILE_DOWNLOADED = "Batch output file downloaded to: {output_file_path}"


