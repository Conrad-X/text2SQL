ERROR_INITIALIZING_DESCRIPTION_FILES = "Error initializing description files: {error}"
ERROR_UPDATING_DESCRIPTION_FILES = "Error updating column descriptions: {error}"
ERROR_GENERATING_TABLE_DESCRIPTIONS = "Error generating table {table_name} description: {error}"
ERROR_GENERATING_COLUMN_DESCRIPTIONS = "Error generating improved description for column '{column_name}': {error}"
ERROR_COLUMN_DOES_NOT_EXIST = "Column '{column_name}' does not exist. Please check {table_name}."
ERROR_TABLE_DOES_NOT_EXIST = "Table '{table_name}' does not exist. Please check the database: {file_path}"
ERROR_FAILED_TO_READ_CSV = "All encoding attempts failed for {file_path}"
ERROR_COLUMN_MEANING_FILE_NOT_FOUND = "Column meaning file not found: {file_path}"

WARNING_ENCODING_FAILED = "Failed with encoding {encoding}: {e}. Trying next..."

INFO_TABLE_ALREADY_HAS_DESCRIPTIONS = "Table {table_name} already has descriptions. Skipping LLM call."
INFO_COLUMN_ALREADY_HAS_DESCRIPTIONS = "Column {column_name} already has descriptions. Skipping LLM call."