ERROR_INITIALIZING_DESCRIPTION_FILES = "Error initializing description files: {error}"
ERROR_UPDATING_DESCRIPTION_FILES = "Error updating column descriptions: {error}"
ERROR_GENERATING_TABLE_DESCRIPTIONS = "Error generating table {table_name} description: {error}"
ERROR_GENERATING_COLUMN_DESCRIPTIONS = "Error generating improved description for column '{column_name}': {error}"
ERROR_COLUMN_DOES_NOT_EXIST = "Column '{column_name}' does not exist. Please check {table_name}."
ERROR_TABLE_DOES_NOT_EXIST = "Table '{table_name}' does not exist. Please check the database: {file_path}"
ERROR_COLUMN_MEANING_FILE_NOT_FOUND = "Column meaning file not found: {file_path}"
ERROR_ENSURING_DESCRIPTION_FILES_EXIST = "Error in Ensuring Description Files Exist for {database_name}: {error}"
ERROR_SQLITE_EXECUTION_ERROR = "Excuting SQL: {sql} failed with error: {error}"

INFO_TABLE_ALREADY_HAS_DESCRIPTIONS = "Table {table_name} already has descriptions. Skipping LLM call."
INFO_COLUMN_ALREADY_HAS_DESCRIPTIONS = "Column {column_name} already has descriptions. Skipping LLM call."
