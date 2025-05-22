"""
This module contains templates for various schema formats used in the application.

It includes templates for semantic schemas, M-schemas, basic schemas, text schemas,
code representation schemas, and OpenAI schemas.
"""

# Template for Semantic Schema
SEMANTIC_COLUMN_ENTRY = "{column_name}: {column_type}, Description: {column_description}, Example: {example_value}"
SEMANTIC_SCHEMA_TABLE_KEY = "Table"
SEMANTIC_SCHEMA_COLUMNS_KEY = "Columns"
SEMANTIC_SCHEMA_DESCRIPTION_KEY = "Description"

# Templates for M-Schema
M_SCHEMA_COLUMN_ENTRY = "({column_name}:{column_type}, {column_description}, {primary_key}Examples: {examples_list})"
M_SCHEMA_PRIMARY_KEY_FLAG = "Primary Key, "
M_SCHEMA_FOREIGN_KEY_ENTRY = "{from_table}.{from_column}={to_table}.{to_column}"
M_SCHEMA_TABLE_ENTRY = """# Table: {table_name}, {table_description}

[
{columns}
]"""
M_SCHEMA_DB_LINE = "【DB_ID】 {database_name}"
M_SCHEMA_SCHEMA_LINE = "【Schema】"
M_SCHEMA_FOREIGN_KEY_LINE = "【Foreign keys】"

# Templates for Basic Schema
BASIC_SCHEMA_LINE_ENTRY = "Table {table}, columns = [ {columns} ]"

# Templates for Text Schema
TEXT_SCHEMA_LINE_ENTRY = "{table}: {columns}"

# Templates for Code Representation Schema
CODE_REPR_SCHEMA_MISSING_SQL_ENTRY = "-- Missing SQL for {table}"

# Templates for OpenAI Schema
OPENAI_SCHEMA_LINE_ENTRY = "# {table} ( {columns} )"