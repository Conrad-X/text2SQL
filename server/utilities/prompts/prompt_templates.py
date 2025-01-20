TABLE_DESCRIPTION_PROMPT = (
    "The following is the complete schema DDL: {schema_ddl} \n"
    "Here is a table with its specific DDL: {ddl} \n"
    "Below is the first row of data values for this table: {first_row} \n"
    "Based on this information, please provide a short one-line business description for the table. Only return the description without any other text."
)

COLUMN_DESCRIPTION_PROMPT = """
Here is a column from table {table_name}:
    Table description: {table_description}
    Table Value Examples: {first_row}
    Column details:
        Name: {column_name}
        Type: {datatype}
    {column_comment_part}
Please provide a short one-line business description for the column. Only return the description without any other text.
"""

IMPROVEMENT_PROMPT_TEMPLATE = """
/* You are a SQLite expert. */
/* Given the following database schema: */
{formatted_schema}

/* Here are some example questions and their corresponding SQL queries: */
{examples}

/* Task: Improve the following predicted SQLite SQL query. */
/* If there are no improvements to be made, return the original query and nothing else. */
/* Provide only the improved query without any explanation. */

/* Question: {target_question} */
/* Predicted SQL: */
{pred_sql}
/* Results from Predicted SQL: */
{results}
"""
