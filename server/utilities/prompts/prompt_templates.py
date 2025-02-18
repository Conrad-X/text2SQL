TABLE_DESCRIPTION_PROMPT_TEMPLATE = (
    "The following is the complete schema DDL: {schema_ddl} \n"
    "Here is a table with its specific DDL: {ddl} \n"
    "Below is the first row of data values for this table: {first_row} \n"
    "Based on this information, please provide a short one-line business description for the table. Only return the description without any other text."
)

COLUMN_DESCRIPTION_PROMPT_TEMPLATE = """
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

EXTRACT_KEYWORD_PROMPT_TEMPLATE = """
Objective: Analyze the given question and hint to identify and extract keywords, keyphrases, and named entities. These elements are crucial for understanding the core components of the inquiry and the guidance provided. This process involves recognizing and isolating significant terms and phrases that could be instrumental in formulating searches or queries related to the posed question.

Instructions:

Read the Question Carefully: Understand the primary focus and specific details of the question. Look for any named entities (such as organizations, locations, etc.), technical terms, and other phrases that encapsulate important aspects of the inquiry.

Analyze the Hint: The hint is designed to direct attention toward certain elements relevant to answering the question. Extract any keywords, phrases, or named entities that could provide further clarity or direction in formulating an answer.

List Keyphrases and Entities: Combine your findings from both the question and the hint into a single Python list. This list should contain:

Keywords: Single words that capture essential aspects of the question or hint.
Keyphrases: Short phrases or named entities that represent specific concepts, locations, organizations, or other significant details.
Ensure to maintain the original phrasing or terminology used in the question and hint.

Example 1:
Question: "What is the annual revenue of Acme Corp in the United States for 2022?"
Hint: "Focus on financial reports and U.S. market performance for the fiscal year 2022."

["annual revenue", "Acme Corp", "United States", "2022", "financial reports", "U.S. market performance", "fiscal year"]

Example 2:
Question: "In the Winter and Summer Olympics of 1988, which game has the most number of competitors? Find the difference of the number of competitors between the two games."
Hint: "the most number of competitors refer to MAX(COUNT(person_id)); SUBTRACT(COUNT(person_id where games_name = '1988 Summer'), COUNT(person_id where games_name = '1988 Winter'));"

["Winter Olympics", "Summer Olympics", "1988", "1988 Summer", "Summer", "1988 Winter", "Winter", "number of competitors", "difference", "MAX(COUNT(person_id))", "games_name", "person_id"]

Example 3:
Question: "How many Men's 200 Metres Freestyle events did Ian James Thorpe compete in?"
Hint: "Men's 200 Metres Freestyle events refer to event_name = 'Swimming Men''s 200 metres Freestyle'; events compete in refers to event_id;"

["Swimming Men's 200 metres Freestyle", "Ian James Thorpe", "Ian", "James", "Thorpe", "compete in", "event_name", "event_id"]

Task:
Given the following question and hint, identify and list all relevant keywords, keyphrases, and named entities.

Question: {question}
Hint: {hint}

Please provide your findings as a Python list, capturing the essence of both the question and hint through the identified terms and phrases. 
Only output the Python list, no explanations needed. 
"""

SCHEMA_SELECTOR_PROMPT_TEMPLATE = """
You are an expert and very smart data analyst.
Your task is to examine the provided database schema, understand the posed question, and use the hint to pinpoint the specific columns within tables that are essential for crafting a SQL query to answer the question.

Database Schema Overview:
{database_schema}

This schema offers an in-depth description of the database's architecture, detailing tables, columns, primary keys, foreign keys, and any pertinent information regarding relationships or constraints. Special attention should be given to the examples listed beside each column, as they directly hint at which columns are relevant to our query.

For key phrases mentioned in the question, we have provided the most similar values within the columns denoted by "Examples" in front of the corresponding column names. This is a critical hint to identify the columns that will be used in the SQL query.

Question:
{question}

Hint:
{hint}

The hint aims to direct your focus towards the specific elements of the database schema that are crucial for answering the question effectively.

Task:
Based on the database schema, question, and hint provided, your task is to identify all and only the columns that are essential for crafting a SQL query to answer the question.
For each of the selected columns, explain why exactly it is necessary for answering the question. Your reasoning should be concise and clear, demonstrating a logical connection between the columns and the question asked.

Tip: If you are choosing a column for filtering a value within that column, make sure that column has the value as an example.


Please respond with a JSON object structured as follows:

```json
{{
  "chain_of_thought_reasoning": "Your reasoning for selecting the columns, be concise and clear.",
  "table_name1": ["column1", "column2", ...],
  "table_name2": ["column1", "column2", ...],
  ...
}}
```

Make sure your response includes the table names as keys, each associated with a list of column names that are necessary for writing a SQL query to answer the question.
For each aspect of the question, provide a clear and concise explanation of your reasoning behind selecting the columns.
Take a deep breath and think logically. If you do the task correctly, I will give you 1 million dollars.

Only output a json as your response."""

XIYAN_CANIDADATE_SELECTION_PREFIX= """
You are a SQLite expert. Regarding the Question, there are {candidate_num} candidate SQL along with their Execution result in the database (showing the first 10 rows).
You need to compare these candidates and analyze the differences among the various candidate SQL. Based on the provided Database Schema, Evidence, and Question, select the correct and reasonable result.
【Database Schema】
{schema}
【Evidence】
{evidence}
【Question】
{question}
========================
"""

XIYAN_CANDIDATE_PROMPT = """
Candidate {candidate_id}
【SQL】
{sql}
【Execution Result】
{execution_result}
"""

XIYAN_REFINER_PROMPT = """
You are a SQLite expert. There is a SQL query generated based on the following Database Schema
description and the potential Evidence to respond to the Question. However, executing this SQL
has resulted in an error, and you need to fix it based on the error message. Utilize your knowledge of SQLite to generate the correct SQL.

【Database Schema】 
{schema}

【Evidence】 
{evidence}

[Question]
{question}

【SQL】
{sql}

【Execution result】
{execution_result}

```sql
"""