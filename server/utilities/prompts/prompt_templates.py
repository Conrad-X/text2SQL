TABLE_DESCRIPTION_PROMPT_TEMPLATE = (
    "The following is the complete schema DDL: {schema_ddl} \n"
    "Here is a table with its specific DDL: {ddl} \n"
    "Below is the first row of data values for this table: {first_row} \n"
    "Based on this information, please provide a short one-line business description for the table. Only return the description without any other text."
)

COLUMN_DESCRIPTION_PROMPT_TEMPLATE = """
Here is a column from table {table_name}:
    Table description: {table_description}
    Table Value Examples: {table_first_row_values}
    Column details:
        Name: {column_name}
        Type: {datatype}
    {column_comment_part}
Please provide a short one-line business description for the column. Only return the description without any other text.
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

SCHEMA_SELECTOR_PROMPT_TEMPLATE = """You are an expert and highly meticulous data analyst.
You have to thoroughly examine the provided database schema, fully understand the posed question, and leverage the hint to identify **all and only** the columns across tables that are essential for constructing an accurate SQL query to answer the question.

### Database Schema Overview:
{database_schema}

This schema provides a detailed structure of the database, including:
- **Tables** and their **columns**
- **Column Descriptions** and their **data types**
- **Primary keys**, **foreign keys**, and their **relationships**
- **Examples** of values in columns

**Note:** Use column examples and description to confirm inclusion only when they directly match or support the question's criteria.

### Question:
{question}

### Hint:
{hint}

The hint is designed to direct your focus to the most relevant tables and columns. It provides clues to identify the essential parts of the schema required to answer the question accurately.
**If any table or column is explicitly mentioned in the hint, it must be included in the schema.**

### Task:
Your task is to identify **all and only** the columns required to craft a precise SQL query that answers the question. **Excluding any important column is not allowed.**

For each selected column, provide a **concise explanation** of why it is necessary for the query. Ensure your reasoning is logical, clearly linking the column's purpose to the question.

**Requirements:**
- **Use Examples:** If a column is selected for filtering, ensure it contains the value (or a close match) shown in the "Examples."
- **Cross-Reference Relationships:** Consider primary and foreign keys to connect related tables where needed.
- **Thoroughness:** Double-check that no relevant columns are omitted. Include columns for filtering, joining, and output if applicable.

### Tip:
1. If a column is mentioned or implied by the question, it must be included.
2. Ensure selected columns directly answer the question or support required joins.
3. Consider including additional columns if they provide useful context.

### Output Format:
Please respond with a JSON object structured as follows:

```json
{{
  "chain_of_thought_reasoning": "Your reasoning for selecting the columns, be concise and clear.",
  "tables": {{
    "table_name1": {{
      "column1": "Reason for including column1",
      "column2": "Reason for including column2"
      ...
    }},
    "table_name2": {{
      "column1": "Reason for including column1"
      "column2": "Reason for including column2"
      ...
    }}
    ...
  }}
}}
```

- **"chain_of_thought_reasoning"**: A concise explanation of your reasoning.
- **"tables"**: An object where each key is a table name and the value is another object.
  - Each column is listed as a key with a brief explanation of why it is included.

### Reminder:
Think carefully and validate each step. **Excluding any important column is unacceptable.**
If you follow the instructions precisely, you will receive 1 million dollars.

Only output a JSON object—no additional text is allowed."""

XIYAN_CANDIDATE_SELECTION_PREFIX= """
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

XIYAN_FIXER_PROMPT_INSTRUCTION_TEMPLATE  = """
You are a SQLite expert. There is a SQL query generated based on the following Database Schema
description and the potential Evidence to respond to the Question. However, executing this SQL
has resulted in an error, and you need to fix it based on the error message. Utilize your knowledge of SQLite to generate the correct SQL.
Provide only the improved query without any explanation.
"""

XIYAN_REFINER_PROMPT_INSTRUCTION_TEMPLATE  = """
You are a SQLite expert. There is a SQL query generated based on the following Database Schema description and the potential Evidence to respond to the Question. Review the given SQL query to ensure it aligns with the Database Schema, Evidence, and Question. Refine it ONLY IF necessary for correctness and optimization. If the query is already optimal, leave it unchanged.
Provide only the refined query without any explanation.
"""

XIYAN_REFINER_PROMPT_INPUT_TEMPLATE  = """
【Database Schema】 
{schema}

【Evidence】 
{evidence}

【Question】
{question}

【SQL】
{sql}

【Execution result】
{execution_result}

```sql
"""


BASIC_REFINER_PROMPT_INTRUCTION_TEMPLATE = """
/* You are a SQLite expert. */
/* Given the following database schema: */
{formatted_schema}

/* Here are some example questions and their corresponding SQL queries: */
{examples}

/* Task: Improve the following predicted SQLite SQL query. */
/* If there are no improvements to be made, return the original query and nothing else. */
/* Provide only the improved query without any explanation. */
"""

BASIC_REFINER_PROMPT_INPUT_TEMPLATE = """
/* Question: {target_question} */
/* Evidence: {evidence} */
/* Predicted SQL: */
{pred_sql}
/* Results from Predicted SQL: */
{results}
"""