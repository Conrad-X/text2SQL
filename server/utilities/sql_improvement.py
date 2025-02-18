import sqlite3
import time

from utilities.constants.prompts_enums import FormatType
from utilities.prompts.prompt_templates import IMPROVEMENT_PROMPT_TEMPLATE
from utilities.config import PATH_CONFIG
from utilities.logging_utils import setup_logger
from utilities.utility_functions import (
    execute_sql_query,
    format_schema,
    format_sql_response,
)
from utilities.constants.script_constants import (
    GOOGLE_RESOURCE_EXHAUSTED_EXCEPTION_STR,
)
from utilities.vectorize import fetch_few_shots

logger = setup_logger(__name__)


def generate_improvement_prompt(pred_sql, results, target_question, shots):
    formatted_schema = format_schema(FormatType.CODE, PATH_CONFIG.sqlite_path())
    examples = fetch_few_shots(shots, target_question)

    examples_text = "\n".join(
        f"/* Question: {example['question']} */\n{example['answer']}\n"
        for example in examples
    )

    return IMPROVEMENT_PROMPT_TEMPLATE.format(
        formatted_schema=formatted_schema,
        examples=examples_text,
        target_question=target_question,
        pred_sql=pred_sql,
        results=results,
    )


def improve_sql_query(
    sql,
    max_improve_sql_attempts,
    database_name,
    client,
    target_question,
    shots,
):
    """Attempts to improve the given SQL query by executing it and refining it using the improvement prompt."""

    connection = sqlite3.connect(
        PATH_CONFIG.sqlite_path(database_name=database_name)
    )
    for idx in range(max_improve_sql_attempts):
        try:
            # Try executing the query
            try:
                res = execute_sql_query(connection, sql)
                if not isinstance(res, RuntimeError):
                    res = res[:5]
                    if idx > 0:
                        break  # Successfully executed the query

            except Exception as e:
                logger.error(f"Error executing SQL: {e}")
                res = str(e)

            # Generate and execute improvement prompt
            prompt = generate_improvement_prompt(sql, res, target_question, shots)
            improved_sql = client.execute_prompt(prompt=prompt)
            improved_sql = format_sql_response(improved_sql)

            # Update SQL for the next attempt
            sql = improved_sql if improved_sql else sql

        except Exception as e:
            if GOOGLE_RESOURCE_EXHAUSTED_EXCEPTION_STR in str(e):
                logger.warning("Quota exhausted. Retrying in 5 seconds...")
                time.sleep(5)
            else:
                logger.error(f"Unhandled exception: {e}")
                break

    return sql

def improve_sql_query_chat(
    sql,
    max_improve_sql_attempts,
    database_name,
    client,
    target_question,
    shots,
):
    """Attempts to improve the given SQL query by executing it and refining it using the improvement prompt."""

    connection = sqlite3.connect(
        PATH_CONFIG.sqlite_path(database_name=database_name)
    )
    chat = []
    idx=0
    last_executable = None
    while idx < max_improve_sql_attempts:
        try:
            # Try executing the query
            try:
                res = execute_sql_query(connection, sql)
                if not isinstance(res, RuntimeError):
                    res = res[:5]
                    if idx > 0:
                        pass
                        # break  # Successfully executed the query
                last_executable = sql
            except Exception as e:
                logger.error(f"Error executing SQL: {e}\nSQL: {sql}")
                res = str(e)   

            # Generate and execute improvement prompt
            prompt = generate_improvement_prompt(sql, res, target_question, shots)
            improved_sql = client.execute_chat(chat, prompt)
            chat.append(["user", prompt])
            chat.append(["model", improved_sql])
            improved_sql = format_sql_response(improved_sql)
            # Update SQL for the next attempt
            sql = improved_sql if improved_sql else sql
            idx+=1 
            if idx == max_improve_sql_attempts:
                if last_executable:
                    return last_executable

        except Exception as e:
            if GOOGLE_RESOURCE_EXHAUSTED_EXCEPTION_STR in str(e):
                logger.warning("Quota exhausted. Retrying in 5 seconds...")
                time.sleep(5)
            else:
                logger.error(f"Unhandled exception: {e}")
                break
    return sql
