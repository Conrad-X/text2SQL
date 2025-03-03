import sqlite3
import time

from utilities.constants.prompts_enums import FormatType, RefinerPromptType
from utilities.prompts.prompt_templates import (
    BASIC_REFINER_PROMPT_INPUT_TEMPLATE,
    BASIC_REFINER_PROMPT_INTRUCTION_TEMPLATE,
    XIYAN_REFINER_PROMPT_INPUT_TEMPLATE,
    XIYAN_REFINER_PROMPT_INSTRUCTION_TEMPLATE
)
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


def generate_refiner_prompt(
    pred_sql,
    results,
    target_question,
    shots,
    evidence,
    schema_used,
    refiner_prompt_type,
):

    if refiner_prompt_type == RefinerPromptType.BASIC:
        formatted_schema = format_schema(FormatType.CODE, PATH_CONFIG.database_name, schema_used)
        examples = fetch_few_shots(shots, target_question)
        examples_text = "\n".join(
            f"/* Question: {example['question']} */\n/* Evidence: {example['evidence']} */\n{example['answer']}\n"
            for example in examples
        )

        prompt = BASIC_REFINER_PROMPT_INTRUCTION_TEMPLATE.format(
            formatted_schema=formatted_schema,
            examples=examples_text,
        )
        prompt += BASIC_REFINER_PROMPT_INPUT_TEMPLATE.format(
            target_question=target_question,
            evidence=evidence,
            pred_sql=pred_sql,
            results=results,
        )

        return prompt
    

    elif refiner_prompt_type == RefinerPromptType.XIYAN:
        formatted_schema = format_schema(
            FormatType.M_SCHEMA, PATH_CONFIG.database_name, schema_used
        )

        prompt = XIYAN_REFINER_PROMPT_INSTRUCTION_TEMPLATE
        prompt += XIYAN_REFINER_PROMPT_INPUT_TEMPLATE.format(
            schema=formatted_schema,
            evidence=evidence,
            question=target_question,
            sql=pred_sql,
            execution_result=results,
        )

        return prompt

    
def generate_refiner_chat(
    pred_sql,
    results,
    target_question,
    shots,
    evidence,
    schema_used,
    refiner_prompt_type,
    chat, 
):
    if refiner_prompt_type == RefinerPromptType.BASIC:
        formatted_schema = format_schema(FormatType.CODE, PATH_CONFIG.database_name, schema_used)

        examples = fetch_few_shots(shots, target_question)
        examples_text = "\n".join(
            f"/* Question: {example['question']} */\n/* Evidence: {example['evidence']} */\n{example['answer']}\n"
            for example in examples
        )

        if len(chat) == 0:
            chat.append(['system', BASIC_REFINER_PROMPT_INTRUCTION_TEMPLATE.format(
                formatted_schema=formatted_schema,
                examples=examples_text,
            )])

        chat.append(['user', BASIC_REFINER_PROMPT_INPUT_TEMPLATE.format(
            target_question=target_question,
            evidence=evidence,
            pred_sql=pred_sql,
            results=results,
        )])

        return chat
    

    elif refiner_prompt_type == RefinerPromptType.XIYAN:
        formatted_schema = format_schema(
            FormatType.M_SCHEMA, PATH_CONFIG.database_name, schema_used
        )

        if len(chat) == 0:
            chat.append(['system', XIYAN_REFINER_PROMPT_INSTRUCTION_TEMPLATE])

        chat.append(['user', XIYAN_REFINER_PROMPT_INPUT_TEMPLATE.format(
            schema=formatted_schema,
            evidence=evidence,
            question=target_question,
            sql=pred_sql,
            execution_result=results,
        )])

        return chat


def improve_sql_query(
    sql,
    max_improve_sql_attempts,
    database_name,
    client,
    target_question,
    shots,
    schema_used,
    evidence,
    refiner_prompt_type=RefinerPromptType.BASIC,
    chat_mode=False
):
    """Attempts to improve the given SQL query by executing it and refining it using the improvement prompt."""

    chat = []

    connection = sqlite3.connect(PATH_CONFIG.sqlite_path(database_name=database_name))
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
            if chat_mode:
                chat = generate_refiner_chat(
                    sql, res, target_question, shots, evidence, schema_used, refiner_prompt_type, chat
                )
                improved_sql = client.execute_chat(chat=chat)
                improved_sql = format_sql_response(improved_sql)

                chat.append(['model', improved_sql])
            else:
                prompt = generate_refiner_prompt(
                    sql, res, target_question, shots, evidence, schema_used, refiner_prompt_type
                )
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
