from collections import defaultdict
import hashlib
from utilities.config import PATH_CONFIG
from utilities.constants.prompts_enums import FormatType
from utilities.utility_functions import format_schema
import sqlite3
from utilities.logging_utils import setup_logger
from utilities.prompts.prompt_templates import (
    XIYAN_CANDIDATE_SELECTION_PREFIX,
    XIYAN_CANDIDATE_PROMPT,
)
from utilities.constants.script_constants import GOOGLE_RESOURCE_EXHAUSTED_EXCEPTION_STR
import time
from utilities.utility_functions import normalize_execution_results
import random

logger = setup_logger(__name__)


def hash_result(result):
    """Generate a hash for SQL results."""
    return hashlib.md5(str(result).encode()).hexdigest()

def get_candidate_selector_prompt(selected_sqls_with_config, target_question, database, pruned_schema, evidence=None):
    """Generate the prompt for the candidate selector."""
    candidate_dict = {}
    sql_dict = {}
    idx_dict = {}

    schema = format_schema(format_type=FormatType.M_SCHEMA, database_name=database, matches=pruned_schema)

    # Rebuild candidate list for selection
    for idx, (sql, config_id, res) in enumerate(selected_sqls_with_config):
        candidate_id = chr(idx + 65)
        candidate_dict[candidate_id] = XIYAN_CANDIDATE_PROMPT.format(
            candidate_id=candidate_id, sql=sql, execution_result=res
        )
        sql_dict[candidate_id] = sql
        idx_dict[candidate_id] = config_id

    prompt_prefix = XIYAN_CANDIDATE_SELECTION_PREFIX.format(
        candidate_num=len(selected_sqls_with_config), schema=schema, evidence=evidence, question=target_question
    )

    cand_ids_suffix = ' or '.join([f"\"{i}\"" for i in list(candidate_dict.keys())])
    suffix = "\nPlease output the selected candidate as " + cand_ids_suffix + ' and nothing else.'
    candidate_string = "\n\n********\n\n".join(list(candidate_dict.values()))

    prompt = prompt_prefix + candidate_string + suffix
    return prompt, candidate_dict, sql_dict, idx_dict

def xiyan_basic_llm_selector(sqls_with_config,target_question, client, database, pruned_schema, evidence=None):
    """ Select SQL from a list of sqls using XiYan Selector"""

    connection = sqlite3.connect(
        PATH_CONFIG.sqlite_path(database_name=database)
    )
    cursor = connection.cursor()

    candidate_dict = {}
    sql_dict = {}
    idx_dict = {}

    # Group SQLs by their results using a hash
    result_groups = defaultdict(list)

    for idx, (sql, config_id) in enumerate(sqls_with_config):
        try:
            cursor.execute(sql)
            res = cursor.fetchall()
            result_hash = hash_result(res)

            # Only consider top 10 results 
            if res and len(res) > 0:
                res = random.sample(res, min(10, len(res)))
                res = normalize_execution_results(res, fetchall=True)
                columns = [desc[0] for desc in cursor.description]

                markdown_table = "| " + " | ".join(columns) + " |\n"
                markdown_table += "| " + " | ".join(["---"] * len(columns)) + " |\n"

                for row in res:
                    markdown_table += "| " + " | ".join(map(str, row)) + " |\n"
                res = markdown_table

        except Exception as e:
            res = str(e)
            result_hash = hash_result(res)

        if result_hash not in result_groups:
            result_groups[result_hash] = []
        result_groups[result_hash].append((sql, config_id, res)) 

    # Select one SQL from each unique result group
    selected_sqls_with_config = [group[0] for group in result_groups.values()]  

    # If only one SQL is left, return it
    if len(selected_sqls_with_config) == 1:
        return selected_sqls_with_config[0][0], selected_sqls_with_config[0][1]

    prompt, candidate_dict, sql_dict, idx_dict = get_candidate_selector_prompt(selected_sqls_with_config, target_question, database, pruned_schema, evidence)
    
    while True:
        try:
            resp = client.execute_prompt(prompt = prompt)

            #finding the first occurence of a letter in the response
            for i in range(len(resp) - 1,-1,-1):
                if resp[i] in list(candidate_dict.keys()):
                    return sql_dict[resp[i]], idx_dict[resp[i]] 
        except Exception as e:
            logger.error(f"Error in XiYan Candidate Selection: {e}")
            if GOOGLE_RESOURCE_EXHAUSTED_EXCEPTION_STR in str(e):
                logger.warning("Quota exhausted. Retrying in 5 seconds...")
                time.sleep(5)
            else:
                logger.error(f"Unhandled exception: {e}")