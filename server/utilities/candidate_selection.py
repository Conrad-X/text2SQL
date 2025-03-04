from utilities.config import PATH_CONFIG
from utilities.constants.prompts_enums import FormatType
from utilities.utility_functions import format_schema
import sqlite3
from utilities.logging_utils import setup_logger
from utilities.prompts.prompt_templates import (
    XIYAN_CANIDADATE_SELECTION_PREFIX,
    XIYAN_CANDIDATE_PROMPT,
)
from utilities.constants.script_constants import GOOGLE_RESOURCE_EXHAUSTED_EXCEPTION_STR
import time

logger = setup_logger(__name__)

def xiyan_basic_llm_selector(sqls_with_config,target_question, client, database, pruned_schema, evidence=None):

    # if sqls the same then return the first one
    sqls = [i[0] for i in sqls_with_config]
    if len(set(sqls)) == 1:
        return [sqls[0], sqls_with_config[0][1]]

    connection = sqlite3.connect(
        PATH_CONFIG.sqlite_path(database_name=database)
    )
    cursor = connection.cursor()

    schema = format_schema(format_type=FormatType.M_SCHEMA,database_name=database, matches=pruned_schema)
    
    prompt_prefix = XIYAN_CANIDADATE_SELECTION_PREFIX.format(candidate_num=len(sqls), schema = schema, evidence = evidence, question = target_question)

    candidate_dict = {}
    sql_dict = {}
    idx_dict = {}
    for idx, sql in enumerate(sqls):
        try:
            res = cursor.execute(sql)
            res = cursor.fetchall()
            if not isinstance(res, RuntimeError):
                res = res[:10]
        except Exception as e:
            res = str(e)
        candidate_id = chr(idx+65)
        candidate_dict[candidate_id]=XIYAN_CANDIDATE_PROMPT.format(candidate_id = candidate_id, sql = sql, execution_result = res)
        sql_dict[candidate_id] = sql
        idx_dict[candidate_id] = sqls_with_config[idx][1]

    cand_ids_suffix = ' or '.join([f"\"{i}\"" for i in list(candidate_dict.keys())])
    suffix = "\nPlease output the selected candidate as " + cand_ids_suffix+' and nothing else.'
    candidate_string = "\n\n********\n\n".join(list(candidate_dict.values()))

    prompt = prompt_prefix + candidate_string + suffix
    
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