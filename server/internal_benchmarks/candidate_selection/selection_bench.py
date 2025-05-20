import concurrent.futures
import json
import os
import threading

import pandas as pd
from services.clients.client_factory import ClientFactory
from tqdm import tqdm
from utilities.candidate_selection import xiyan_basic_llm_selector
from utilities.config import PATH_CONFIG
from utilities.constants.services.llm_enums import LLMConfig, LLMType, ModelType
from utilities.logging_utils import setup_logger
from utilities.utility_functions import execute_sql_query, execute_sql_timeout

logger = setup_logger(__name__)

def save_df(data_dict, file_path):
    df = pd.DataFrame.from_dict(data_dict, orient='index')

    # Reset index and rename columns
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'database'}, inplace=True)

    # Save to CSV
    df.to_csv(file_path, sep='\t', index=False)

def get_dict(file_path):
    if os.path.exists(file_path):
        df = pd.read_csv(file_path, sep='\t', index_col=False)
        df_dict = df.set_index("database").to_dict(orient='index')
    else:
        df_dict={}
    return df_dict


def process_item(item, correct_gen_dict, correct_sel_dict, config_sel_dict, cache, errors):

    gold_res = execute_sql_timeout(item['database'], item['gold'])
    candidates = [(sql, id) for id, sql in item['candidates'].items()]

    correct_gen = []
    cand_exec_results = {}

    correct_gen_dict[item['database']] = correct_gen_dict.get(item['database'], {idx: 0 for _, idx in candidates})
    correct_sel_dict[item['database']] = correct_sel_dict.get(item['database'], {'correct_generated': 0, 'correct_selected': 0})
    config_sel_dict[item['database']] = config_sel_dict.get(item['database'], {idx: 0 for _, idx in candidates})

    for sql, id in candidates:
        try:
            res = execute_sql_timeout(item['database'], sql)
            cand_exec_results[id]=res
            if set(res) == set(gold_res):
                correct_gen.append(sql)
                correct_gen_dict[item['database']][id] += 1
                
        except Exception as e:
            logger.error(f"Error in Candidate SQL {e}")

    if len(correct_gen) > 0:
        correct_sel_dict[item['database']]['correct_generated'] += 1

    sql, config_id = xiyan_basic_llm_selector(
        sqls_with_config=candidates,
        target_question=item['question'],
        client=selector_client,
        database=item['database'],
        pruned_schema=item['schema_used'],
        evidence=item['evidence']
    )

    config_sel_dict[item['database']][config_id]+=1
    error = None
    if sql in correct_gen:
        correct_sel_dict[item['database']]['correct_selected']+=1
    elif len(correct_gen) > 0:
        error = {
            "id": item['question_id'],
            'selected':sql,
            "correct": correct_gen,
            "candidates": [sql for sql, _ in candidates],
            "database": item['database']
        }
            
    cache.append(item['question_id'])
    return item['question_id'], error


if __name__ == "__main__":
    """
    This scripts uses the already generated candidates in cand_bench_data.json and benchmarks the selection process.
    This dataset contains 6 candidates made from the Dev Set. Hence Ensure that DATASET_TYPE set to bird_dev. 
    Run the script from server directory as follows:

    python -m internal_benchmarks.candidate_selection.selection_bench
    """

    selector_config = LLMConfig(
        llm_type=LLMType.GOOGLE_AI,
        model_type=ModelType.GOOGLEAI_GEMINI_2_0_FLASH_THINKING_EXP_0121,
        temperature=0.2,
        max_tokens=8192
    )

    selector_client = ClientFactory.get_client(selector_config)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    bench_file_path = os.path.join(script_dir, "cand_bench_data.json")
    with open(bench_file_path, "r") as file:
        bench_data = json.load(file)

    bench_res_dir = os.path.join(script_dir, "bench_results1")
    error_file_path = os.path.join(bench_res_dir,"bench_error.json")

    os.makedirs(bench_res_dir, exist_ok=True)

    cache_file_path = os.path.join(bench_res_dir,"bench_cache.txt")
    if os.path.exists(cache_file_path):
        with open(cache_file_path, "r") as file:
            cache = [int(line.strip()) for line in file]
    else:
        cache = []
    
    if os.path.exists(error_file_path):
        with open(error_file_path, "r") as file:
            error_data = json.load(file)
    else:
        error_data = []


    correct_gen_dict = get_dict(os.path.join(bench_res_dir, "correct_gen.csv"))
    correct_sel_dict = get_dict(os.path.join(bench_res_dir, "correct_sel.csv"))
    config_sel_dict = get_dict(os.path.join(bench_res_dir, "config_sel.csv"))

    futures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        for item in bench_data:
            if item['question_id'] in cache:
                logger.info(f"Skipping {item['question_id']}")
                continue
            futures.append(executor.submit(process_item, item, correct_gen_dict, correct_sel_dict, config_sel_dict, cache, error_data))

        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
            question_id, error = future.result()
            save_df(correct_sel_dict, os.path.join(bench_res_dir, "correct_sel.csv"))
            save_df(config_sel_dict, os.path.join(bench_res_dir, "config_sel.csv"))
            save_df(correct_gen_dict, os.path.join(bench_res_dir, "correct_gen.csv"))

            if error:
                error_data.append(error)
            with open(error_file_path, "w") as f:
                json.dump(error_data, f, indent=4)
                f.close()

            with open(cache_file_path, "a") as f:
                f.write(str(question_id) + "\n")
                f.close()