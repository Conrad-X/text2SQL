from utilities.config import PATH_CONFIG
import json
from utilities.sql_improvement import improve_sql_query
from utilities.logging_utils import setup_logger
import pandas as pd
from utilities.constants.LLM_enums import LLMType, ModelType
from services.client_factory import ClientFactory
from tqdm import tqdm
import os
import threading
import concurrent.futures
from utilities.utility_functions import execute_sql_timeout
from utilities.constants.prompts_enums import RefinerPromptType
import time
from collections import defaultdict
from app.db import set_database

logger = setup_logger(__name__)

def get_dict(database, file_path, columns):
    if os.path.exists(file_path):
        df = pd.read_csv(file_path, sep='\t', index_col=False)
        df_dict = df.set_index("database").to_dict(orient='index')
        if database not in list(df_dict.keys()):
            df_dict[database] = {str(i): 0 for i in columns}
    else:
        df_dict={database : {str(i): 0 for i in columns}}
    return df_dict

def save_df(data_dict, file_path):
    df = pd.DataFrame.from_dict(data_dict, orient='index')

    # Reset index and rename columns
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'database'}, inplace=True)

    # Save to CSV
    df.to_csv(file_path, sep='\t', index=False)
      

def process_question(item, client, refiner_dict, cache, cache_file, refiner_data_file, lock, prompt_type, shots, max_attempts, chat_mode):
    """Process a single question in a thread-safe manner."""
    question_id = item['question_id']
    sql = item['sql_gen']
    database = item['db_id']
    data_id = item['data_id']
    
    with lock:
        if database not in refiner_dict:
            refiner_dict[database] = {str(i): 0 for i in ['already correct', 'improver success', 'improver failed', 'improver degrade','non error improve', 'error improve']}

    # Read processed test file
    while True:
        try:
            with open(PATH_CONFIG.processed_test_path(database), 'r') as file:
                process_test = json.load(file)
                file.close()
            break
        except Exception as e:
            logger.error(f"Failed to read processed test file for {database}: {str(e)}")
            time.sleep(5)

    # Find matching question
    test_question = next((q for q in process_test if q["question_id"] == int(question_id)), None)
    if not test_question:
        logger.info(f"Skipping question_id: {question_id} not found in processed test")
        return

    error_before = False
    already_correct = False
    try:
        gold_res = execute_sql_timeout(database= database, sql_query=test_question['SQL'])
        try:
            res = execute_sql_timeout(database=database, sql_query=sql)
            if set(gold_res) == set(res):
                with lock:
                    refiner_dict[database]['already correct'] += 1
                already_correct = True
        except:
            error_before = True
    except Exception as e:
        logger.error(f"Failed to execute SQL query for {database}: {e}")
        
    # Improve SQL query
    sql = improve_sql_query(
        sql=sql,
        max_improve_sql_attempts=max_attempts,
        database_name=database,
        client=client,
        target_question=test_question['question'],
        shots=shots,
        refiner_prompt_type=prompt_type,
        schema_used=test_question['schema_used'],
        evidence=test_question['evidence'],
        chat_mode=chat_mode
    )

    try:
        res = execute_sql_timeout(database=database, sql_query=sql)
    except:
        res = []
    
    with lock:
        if not already_correct and (set(res) == set(gold_res)):
            refiner_dict[database]['improver success'] += 1
            if error_before:
                refiner_dict[database]['error improve'] += 1
            else:
                refiner_dict[database]['non error improve'] += 1
        else:
            if not (set(res) == set(gold_res)):
                if already_correct:
                    refiner_dict[database]['improver degrade'] += 1
                else :
                    refiner_dict[database]['improver failed'] += 1

    # Update cache and save results
    with lock:
        cache.append(data_id)
        save_df(refiner_dict, refiner_data_file)
        with open(cache_file, "a") as file:  # Append to avoid rewriting every time
            file.write(f"{data_id}\n")
            file.close()


if __name__ == '__main__':

    """
    Generates a 'refiner_bench_result.csv' file in the dataset directory.
    This file should be cleared before running a new benchmark to ensure that only the latest results are stored.
    Run this file from the server directory as so:

    python -m internal_benchmarks.refiner.refiner_bench
    
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))

    max_attempts = 5
    client = [LLMType.OPENAI, ModelType.OPENAI_GPT4_O_MINI]
    temperature =0.7
    max_tokens = 1024
    shots = 5
    prompt_type = RefinerPromptType.XIYAN
    chat_mode = True

    refiner_client = ClientFactory.get_client(
        type=client[0],
        model=client[1],
        temperature=temperature,
        max_tokens=max_tokens,
    )
    dataset_dir = PATH_CONFIG.dataset_dir()
    databases = [d for d in os.listdir(dataset_dir) if os.path.isdir(os.path.join(dataset_dir, d))]

    # Load refiner data
    with open(os.path.join(script_dir, 'refiner_bench_data.json'), 'r') as file:
        refiner_data = json.load(file)
        file.close()
    


    # Load cache
    cache_file = os.path.join(script_dir, 'refiner_bench_cache.txt')
    if os.path.exists(cache_file):
        with open(cache_file, 'r') as file:
            cache = [int(line.strip()) for line in file]
    else:
        cache = []

    # Load existing refiner results
    result_file = os.path.join(script_dir, 'refiner_bench_result.csv')
    if os.path.exists(result_file):
        df = pd.read_csv(result_file, sep='\t', index_col=False)
        refiner_dict = df.set_index("database").to_dict(orient='index')
    else:
        refiner_dict = {}

    # Thread-safe lock for shared data
    lock = threading.Lock()

    db_groups = defaultdict(list)
    for item in refiner_data:
        db_groups[item["db_id"]].append(item)

    # ThreadPoolExecutor for parallel execution
    for database, items in db_groups.items():
        set_database(database)
        futures = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    
            for item in items:
                if int(item['data_id']) in cache:
                    logger.info(f"Skipping {item['data_id']} as it is already in cache")
                    continue
                futures.append(
                    executor.submit(
                    process_question,
                    item,
                    refiner_client,
                    refiner_dict,
                    cache,
                    cache_file,
                    result_file,
                    lock,
                    prompt_type,
                    shots, 
                    max_attempts,
                    chat_mode
                ))

            for _ in tqdm(
                concurrent.futures.as_completed(futures),
                total=len(futures),
                desc=f"Processing {database}",
            ):
                pass

    with open(cache_file, 'w') as file:
        file.write("")
        file.close()