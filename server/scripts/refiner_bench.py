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
      

# Assuming necessary imports for PATH_CONFIG, ClientFactory, improve_sql_query_chat, save_df, etc.

logger = setup_logger(__name__)

def process_question(item, client, refiner_dict, cache, cache_file, refiner_data_file, lock, prompt_type, shots, max_attempts ):
    """Process a single question in a thread-safe manner."""
    question_id = item['question_id']
    sql = item['sql_gen']
    database = item['db_id']
    data_id = item['data_id']

    # Skip if already processed
    with lock:
        if int(data_id) in cache:
            logger.info(f"Skipping data_id: {data_id} already processed")
            return
    
    with lock:
        if database not in refiner_dict:
            refiner_dict[database] = {str(i): 0 for i in ['already correct', 'improver success', 'improver failed', 'improver degrade']}

    # Read processed test file
    try:
        with open(PATH_CONFIG.processed_test_path(database), 'r') as file:
            process_test = json.load(file)
    except Exception as e:
        logger.error(f"Failed to read processed test file for {database}: {e}")
        return

    # Find matching question
    test_question = next((q for q in process_test if q["question_id"] == int(question_id)), None)
    if not test_question:
        logger.info(f"Skipping question_id: {question_id} not found in processed test")
        return

    already_correct = False
    try:
        gold_res = execute_sql_timeout(database= database, sql_query=test_question['SQL'])
        res = execute_sql_timeout(database=database, sql_query=sql)

        if set(gold_res) == set(res):
            with lock:
                refiner_dict[database]['already correct'] += 1
            already_correct = True
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
    )

    try:
        res = execute_sql_timeout(database=database, sql_query=sql)
    except:
        res = []

    with lock:
        if not already_correct and (set(res) == set(gold_res)):
            refiner_dict[database]['improver success'] += 1
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


if __name__ == '__main__':

    """
    Generates a 'refiner_bench_result.csv' file in the dataset directory.
    This file should be cleared before running a new benchmark to ensure that only the latest results are stored.
    """

    max_attempts = 5
    client = [LLMType.GOOGLE_AI, ModelType.GOOGLEAI_GEMINI_2_0_FLASH]
    temperature =0.7
    max_tokens = 1024
    shots = 5
    prompt_type = RefinerPromptType.XIYAN


    dataset_dir = PATH_CONFIG.dataset_dir()
    databases = [d for d in os.listdir(dataset_dir) if os.path.isdir(os.path.join(dataset_dir, d))]

    # Load refiner data
    with open(PATH_CONFIG.base_dir() / 'refiner_bench_data.json', 'r') as file:
        refiner_data = json.load(file)

    # Load cache
    cache_file = PATH_CONFIG.base_dir() / 'refiner_bench_cache.txt'
    if os.path.exists(cache_file):
        with open(cache_file, 'r') as file:
            cache = [int(line.strip()) for line in file]
    else:
        cache = []

    # Load existing refiner results
    result_file = PATH_CONFIG.base_dir() / 'refiner_bench_result.csv'
    if os.path.exists(result_file):
        df = pd.read_csv(result_file, sep='\t', index_col=False)
        refiner_dict = df.set_index("database").to_dict(orient='index')
    else:
        refiner_dict = {}

    # Thread-safe lock for shared data
    lock = threading.Lock()

    # ThreadPoolExecutor for parallel execution
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(process_question, item, ClientFactory.get_client(type=client[0], model=client[1], temperature=temperature, max_tokens=max_tokens),
                            refiner_dict, cache, cache_file, result_file, lock, prompt_type, shots, max_attempts)
            for item in refiner_data
        ]

        for _ in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Processing Questions"):
            pass  # Ensures progress bar updates correctly

    with open(cache_file, 'w') as file:
        file.write("")
        file.close()

            






