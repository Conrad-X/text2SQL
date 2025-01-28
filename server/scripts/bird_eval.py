import sys
import json
import argparse
import sqlite3
import multiprocessing as mp
from func_timeout import func_timeout, FunctionTimedOut
import os
from utilities.constants.script_constants import (
    GENERATE_BATCH_SCRIPT_PATH,
    FORMATTED_PRED_FILE,
    BIRD_EVAL_FOLDER
)
from datetime import datetime
import pandas as pd


def load_json(dir):
    with open(dir, 'r') as j:
        contents = json.loads(j.read())
    return contents

def result_callback(result):
    exec_result.append(result)


def execute_sql(predicted_sql,ground_truth, db_path):
    conn = sqlite3.connect(db_path)
    # Connect to the database
    cursor = conn.cursor()
    cursor.execute(predicted_sql)
    predicted_res = cursor.fetchall()
    cursor.execute(ground_truth)
    ground_truth_res = cursor.fetchall()
    res = 0
    if set(predicted_res) == set(ground_truth_res):
        res = 1
    return res



def execute_model(predicted_sql,ground_truth, db_place, idx, meta_time_out):
    try:
        res = func_timeout(meta_time_out, execute_sql,
                                  args=(predicted_sql, ground_truth, db_place))
        error=None
    except KeyboardInterrupt:
        sys.exit(0)
    except FunctionTimedOut:
        result = [(f'timeout',)]
        res = 0
    except Exception as e:
        split_dir=db_place.split('/')
        db=split_dir[-1][:-7]
        result = [(f'error',)]  # possibly len(query) > 512 or not executable
        res = 0
        error=e

    result = {'sql_idx': idx, 'res': res, 'error':[predicted_sql, error]}

    return result


def package_sqls_pred(sql_path, db_root_path):
    clean_sqls = []
    db_path_list = []
    
    sql_data = json.load(open(sql_path, 'r'))
    for idx, sql_str in sql_data.items():
        if type(sql_str) == str:
            sql, db_name = sql_str.split('\t----- bird -----\t')
        else:
            sql, db_name = " ", "financial"
        clean_sqls.append(sql)
        db_path_list.append(db_root_path)

  

    return clean_sqls, db_path_list

def package_sqls_gold(sql_path, db_root_path):
    clean_sqls = []
    db_path_list = []
    
    sqls = open(sql_path)
    sql_txt = sqls.readlines()
    # sql_txt = [sql.split('\t')[0] for sql in sql_txt]
    for idx, sql_str in enumerate(sql_txt):
        sql, db_name = sql_str.strip().split('\t')
        clean_sqls.append(sql)
        db_path_list.append(db_root_path)

    return clean_sqls, db_path_list

def run_sqls_parallel(sqls, db_places, num_cpus=1, meta_time_out=30.0):
    pool = mp.Pool(processes=num_cpus)
    for i,sql_pair in enumerate(sqls):

        predicted_sql, ground_truth = sql_pair
        pool.apply_async(execute_model, args=(predicted_sql, ground_truth, db_places[i], i, meta_time_out), callback=result_callback)
    pool.close()
    pool.join()

def sort_results(list_of_dicts):
  return sorted(list_of_dicts, key=lambda x: x['sql_idx'])

def compute_acc_by_diff(exec_results,diff_json_path):
    num_queries = len(exec_results)
    results = [res['res'] for res in exec_results]
    all_acc = sum(results)/num_queries
    return all_acc * 100, num_queries-sum(results)



def print_data(score_lists,count_lists):
    levels = ['simple', 'moderate', 'challenging', 'total']
    print("{:20} {:20} {:20} {:20} {:20}".format("", *levels))
    print("{:20} {:<20} {:<20} {:<20} {:<20}".format('count', *count_lists))

    print('======================================    ACCURACY    =====================================')
    print("{:20} {:<20.2f} {:<20.2f} {:<20.2f} {:<20.2f}".format('accuracy', *score_lists))


if __name__ == '__main__':


    directories = [d for d in os.listdir(GENERATE_BATCH_SCRIPT_PATH) if os.path.isdir(os.path.join(GENERATE_BATCH_SCRIPT_PATH, d))]

    # allocate number of CPUs for concurrency
    num_cpus=16
    # max timeout value for a thread
    meta_time_out=30.0

    acc_string=[]
    acc_score=[]
    score_dict={"database":[],"accuracy":[],"num_queries":[], 'errors':[],'mismatch':[]}
    errors={} 
    db_error_count={}
    total_error_count=0
    total_mismatch=0

    for database in directories:
        predicted_sql_path=f"{GENERATE_BATCH_SCRIPT_PATH}{database}/{FORMATTED_PRED_FILE}_{database}.json"
        ground_truth_path=f'{GENERATE_BATCH_SCRIPT_PATH}{database}/gold_{database}.sql'
        db_path=f'{GENERATE_BATCH_SCRIPT_PATH}{database}/{database}.sqlite'
        diff_json_path=f'{GENERATE_BATCH_SCRIPT_PATH}{database}/test_{database}.json'

        if not os.path.exists(predicted_sql_path):
            print(f"Cant find predicted queries for {database}")
            continue

        exec_result = []

        pred_queries, db_paths = package_sqls_pred(predicted_sql_path, db_path)
    
        gt_queries, db_paths_gt = package_sqls_gold(ground_truth_path, db_path)


        print("Running for DB:",database)
        query_pairs = list(zip(pred_queries,gt_queries))
        try:
            run_sqls_parallel(query_pairs, db_places=db_paths, num_cpus=num_cpus, meta_time_out=meta_time_out)
            exec_result = sort_results(exec_result)
            
        
            acc,mismatched= compute_acc_by_diff(exec_result,diff_json_path)

            score_dict['database'].append(database)
            score_dict['accuracy'].append(acc)
            score_dict['num_queries'].append(len(query_pairs))
            

            acc_string.append(f"Total Accuracy for {database}: {acc}\n")
            acc_score.append(acc)

        except Exception as e: 
            print(f"Failure for {database} because of: {e}")
            acc_score.append(f"Failure on DB: {database}")
        db_error=[]
       
        for error in exec_result:
            if not error['error'][1]==None:
                db_error.append(f"Predicted: {error['error'][0]}, Error: {error['error'][1]}")
            
        errors[database]=db_error
        error_count=len(db_error)
        mismatched=mismatched-error_count
        db_error_count[database]=f'Error Count: {error_count}, Mismatched Count: {mismatched}'
        total_error_count+=error_count
        total_mismatch+=mismatched

        score_dict['errors'].append(error_count)
        score_dict['mismatch'].append(mismatched)

    timestamp=datetime.now()
    timestamp=timestamp.strftime("%Y-%m-%d_%H:%M:%S")


    
    score_df=pd.DataFrame(score_dict)
    os.makedirs(BIRD_EVAL_FOLDER, exist_ok=True)

    with open(f"{BIRD_EVAL_FOLDER}{timestamp}.txt",'w') as file:
        file.write("SCORE:\n\n")
        score_df.to_csv(file, sep='\t', index=False)
        file.write(f"\nAverage Accuracy: {score_df['accuracy'].mean()}\n")
        file.write(f"Total Errors: {total_error_count}, Total Mismatch: {total_mismatch}\n")
        file.write("\n\nErrors:\n\n")
        for i in errors:
            file.write(f"Database: {i}\n")
            file.write(f'{db_error_count[i]}\n')
            for j in errors[i]:
                file.write(f'{j}\n')
            file.write('\n\n')

        file.close()

