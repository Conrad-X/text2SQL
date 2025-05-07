import json
import sqlite3
import os
import argparse
import re
from utilities.constants.database_enums import DatasetType
from utilities.config import PATH_CONFIG

# Helper function to get dataset paths
def get_dataset_paths(database_name: str, dataset_type: DatasetType):
    """
    Helper function: Generate paths for a given WIKISQL dataset type ('dev' or 'test').
    """
    return {
        'db_path': PATH_CONFIG.sqlite_path(database_name=database_name, dataset_type=dataset_type),
        'gold_file_path': os.path.join(os.path.dirname(PATH_CONFIG.wiki_file_path(dataset_type=dataset_type)), f"{database_name}.json"),
        'pred_file_path': PATH_CONFIG.formatted_predictions_path()
    }

# Normalize SQL (basic)
def normalize_sql(sql: str) -> str:
    sql = sql.strip().lower()
    sql = re.sub(r'\s+', ' ', sql)
    return sql

# Execute SQL and get result as list
def execute_sql(conn, sql):
    try:
        cursor = conn.execute(sql)
        results = cursor.fetchall()
        return [str(row[0]) for row in results]
    except Exception as e:
        raise RuntimeError(str(e))

# Helper: get next versioned result filename
def get_next_result_filename(dataset_type: DatasetType):
    version = 1
    # Create a separate folder for each dataset (dev/test)
    results_dir = os.path.join('./wiki_eval_result', dataset_type.name)
    os.makedirs(results_dir, exist_ok=True)
    
    while True:
        result_file = os.path.join(results_dir, f'eval_results_v{version}.json')
        if not os.path.exists(result_file):
            return result_file
        version += 1

# Evaluate predictions
def evaluate(dataset_type: DatasetType):
    # Set paths based on dataset type
    database_name = "dev" if dataset_type == DatasetType.WIKI_DEV else "test"
    
    # Get paths for the dataset
    paths = get_dataset_paths(database_name=database_name, dataset_type=dataset_type)
    GOLD_FILE = paths['gold_file_path']
    PRED_FILE = paths['pred_file_path']
    DB_PATH = paths['db_path']
    
    # Get the result folder for this dataset type
    RESULTS_DIR = os.path.join('./wiki_eval_result', dataset_type.name)
    os.makedirs(RESULTS_DIR, exist_ok=True)

    # Load gold data
    with open(GOLD_FILE, 'r') as f:
        gold_raw = json.load(f)
        gold_data = {str(entry["question_id"]): entry for entry in gold_raw}

    # Load predicted SQLs
    with open(PRED_FILE, 'r') as f:
        pred_data = json.load(f)

    results = []
    total = 0
    correct = 0
    errors = 0

    # Connect to the SQLite database
    conn = sqlite3.connect(DB_PATH)

    for qid, pred_entry in pred_data.items():
        if qid not in gold_data:
            continue

        total += 1
        gt = gold_data[qid]
        gold_sql = gt['SQL']
        gold_result = gt['execution_result']

        pred_sql = pred_entry.split('\t')[0].strip()
        normalized = normalize_sql(pred_sql)
        pred_result = []
        error = ""
        is_correct = False

        try:
            pred_result = execute_sql(conn, pred_sql)
            is_correct = (pred_result == gold_result)
            if is_correct:
                correct += 1
        except Exception as e:
            error = str(e)
            errors += 1

        results.append({
            'id': qid,
            'question': gt['question'],
            'gold_sql': gold_sql,
            'pred_sql': pred_sql,
            'normalized_sql': normalized,
            'gold_result': gold_result,
            'pred_result': pred_result,
            'error': error,
            'is_correct': is_correct
        })

    conn.close()

    # Calculate accuracy percentage
    accuracy_percent = (correct / total) * 100 if total > 0 else 0

    # Save results in the appropriate folder (dev/test)
    result_file = get_next_result_filename(dataset_type)
    with open(result_file, 'w') as f:
        json.dump({
            'total': total,
            'correct': correct,
            'accuracy': f"{accuracy_percent:.2f}%",  # Accuracy as percentage
            'errors': errors,
            'results': results
        }, f, indent=2)

    # Print results in the command line
    print(f"[✓] Evaluation complete for {dataset_type.name} dataset. Accuracy: {accuracy_percent:.2f}%")
    print(f"Results saved to: {result_file}")

def main():
    """Main function to process WikiSQL datasets based on command-line arguments."""
    parser = argparse.ArgumentParser(description='WikiSQL Evaluation')
    parser.add_argument('--dataset', choices=['dev', 'test', 'all'], default='all',
                       help='Which dataset to evaluate (dev, test, or all)')

    args = parser.parse_args()

    if args.dataset == 'dev' or args.dataset == 'all':
        evaluate(DatasetType.WIKI_DEV)

    if args.dataset == 'test' or args.dataset == 'all':
        evaluate(DatasetType.WIKI_TEST)

    print("\nEvaluation complete!")

if __name__ == "__main__":
    """
    WikiSQL Evaluation Script

    This script evaluates the predicted SQL queries against the gold data and computes accuracy.

    Usage (from server directory):
        python3 -m evaluate_wiki --dataset dev
        python3 -m evaluate_wiki --dataset test
        python3 -m evaluate_wiki --dataset all  (evaluates both dev and test)
    """
    main()
