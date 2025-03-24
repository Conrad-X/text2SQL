import json
from utilities.config import PATH_CONFIG
from collections.abc import Mapping
from difflib import SequenceMatcher
import os
import pandas as pd

def similarity_score(val1, val2):
    """
    Computes a similarity score between two values:
    - Exact match: 1.0
    - Partial match for strings: Similarity ratio
    - Lists/Dictionaries: Recursive similarity
    """
    if val1 == val2:
        return 1.0  # Exact match
    elif isinstance(val1, str) and isinstance(val2, str):
        return SequenceMatcher(None, val1.lower(), val2.lower()).ratio()  # String similarity
    elif isinstance(val1, Mapping) and isinstance(val2, Mapping):
        return compare_dicts(val1, val2)  # Recursive call for dicts
    elif isinstance(val1, list) and isinstance(val2, list):
        return compare_lists(val1, val2)  # Recursive call for lists
    return 0  # No similarity

def compare_lists(list1, list2):
    """Compare lists by averaging similarity of elements."""
    if not list1 or not list2:
        return 0  # No similarity if one list is empty
    scores = [max(similarity_score(item, other) for other in list2) for item in list1]
    return sum(scores) / max(len(list1), len(list2))

def compare_dicts(gold, runtime):
    """Compute similarity score between two dictionaries."""

    gold={key.lower():value for key, value in gold.items()}
    runtime={key.lower():value for key, value in runtime.items()}
    
    gold_tables, runtime_tables = set(gold.keys()), set(runtime.keys())
    matching_keys = gold_tables & runtime_tables  # Common keys
    total_keys = gold_tables | runtime_tables  # All unique keys

    if not total_keys:
        return 1.0  # Both empty dicts are identical

    key_score = len(matching_keys) / len(total_keys)  # Key presence score

    common_tables = runtime_tables & gold_tables
    missed_tables = gold_tables - runtime_tables

    common_columns = 0
    missed_columns = 0
    for table in common_tables:
        common_columns += len(set(gold[table]) & set(runtime[table]))
        missed_columns += len(set(gold[table]) - set(runtime[table]))

    for table in missed_tables:
        missed_columns+=len(gold[table])

    value_scores = []
    for key in matching_keys:
        value_scores.append(similarity_score(gold[key], runtime[key]))

    value_score = sum(value_scores) / len(matching_keys) if matching_keys else 0

    total_score = (0.5 * key_score) + (0.5 * value_score)  # Weight keys & values equally
    return total_score, len(common_tables), len(missed_tables), common_columns, missed_columns

def score_schema(questions, score_dict, database_name):
    
    score, common_tables, common_columns, missed_tables, missed_columns = 0, 0, 0, 0, 0
    for item in questions:
        true_schema = item['schema_used']
        run_schema = item['runtime_schema_used']
        score_item, common_tables_item, missed_tables_item, common_columns_item, missed_columns_item = compare_dicts(true_schema, run_schema)
        common_tables += common_tables_item
        missed_tables += missed_tables_item
        common_columns += common_columns_item
        missed_columns += missed_columns_item
        score+=score_item

    avg_score = score/len(questions)
    score_dict['database'].append(database_name)
    score_dict['prune_score'].append(avg_score)
    score_dict['common_tables'].append(common_tables)
    score_dict['missed_tables'].append(missed_tables)
    score_dict['common_columns'].append(common_columns)
    score_dict['missed_columns'].append(missed_columns)
    score_dict['common_elements'].append((common_tables + common_columns))
    score_dict['missed_elements'].append((missed_tables + missed_columns))
    score_dict['missing_columns_%'].append(missed_columns/(common_columns + missed_columns))
    score_dict['missing_tables_%'].append(missed_tables/(common_tables + missed_tables))
    score_dict['missing_elements_%'].append((missed_tables + missed_columns)/(common_tables + common_columns + missed_tables + missed_columns))
    score_dict['num_queries'].append(len(questions))

    return score_dict

def process_databases(single_file = False):

    directories = [d for d in os.listdir(PATH_CONFIG.dataset_dir()) if os.path.isdir(os.path.join(PATH_CONFIG.dataset_dir(), d))]

    score_dict = {'database':[], 'prune_score':[], 'common_tables': [], 'missed_tables': [], 'common_columns': [], 'missed_columns': [], 'common_elements':[], 'missed_elements':[], 'missing_columns_%': [],'missing_tables_%':[],'missing_elements_%':[],  'num_queries':[]}
    if single_file:
        with open(PATH_CONFIG.processed_test_path(global_file=True)) as file:
            questions = json.load(file)
            file.close()

        score_dict = score_schema(questions, score_dict, "ALL")
        
    else:  
        for database in directories:           
            with open(PATH_CONFIG.processed_test_path(database_name=database)) as file:
                file_data=json.load(file)
                file.close()
            score_dict =  score_schema(file_data, score_dict, database)

    return score_dict

if __name__ == '__main__':
    """
    Main execution block:
    
    - Processes multiple databases by computing schema similarity scores.
    - Iterates through dataset directories and evaluates query schema consistency.
    - Computes an average pruning score based on schema similarity.
    - Aggregates results for both training and testing sets.
    - Outputs a DataFrame summarizing the pruning scores and query counts.

    The computed DataFrame contains the following columns:
        - 'database': Name of the database.
        - 'prune_score': Average similarity score between expected and runtime schemas.
        - 'num_queries': Number of queries processed for the database.
        - 'set': Indicates whether the data belongs to 'train' or 'test'.

    The final DataFrame is printed to the console.

    Run the script from the server directory as such:
    python -m internal_benchmarks.schema_pruning.prune_test
    """
    
    # Test on the Single Test File
    single_file = False

    score_dict = process_databases(single_file)
    df = pd.DataFrame(score_dict)
    avg_row = df.iloc[:, 1:].mean().round(2).to_frame().T
    avg_row.insert(0, "database", "AVERAGE") 
    df = pd.concat([df, avg_row], ignore_index=True)
    print(df.to_csv(sep="\t", index=False))
    