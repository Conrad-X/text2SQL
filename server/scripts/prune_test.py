import json
from utilities.config import (
    DATASET_DIR,
    UNMASKED_SAMPLE_DATA_FILE_PATH,
    TEST_DATA_FILE_PATH
)
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
        return SequenceMatcher(None, val1, val2).ratio()  # String similarity
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

def compare_dicts(dict1, dict2):
    """Compute similarity score between two dictionaries."""
    keys1, keys2 = set(dict1.keys()), set(dict2.keys())
    matching_keys = keys1 & keys2  # Common keys
    total_keys = keys1 | keys2  # All unique keys

    if not total_keys:
        return 1.0  # Both empty dicts are identical

    key_score = len(matching_keys) / len(total_keys)  # Key presence score

    value_scores = []
    for key in matching_keys:
        value_scores.append(similarity_score(dict1[key], dict2[key]))

    value_score = sum(value_scores) / len(matching_keys) if matching_keys else 0

    total_score = (0.5 * key_score) + (0.5 * value_score)  # Weight keys & values equally
    return total_score

if __name__ == '__main__':

    directories = [d for d in os.listdir(DATASET_DIR) if os.path.isdir(os.path.join(DATASET_DIR, d))]

    score_dict = {'database':[], 'prune_score':[], 'num_queries':[], 'set':[]}
    for database in directories:

        with open(UNMASKED_SAMPLE_DATA_FILE_PATH.format(database_name=database)) as file:
            train_data=json.load(file)
            file.close()
        score=0
        for item in train_data:
            true_schema = item['schema_used']
            run_schema = item['runtime_schema_used']
            score+=compare_dicts(true_schema, run_schema)

        avg_score=score/len(train_data)
        score_dict['database'].append(database)
        score_dict['prune_score'].append(avg_score)
        score_dict['num_queries'].append(len(train_data))
        score_dict['set'].append('train')

        with open(TEST_DATA_FILE_PATH.format(database_name=database)) as file:
            test_data=json.load(file)
            file.close()
        score=0
        for item in test_data:
            true_schema = item['schema_used']
            run_schema = item['runtime_schema_used']
            score+=compare_dicts(true_schema, run_schema)

        avg_score=score/len(test_data)
        score_dict['database'].append(database)
        score_dict['prune_score'].append(avg_score)
        score_dict['num_queries'].append(len(test_data))
        score_dict['set'].append('test')

    for split in ['train','test']:
        with open(f"{DATASET_DIR}/processed_{split}.json") as file:
            questions = json.load(file)
            file.close()

        score = 0
        for item in questions:
            true_schema = item['schema_used']
            run_schema = item['runtime_schema_used']
            score+=compare_dicts(true_schema, run_schema)

        avg_score = score/len(questions)
        score_dict['database'].append("ALL")
        score_dict['prune_score'].append(avg_score)
        score_dict['num_queries'].append(len(questions))
        score_dict['set'].append(split)

    score_df=pd.DataFrame(score_dict)
    print(score_df)