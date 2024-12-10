import argparse
import json
import os
import pickle
from pathlib import Path
import sqlite3
from tqdm import tqdm
import random
import time
import copy
from scripts.masking.pretrained_embeddings import (
    GloVe
)
from scripts.masking.linking_utils.spider_match_utils import(
    load_tables
)
from scripts.masking.linking_process import (
    SpiderEncoderV2Preproc
)
from utilities.constants.script_constants import(
    GENERATE_BATCH_SCRIPT_PATH,
)
from scripts.masking.linking_utils.application import (
    mask_question_with_schema_linking
)

directories = [d for d in os.listdir(GENERATE_BATCH_SCRIPT_PATH) if os.path.isdir(os.path.join(GENERATE_BATCH_SCRIPT_PATH, d))]
schema_path="./data/bird/train/train_tables.json"
unmasked_question_path="./data/bird/train/train_databases/"


def json_preprocess(data_jsons, with_evidence):
    new_datas = []
    for data_json in data_jsons:
        if with_evidence and len(data_json["evidence"]) > 0:
            data_json['question'] = (data_json['question'] + " " + data_json["evidence"]).strip()
        question = data_json['question']
        tokens = []
        for token in question.split(' '):
            if len(token) == 0:
                continue
            if token[-1] in ['?', '.', ':', ';', ','] and len(token) > 1:
                tokens.extend([token[:-1], token[-1:]])
            else:
                tokens.append(token)
        data_json['question_toks'] = tokens
        data_json['query'] = data_json['answer']
        new_datas.append(data_json)
    return new_datas

unmasked_questions={}
for database in tqdm(directories,desc="Bird Preprocessing"):
    unmasked_file_path=f"{unmasked_question_path}{database}/samples/unmasked_{database}.json"
    preprocessed_data=f"{unmasked_question_path}{database}/samples/processed_{database}.json"

    with open(unmasked_file_path, 'r') as file:
        unmasked_questions[database]=json.load(file)
        file.close()
    
    processed_json=json_preprocess(unmasked_questions[database], False)

    with open(preprocessed_data, 'w') as file:
        json.dump(processed_json, file, indent=4)
        file.close()

sqlite_dict={}
for database in directories:
    sqlite_dict[database]=f"{GENERATE_BATCH_SCRIPT_PATH}{database}/{database}.splite"

schemas, _ = load_tables([schema_path])

for db_id, schema in tqdm(schemas.items(), desc="Defining Schema Connections"):
    
    try:
        sqlite_path = sqlite_dict[db_id]
    except KeyError as e:
        continue
    source: sqlite3.Connection
    with sqlite3.connect(str(sqlite_path)) as source:
        dest = sqlite3.connect(':memory:')
        dest.row_factory = sqlite3.Row
        source.backup(dest)
    schema.connection = dest


word_emb = GloVe(kind='42B', lemmatize=True)
linking_processor = SpiderEncoderV2Preproc("./data/bird/train",
        min_freq=4,
        max_count=5000,
        include_table_name_in_column=False,
        word_emb=word_emb,
        fix_issue_16_primary_keys=True,
        compute_sc_link=True,
        compute_cv_link=True)

for database in tqdm(directories,desc=f'Linking'):
    unmasked_file_path=f"{unmasked_question_path}{database}/samples/processed_{database}.json"

    unmasked_samples=[]
    with open(unmasked_file_path, 'r') as file:
        unmasked_samples=json.load(file)
        file.close()
    schema = schemas[database]
    for question in unmasked_samples:
        linking_processor.add_item(question, schema, "test", None)

linking_processor.save()

schema_linking_file_path="./data/bird/train/enc/test_schema-linking.jsonl"

linking_infos = []
with open(schema_linking_file_path, 'r') as f:
    for line in f.readlines():
        if line.strip():
            linking_infos.append(json.loads(line))

masked_sample_questions=mask_question_with_schema_linking(linking_infos, "<mask>", "<unk>")


masked_questions={}
for database in tqdm(unmasked_questions, desc="Saving Masked Questions"):
    masked_questions_to_save=[]
    for question in unmasked_questions[database]:
        masked_questions_to_save.append({
            'id':question['id'],
            'question':masked_sample_questions[question['id']],
            'evidence':question['evidence'],
            'answer':question['answer']
        })
    masked_file_path=f"{unmasked_question_path}{database}/samples/masked_{database}.json"

    with open(masked_file_path, 'w') as file:
        json.dump(masked_questions_to_save, file, indent=4)
        file.close()
    


    
        













    


