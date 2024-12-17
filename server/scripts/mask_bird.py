import json
import os
import sqlite3
from tqdm import tqdm
from utilities.masking.pretrained_embeddings import (
    GloVe
)
from utilities.masking.linking_utils.utils import(
    load_tables,
    mask_question_with_schema_linking
)
from utilities.masking.linking_process import (
    SpiderEncoderV2Preproc
)
from utilities.constants.script_constants import(
    SCHEMA_PATH,
    PROCESSED_SAMPLE_DATA_FILE_PATH,
)
from utilities.config import (
    DATASET_DIR,
    DATABASE_SQLITE_PATH, 
    UNMASKED_SAMPLE_DATA_FILE_PATH,
    MASKED_SAMPLE_DATA_FILE_PATH,
    
)


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

def read_json_file(path):
    with open(path, 'r') as file:
        json_file=json.load(file)
        file.close()
    return json_file

def write_json_file(path, content):
    with open(path, 'w') as file:
        json.dump(content, file, indent=4)
        file.close()

def make_sqlite_connection(path):
    source: sqlite3.Connection
    with sqlite3.connect(str(path)) as source:
        dest = sqlite3.connect(':memory:')
        dest.row_factory = sqlite3.Row
        source.backup(dest)
    return dest

def save_masked_questions(unmasked_questions, masked_questions, masked_file_path):
    for database in tqdm(unmasked_questions, desc="Saving Masked Questions"):
        masked_questions_to_save=[]
        for question in unmasked_questions[database]:
            masked_questions_to_save.append({
                'id':question['id'],
                'question':masked_questions[question['id']],
                'evidence':question['evidence'],
                'answer':question['answer']
            })
        masked_file_path=masked_file_path.format(database_name=database)

        write_json_file(masked_file_path, masked_questions_to_save)
    

if __name__ == "__main__":

    directories = [d for d in os.listdir(DATASET_DIR) if os.path.isdir(os.path.join(DATASET_DIR, d))][:2]

    #processing unmasked sample questions from the BIRD Dataset
    unmasked_questions={}
    for database in tqdm(directories,desc="Bird Preprocessing"):
        preprocessed_data=PROCESSED_SAMPLE_DATA_FILE_PATH.format(database_name=database)
        
        unmasked_questions[database]=read_json_file(UNMASKED_SAMPLE_DATA_FILE_PATH.format(database_name=database))
        processed_json=json_preprocess(unmasked_questions[database], False)
        write_json_file(preprocessed_data, processed_json)

    #setting sqlite paths
    sqlite_dict={}
    for database in directories:
        sqlite_dict[database]=DATABASE_SQLITE_PATH.format(database_name=database)

    # getting schemas from train_tables.json
    schemas, _ = load_tables([SCHEMA_PATH])

    #declaring embedding model
    word_emb = GloVe(kind='42B', lemmatize=True)

    #declaring linking model
    linking_processor = SpiderEncoderV2Preproc("./data/bird/train",
            min_freq=4,
            max_count=5000,
            include_table_name_in_column=False,
            word_emb=word_emb,
            fix_issue_16_primary_keys=True,
            compute_sc_link=True,
            compute_cv_link=False)

    #transforming processed unmasked samples to masked samples
    for database in tqdm(directories,desc=f'Linking'):
        unmasked_file_path=PROCESSED_SAMPLE_DATA_FILE_PATH.format(database_name=database)

        unmasked_samples=read_json_file(unmasked_file_path)
        schema = schemas[database]
        
        try:
            sqlite_path = sqlite_dict[database]
        except KeyError as e:
            print(f"Database path not found: {database}")
            continue
        
        dest=make_sqlite_connection(sqlite_path)
        
        for question in tqdm(unmasked_samples, desc=f'Linking {database}'):
            linking_processor.add_item(question, schema,dest)
        dest.close()

    linking_infos = linking_processor.get_linked_schema()

    masked_sample_questions=mask_question_with_schema_linking(linking_infos, "<mask>", "<unk>")

    save_masked_questions(unmasked_questions, masked_sample_questions, MASKED_SAMPLE_DATA_FILE_PATH)
        


    
        













    


