"""
Script for masking and processing BIRD dataset questions.

This module implements functionality to mask entities in natural language questions
that correspond to database schema elements. It handles the preprocessing of the BIRD
dataset, applies schema linking and entity masking, and saves the processed data
to files for further use in text-to-SQL models.
"""

import json
import os

from tqdm import tqdm
from utilities.config import (DATABASE_SQLITE_PATH, DATASET_DIR,
                              MASKED_SAMPLE_DATA_FILE_PATH,
                              UNMASKED_SAMPLE_DATA_FILE_PATH)
from utilities.constants.script_constants import (
    PROCESSED_SAMPLE_DATA_FILE_PATH, SCHEMA_PATH)
from utilities.masking.linking_process import SpiderEncoderV2Preproc
from utilities.masking.linking_utils.utils import (
    load_tables, mask_question_with_schema_linking)
from utilities.masking.pretrained_embeddings import GloVe

from text2SQL.server.utilities.connections.sqlite import make_sqlite_connection


def json_preprocess(data_jsons, with_evidence):
    """
    Preprocess JSON data for schema linking.
    
    Processes each question in the dataset, handling evidence integration
    if requested, tokenizing questions, and restructuring the data for
    further processing.
    
    Args:
        data_jsons: List of question data dictionaries to process.
        with_evidence: Boolean flag indicating whether to include evidence text in the question.
        
    Returns:
        A list of preprocessed data dictionaries.
    """
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
    """
    Read and parse a JSON file.
    
    Args:
        path: Path to the JSON file to read.
        
    Returns:
        The parsed JSON content as a Python object.
    """
    with open(path, 'r') as file:
        return json.load(file)

def write_json_file(path, content):
    """
    Write content to a JSON file.
    
    Args:
        path: Path where the JSON file should be written.
        content: Python object to be serialized to JSON.
    """
    with open(path, 'w') as file:
        json.dump(content, file, indent=4)

def save_masked_questions(unmasked_questions, masked_questions, masked_file_path_format):
    """
    Save masked questions to files organized by database.
    
    Iterates through the databases, extracts the masked questions corresponding
    to each unmasked question, and saves them to database-specific files.
    
    Args:
        unmasked_questions: Dictionary mapping database names to lists of original questions.
        masked_questions: Dictionary mapping question IDs to masked question strings.
        masked_file_path_format: String format for the output file paths, with a {database_name} placeholder.
    """
    for database in tqdm(unmasked_questions, desc="Saving Masked Questions"):
        masked_questions_to_save=[]
        for question in unmasked_questions[database]:
            masked_questions_to_save.append({
                'id':question['id'],
                'question':masked_questions[question['id']],
                'evidence':question['evidence'],
                'answer':question['answer']
            })
        masked_file_path=masked_file_path_format.format(database_name=database)

        write_json_file(masked_file_path, masked_questions_to_save)
    

if __name__ == "__main__":

    directories = [d for d in os.listdir(DATASET_DIR) if os.path.isdir(os.path.join(DATASET_DIR, d))]

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