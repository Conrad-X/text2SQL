import json
import os
import random

import requests
from dotenv import load_dotenv
from openai import OpenAI
from tqdm import tqdm
from utilities.constants.script_constants import (
    DB_CHANGE_ENPOINT, FINE_TUNE_EXAMPLES_DIR, FINE_TUNE_EXAMPLES_FILE_PREFIX,
    FINE_TUNE_NUMBER, FINE_TUNE_SYSTEM_MESSAGE, GENERATE_BATCH_RELATIVE_PATH,
    GENERATE_BATCH_SCRIPT_PATH, NUM_SHOTS, PROMPT_GENERATE_ENDPOINT,
    PROMPT_TYPE, SAMPLE_QUESTIONS_DIR, APIStatusCode)

# getting databases 
directories = [d for d in os.listdir(GENERATE_BATCH_SCRIPT_PATH) if os.path.isdir(os.path.join(GENERATE_BATCH_SCRIPT_PATH, d))]

ft_examples=[]

# iterating over all Databases to generate prompts and make batch input files
for database in tqdm(directories,desc=f'Processing Directories:'):

    response=requests.post(DB_CHANGE_ENPOINT,json={'database_name':database,"sample_path":f"{GENERATE_BATCH_RELATIVE_PATH}{database}{SAMPLE_QUESTIONS_DIR}unmasked_{database}.json"})

    with open(f"{GENERATE_BATCH_SCRIPT_PATH}{database}{SAMPLE_QUESTIONS_DIR}unmasked_{database}.json") as file:
        sample_qs=json.loads(file.read())
        file.close()
    
    if not response.status_code==APIStatusCode.SUCCESS.value:
        print(response.json())
        exit() 


    for sample in sample_qs:
        payload={'prompt_type':PROMPT_TYPE,'shots':NUM_SHOTS,'question':sample['question']}
        response=requests.post(PROMPT_GENERATE_ENDPOINT,json=payload)

        if not response.status_code==200:
            print(response.json())
            exit() 

        entry={"messages":[
            {"role":"system", "content":FINE_TUNE_SYSTEM_MESSAGE},
            {"role":"user","content":response.json()['generated_prompt']},
            {"role":"assistant","content":sample['answer']}
        ]} 

        ft_examples.append(entry)

ft_training_data=random.sample(ft_examples,FINE_TUNE_NUMBER)

os.makedirs(f"{FINE_TUNE_EXAMPLES_DIR}" ,exist_ok=True)
with open(f"{FINE_TUNE_EXAMPLES_DIR}{FINE_TUNE_EXAMPLES_FILE_PREFIX}.jsonl",'w') as file:
    for i, item in enumerate(ft_training_data):
        json.dump(item, file)
        if i < len(ft_training_data) - 1: 
            file.write('\n')
    file.close()



    



