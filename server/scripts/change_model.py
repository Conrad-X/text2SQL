import json
import os
import time
from datetime import datetime

import requests
from dotenv import load_dotenv
from openai import OpenAI
from tqdm import tqdm
from utilities.constants.script_constants import *

load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")


# getting databases 
directories = [d for d in os.listdir(GENERATE_BATCH_SCRIPT_PATH) if os.path.isdir(os.path.join(GENERATE_BATCH_SCRIPT_PATH, d))]


# iterating over all Databases to generate prompts and make batch input files
for database in tqdm(directories,desc=f'Processing Directories'):

    input_file=[]
    with open(f"{GENERATE_BATCH_SCRIPT_PATH}{database}{BATCH_DIR_SUFFIX}{BATCHINPUT_FILE_PREFIX}_{database}.jsonl",'r') as file:
        for line in file:
            dict_item = json.loads(line.strip())  # Parse JSON line to a dictionary
            input_file.append(dict_item)
        file.close()

    for item in input_file:
        item['body']['model']=MODEL
    
    with open(f"{GENERATE_BATCH_SCRIPT_PATH}{database}{BATCH_DIR_SUFFIX}{BATCHINPUT_FILE_PREFIX}_{database}.jsonl",'w') as file:
        for item in input_file:
            json.dump(item, file)
            file.write('\n')
        file.close()



