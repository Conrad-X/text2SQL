import json
import os
from tqdm import tqdm

from utilities.constants.script_constants import (
    GENERATE_BATCH_RELATIVE_PATH,
    GENERATE_BATCH_SCRIPT_PATH,
    BATCH_DIR_SUFFIX,
    FORMATTED_PRED_FILE,
    BATCHOUTPUT_FILE_PREFIX
) 

# getting databases 
directories = [d for d in os.listdir(GENERATE_BATCH_SCRIPT_PATH) if os.path.isdir(os.path.join(GENERATE_BATCH_SCRIPT_PATH, d))]

for database in tqdm(directories[:1],desc=f'Processing Directories'):
    if os.path.exists(f"{GENERATE_BATCH_SCRIPT_PATH}{database}{BATCH_DIR_SUFFIX}{BATCHOUTPUT_FILE_PREFIX}_{database}.jsonl"):

        with open(f"{GENERATE_BATCH_SCRIPT_PATH}{database}{BATCH_DIR_SUFFIX}{BATCHOUTPUT_FILE_PREFIX}_{database}.jsonl",'r') as file:
            batch_output=[json.loads(line) for line in file]
            file.close()
        
        predicted_scripts={}

        for prediction in batch_output:
            id=prediction['custom_id'][8:]
            pred_sql=prediction['response']['body']['choices'][0]['message']['content']
            predicted_scripts[id]=f'{pred_sql}\t----- bird -----\t{database}'
        
        with open(f"{GENERATE_BATCH_SCRIPT_PATH}{database}/{FORMATTED_PRED_FILE}_{database}.json",'w') as file:
            json.dump(predicted_scripts, file)
            file.close()