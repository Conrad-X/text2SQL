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

#Iterating over all databases
for database in tqdm(directories,desc=f'Processing Directories'):

    # if the batch output file exists i.e. batch has been run
    if os.path.exists(f"{GENERATE_BATCH_SCRIPT_PATH}{database}{BATCH_DIR_SUFFIX}{BATCHOUTPUT_FILE_PREFIX}_{database}.jsonl"):

        # get the batch output file
        with open(f"{GENERATE_BATCH_SCRIPT_PATH}{database}{BATCH_DIR_SUFFIX}{BATCHOUTPUT_FILE_PREFIX}_{database}.jsonl",'r') as file:
            batch_output=[json.loads(line) for line in file]
            file.close()

        # get the test.json file of the corresponding DB
        with open(f"{GENERATE_BATCH_SCRIPT_PATH}{database}/test_{database}.json",'r') as file:
            test_file=json.loads(file.read())
            file.close()
        
        predicted_scripts={}
        gold_items=[]

        # Formats each predicted query
        for prediction in batch_output:
            id=prediction['custom_id'][8:]
            pred_sql=prediction['response']['body']['choices'][0]['message']['content']
            predicted_scripts[id]=f'{pred_sql}\t----- bird -----\t{database}'

            # finds the corresponding gold query of the predicted query
            for item in test_file:
                if int(id)==item['id']:
                    gt_sql=item['SQL']
                    gold_items.append(f'{gt_sql}\t{database}')


        
        with open(f"{GENERATE_BATCH_SCRIPT_PATH}{database}/{FORMATTED_PRED_FILE}_{database}.json",'w') as file:
            json.dump(predicted_scripts, file)
            file.close()
        
        # makes a gold .sql file that contains gold queries of the predicted queries in the same order
        with open(f'{GENERATE_BATCH_SCRIPT_PATH}{database}/gold_{database}.sql','w') as file:
            for item in gold_items:
                file.write(f'{item}\n')
            file.close()
        
        