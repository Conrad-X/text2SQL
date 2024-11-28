import os
import requests
from datetime import datetime
import json
from tqdm import tqdm
from dotenv import load_dotenv
from openai import OpenAI
import time
from utilities.constants.script_constants import *



load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")


# getting databases 
directories = [d for d in os.listdir(GENERATE_BATCH_SCRIPT_PATH) if os.path.isdir(os.path.join(GENERATE_BATCH_SCRIPT_PATH, d))]


# iterating over all Databases to generate prompts and make batch input files
for database in tqdm(directories,desc=f'Processing Directories'):

    response=requests.post(DB_CHANGE_ENPOINT,json={'database_name':database,"sample_path":f"{GENERATE_BATCH_RELATIVE_PATH}{database}{SAMPLE_QUESTIONS_DIR}unmasked_{database}.json"})
    with open(f"{GENERATE_BATCH_SCRIPT_PATH}{database}/test_{database}.json",'r') as file:
        json_file=json.loads(file.read())
        file.close()
    
    prompts=[]

    if not response.status_code==200:
        print(response.json())
        exit() 

    # iterating over all NLP questions in each database
    for item in tqdm(json_file,desc=f'Generating prompts for {database}'):

        payload={'prompt_type':PROMPT_TYPE,'shots':NUM_SHOTS,'question':item['question']}
        response=requests.post(PROMPT_GENERATE_ENDPOINT,json=payload)

        if not response.status_code==200:
            print(response.json())
            exit()
         
        prompts.append({
                "custom_id": f"request-{item['question_id']}",
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": MODEL,
                    "messages": [
                        {"role": "system", "content": response.json()['generated_prompt']},
                    ],
                    "max_tokens": MAX_TOKENS,
                    "temperature": TEMPERATURE
                }
            })

    os.makedirs(f"{GENERATE_BATCH_SCRIPT_PATH}{database}{BATCH_DIR_SUFFIX}" ,exist_ok=True)
    with open(f"{GENERATE_BATCH_SCRIPT_PATH}{database}{BATCH_DIR_SUFFIX}{BATCHINPUT_FILE_PREFIX}_{database}.jsonl",'w') as file:
        for item in prompts:
            json.dump(item, file)
            file.write('\n')
        file.close()



print("BATCH INPUT FILES CREATED")
# exit()


openAI_client=OpenAI(api_key=OPENAI_API_KEY)
uploaded_files=[]
batch_jobs=[]
batch_jobs_dict={}

# for each DB upload the batch input file and create a batch job
for i in tqdm(directories,desc='Uploading and Creating Batch Jobs'):

    # upload batch input file
    with open(f"{GENERATE_BATCH_SCRIPT_PATH}{i}{BATCH_DIR_SUFFIX}{BATCHINPUT_FILE_PREFIX}_{i}.jsonl",'rb') as file:
        uploaded_file=openAI_client.files.create(file=file, purpose='batch')
        file.close()
    uploaded_files.append(uploaded_file.id)

    # create batch job
    batch = openAI_client.batches.create(
                input_file_id=uploaded_file.id,
                endpoint="/v1/chat/completions",
                completion_window="24h",
            )
    batch_jobs.append(batch.id)
    batch_jobs_dict[batch.id]={"dataset":GENERATE_BATCH_SCRIPT_PATH, "database":i}

now=datetime.now()


# storing batch job id with corresponding DB directory
time_stamp=now.strftime("%Y-%m-%d_%H:%M:%S")
os.makedirs(f"{BATCH_JOB_METADATA_DIR}", exist_ok=True)
with open(f"{BATCH_JOB_METADATA_DIR}{time_stamp}.txt",'w') as file:
    json.dump(batch_jobs_dict,file)
    file.close()

print("METADATA IN: ",f"{BATCH_JOB_METADATA_DIR}{time_stamp}.txt")