import os
import requests
import time
import json
from tqdm import tqdm
from dotenv import load_dotenv
from openai import OpenAI
from utilities.constants.script_constants import *



load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")


# getting databases 
directories = [d for d in os.listdir(GENERATE_BATCH_SCRIPT_PATH) if os.path.isdir(os.path.join(GENERATE_BATCH_SCRIPT_PATH, d))]


# iterating over all Databases
for i in tqdm(directories,desc=f'Processing Directories'):

    response=requests.post(DB_CHANGE_ENPOINT,json={'database_type':"hotel","sample_path":f"{GENERATE_BATCH_RELATIVE_PATH}{i}/sample_questions/{i}_schema.json"})
    with open(f"{GENERATE_BATCH_SCRIPT_PATH}{i}/test/{i}.json",'r') as file:
        json_file=json.loads(file.read())
        file.close()
    
    prompts=[]

    # iterating over all prompts in each database
    for item in tqdm(json_file[:3],desc=f'Generating prompts for {i}'):

        payload={'prompt_type':PROMPT_TYPE,'shots':NUM_SHOTS,'question':item['question']}
        response=requests.post(PROMPT_GENERATE_ENDPOINT,json=payload)

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
    
    # saving the generated batch input file in each DB directory        
    with open(f"{GENERATE_BATCH_SCRIPT_PATH}{i}/test/{i}_batch_input.jsonl",'w') as file:
        for item in prompts:
            json.dump(item, file)
            file.write('\n')
        file.close()


print("BATCH INPUT FILES CREATED")


openAI_client=OpenAI(api_key=OPENAI_API_KEY)
uploaded_files=[]
batch_jobs=[]
batch_jobs_dict={}

# for each DB upload the batch input file and create a batch job
for i in tqdm(directories,desc='Uploading and Creating Batch Jobs'):

    # upload batch input file
    with open(f"{GENERATE_BATCH_SCRIPT_PATH}{i}/test/{i}_batch_input.jsonl",'rb') as file:
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
    batch_jobs_dict[batch.id]=f"{GENERATE_BATCH_SCRIPT_PATH}{i}/test/"


# storing batch job id with corresponding DB directory
with open("batch_jobs_created.txt",'w') as file:
    json.dump(batch_jobs_dict,file)
    file.close()