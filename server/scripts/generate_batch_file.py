import os
import requests
import time
import json
from tqdm import tqdm
from dotenv import load_dotenv
from openai import OpenAI

MODEL='gpt-4o-mini'
MAX_TOKENS=1000
TEMPERATURE=0.7


script_path = "../data/bird/"
relative_path='./data/bird/'
db_change_url="http://localhost:8000/database/change/"
prompt_generate_url='http://localhost:8000/prompts/generate/'
prompt_type='dail_sql'
num_shots=3

directories = [d for d in os.listdir(script_path) if os.path.isdir(os.path.join(script_path, d))]
directories=directories[:2]

for i in tqdm(directories,desc=f'Processing Directories'):

    response=requests.post(db_change_url,json={'database_type':"hotel","sample_path":f"{relative_path}{i}/sample_questions/{i}_schema.json"})
    with open(f"{script_path}{i}/test/{i}.json",'r') as file:
        json_file=json.loads(file.read())
        file.close()
    prompts=[]
    for item in tqdm(json_file[:3],desc=f'Generating prompts for {i}'):
        payload={'prompt_type':prompt_type,'shots':num_shots,'question':item['question']}
        response=requests.post(prompt_generate_url,json=payload)
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
    with open(f"{script_path}{i}/test/{i}_prompts_batch.jsonl",'w') as file:
        for item in prompts:
            json.dump(item, file)
            file.write('\n')
        file.close()


print("BATCH INPUT FILES CREATED")

load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

openAI_client=OpenAI(api_key=OPENAI_API_KEY)
uploaded_files=[]
batch_jobs=[]
batch_jobs_dict={}


for i in tqdm(directories,desc='Uploading and Creating Batch Jobs'):
    with open(f"{script_path}{i}/test/{i}_prompts_batch.jsonl",'rb') as file:
        uploaded_file=openAI_client.files.create(file=file, purpose='batch')
        file.close()
    uploaded_files.append(uploaded_file.id)

    batch = openAI_client.batches.create(
                input_file_id=uploaded_file.id,
                endpoint="/v1/chat/completions",
                completion_window="24h",
            )
    batch_jobs.append(batch.id)
    batch_jobs_dict[batch.id]=f"{script_path}{i}/test/"



with open("batch_jobs_created.txt",'w') as file:
    json.dump(batch_jobs_dict,file)
    file.close()