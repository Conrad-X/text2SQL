import os
import requests
import time
import json
from tqdm import tqdm

path = "data/sample_questions_and_queries/experiment_dir/"
directories = [d for d in os.listdir(path) if os.path.isdir(os.path.join(path, d))]
print("DBS:",directories)


for i in directories:
    print(f"{path}{i}/sample_questions/{i}")

db_change_url="http://localhost:8000/database/change/"
prompt_generate_url='http://localhost:8000/prompts/generate/'
prompt_type='dail_sql'
num_shots=3


for i in tqdm(directories,desc='Processing Directories'):
    # print("changing to",i)
    response=requests.post(db_change_url,json={'database_type':f"experiment_dir/{i}/sample_questions/{i}"})
    # print(response.json())

    with open(f"{path}{i}/test/{i}.json",'r') as file:
        json_file=json.loads(file.read())
        file.close()
    prompts=[]
    for item in tqdm(json_file[:10],desc=f'Generating prompts for {i}',leave=False):
        payload={'prompt_type':prompt_type,'shots':num_shots,'question':item['question']}
        response=requests.post(prompt_generate_url,json=payload)
        prompts.append({'id':item['question_id'],'prompt':response.json()['generated_prompt']})
        # print(response.json()['generated_prompt'])
    with open(f"{path}{i}/test/{i}_prompts.json",'w') as file:
        json.dump(prompts, file, indent=4)
        file.close()

        











