
from dotenv import load_dotenv
from openai import OpenAI
import time
import os
import json


load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

openAI_client=OpenAI(api_key=OPENAI_API_KEY)

time.sleep(5)
downloaded=[]

with open('batch_jobs_created.txt', 'r') as file:
    batch_jobs = json.loads(file.read())
    file.close()

count=0
while len(downloaded)<len(batch_jobs):
    print(f'Try: {count}')
    for i in batch_jobs:
        if  i not in downloaded:
            job=openAI_client.batches.retrieve(i)
            if job.status=='completed':
                file_content=openAI_client.files.content(job.output_file_id)
                with open(f"{batch_jobs[i]}output.jsonl",'w') as file:
                    file.write(file_content.text)
                    file.close()
                print("downloading ",i)
                downloaded.append(i)
    time.sleep(10)
    count+=1