from dotenv import load_dotenv
from openai import OpenAI
import time
import os
import json
from utilities.constants.script_constants import BatchJobStatus, BATCH_DIR, BATCHOUTPUT_FILE


load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

openAI_client=OpenAI(api_key=OPENAI_API_KEY)

downloaded=[]

# getting the batch job ids created in the last run
with open('batch_jobs_created.txt', 'r') as file:
    batch_jobs = json.loads(file.read())
    file.close()

print(batch_jobs)


count=0
# while loop keeps retrying till all batch jobs have not been 
while len(downloaded)<len(batch_jobs):
    print(f'Try: {count}')
    for i in batch_jobs:
        if  i not in downloaded:
            job=openAI_client.batches.retrieve(i)
            if job.status==BatchJobStatus.COMPLETED:
                file_content=openAI_client.files.content(job.output_file_id)
                with open(BATCH_DIR+BATCHOUTPUT_FILE.from,'w') as file:
                    file.write(file_content.text)
                    file.close()
                print("Downloading:",i)
                downloaded.append(i)
    time.sleep(10)
    count+=1