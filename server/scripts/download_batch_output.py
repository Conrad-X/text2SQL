from dotenv import load_dotenv
from openai import OpenAI
import time
import os
import json
from utilities.constants.script_constants import BatchJobStatus, BATCHOUTPUT_FILE_PREFIX, BATCH_JOB_METADATA_DIR, GENERATE_BATCH_SCRIPT_PATH, BATCH_DIR_SUFFIX


load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

openAI_client=OpenAI(api_key=OPENAI_API_KEY)

downloaded=[]

# Enter the correct timestamp file of the last run batch jobs
time_stamp="2024-11-01_15:21:27.txt"

# getting the batch job ids created in the last run
with open(f"{BATCH_JOB_METADATA_DIR}{time_stamp}", 'r') as file:
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
            if job.status==BatchJobStatus.COMPLETED.value:
                file_content=openAI_client.files.content(job.output_file_id)
                with open(f"{GENERATE_BATCH_SCRIPT_PATH}{batch_jobs[i]['database']}{BATCH_DIR_SUFFIX}{BATCHOUTPUT_FILE_PREFIX}_{batch_jobs[i]['database']}.jsonl",'w') as file:
                    file.write(file_content.text)
                    file.close()
                print("Downloading:",i)
                downloaded.append(i)
    time.sleep(10)
    count+=1