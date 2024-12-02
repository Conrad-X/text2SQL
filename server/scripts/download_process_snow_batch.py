from dotenv import load_dotenv
from openai import OpenAI
import time
import os
import json
from utilities.constants.script_constants import BatchJobStatus, BATCHOUTPUT_FILE_PREFIX, BATCH_JOB_METADATA_DIR, GENERATE_BATCH_SCRIPT_PATH, BATCH_DIR_SUFFIX, GENERATE_BATCH_SCRIPT_PATH


BATCH_ID="batch_67486a3636f48190bc8d27d0dfc0444f"
OUTPUT_FOLDER='./data/spider2/spider2-snow/results/'


load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

openAI_client=OpenAI(api_key=OPENAI_API_KEY)

try_count=1
while True:
    print("Try: ",try_count)
    job=openAI_client.batches.retrieve(BATCH_ID)
    if job.status==BatchJobStatus.COMPLETED.value:
        file_content=openAI_client.files.content(job.output_file_id)

        batch_output=file_content.text.split('\n')[:-1]
        batch_output=[json.loads(line) for line in batch_output]
       
        break
    try_count+=1
    time.sleep(10)

formatted_response={ response['custom_id']:response['response']['body']['choices'][0]['message']['content'] for response in batch_output}


os.makedirs(OUTPUT_FOLDER, exist_ok=True)

for response in formatted_response:
    with open(f'{OUTPUT_FOLDER}{response}.sql', 'w') as file:
        file.write(formatted_response[response])
        file.close()

