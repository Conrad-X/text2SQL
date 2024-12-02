import argparse
import requests
import json
from dotenv import load_dotenv
from openai import OpenAI
import os
from utilities.constants.script_constants import(
    DB_CHANGE_ENDPOINT,
    PROMPT_TYPE,
    NUM_SHOTS,
    PROMPT_GENERATE_ENDPOINT,
    MAX_TOKENS,
    TEMPERATURE,
    MODEL
)


load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate and Upload Batch File for the Snow Dataset")
    parser.add_argument("--test_file", type=str,  default='./data/spider2/spider2-snow/test_data/spider2-snow.jsonl', help="File path to the snow test file .jsonl")
    parser.add_argument("--sample_questions", type=str, default="./data/spider2/spider2-snow/sample_questions_from_bird.json", help="File path to sample questions for few shots")
    parser.add_argument("--upload", type=int, default=1, help="True if you also want to upload batches")
    parser.add_argument("--batch_input_path", type=str, default="")
    args = parser.parse_args()

    print(args)
    batch_input_path=args.batch_input_path
    if args.batch_input_path=="":
        path_split=args.test_file.split('/')[:-1]
        batch_input_path='/'.join(path_split)+'/snow_batch_input.jsonl'
    
    print("batch_input_path:",batch_input_path)

    response=requests.post(DB_CHANGE_ENDPOINT, json={'database_name':'hotel', "sample_path":args.sample_questions})
    print("Vector DB created")

    if not response.status_code==APIStatusCode.SUCCESS.value:
        print(response.json())
        exit()

    with open(args.test_file, 'r') as file:
        test_questions=[json.loads(line) for line in file]
        file.close()
    
    print(test_questions[:1])
    prompts=[]
    for question in test_questions:
        payload={'prompt_type':PROMPT_TYPE,'shots':NUM_SHOTS,'question':question['instruction']}
        response=requests.post(PROMPT_GENERATE_ENDPOINT,json=payload)

        if not response.status_code==APIStatusCode.SUCCESS.value:
            print(response.json())
            exit()
        
        prompts.append({
                "custom_id": f"{question['instance_id']}",
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
    
    with open(batch_input_path, 'w') as file:
        for prompt in prompts:
            json.dump(prompt, file)
            file.write('\n')
        file.close()

    print("BATCH INPUT FILES CREATED")
    
    if not args.upload:
        exit()


    openAI_client=OpenAI(api_key=OPENAI_API_KEY)

    with open(batch_input_path, 'rb') as file:
        uploaded_file=openAI_client.files.create(file=file, purpose='batch')
        file.close()
    
    batch = openAI_client.batches.create(
                input_file_id=uploaded_file.id,
                endpoint="/v1/chat/completions",
                completion_window="24h",
            )

    print("Batch ID: ",batch.id)



    


    
