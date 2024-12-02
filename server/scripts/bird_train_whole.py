import json
import os

TRAIN_FILE = "././data/bird/train/train.json"
SAMPLE_QUESTION_FILE='././data/spider2/spider2-snow/sample_questions_from_bird.json'

if __name__ == "__main__":

    """
    Takes the Bird Dataset and creates sample questions from the whole dataset in one consolidate file.
    """
    
    with open(TRAIN_FILE, 'r') as file:
        data = json.load(file)

    samples=[{
            'question':item['question'],
            'answer':item['SQL'],
            'id':idx,
            'evidence':item['evidence']
    }
    for idx, item in enumerate(data)
    ]

    with open(SAMPLE_QUESTION_FILE, 'w') as file:
        json.dump(samples, file, indent=4)