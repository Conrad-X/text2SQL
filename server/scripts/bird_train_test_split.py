import json
import os
import random
from sklearn.model_selection import train_test_split

dev_json_path='./bird/dev_20240627/dev.json'

with open(dev_json_path,'r') as file:
    json_file=json.loads(file.read())
    file.close()


root_dir='experiment_dir'

dbs={}
for i in json_file:
    try:
        dbs[i['db_id']].append(i)
    except KeyError:
        dbs[i['db_id']]=[i]

for i in dbs:
    print(i, len(dbs[i]))


for i in dbs:
    data=dbs[i]
    random.shuffle(data)
    train, test = train_test_split(data, test_size=0.3,random_state=42)
    if not os.path.exists(f"{root_dir}/{i}/test"):
        os.makedirs(f"{root_dir}/{i}/test")
    if not os.path.exists(f"{root_dir}/{i}/sample_questions"):
        os.makedirs(f"{root_dir}/{i}/sample_questions")
    with open(f"{root_dir}/{i}/test/{i}.json",'w') as file:
        json.dump(train,file, indent=4)
        file.close()
    sample_questions=[]
    for idx, j in enumerate(test):
        sample_questions.append({'id':idx+1,'question':j['question'],'answer':j['SQL']})
    with open(f"{root_dir}/{i}/sample_questions/{i}_schema.json",'w') as file:
        json.dump(sample_questions,file, indent=4)
        file.close()
    
    
