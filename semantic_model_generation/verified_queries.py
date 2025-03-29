import json
import random
import semantic_model_pb2
from config import(
    VERIFIED_QUERY_NUMBER
)

def get_verified_queries(sample_path):
    
    with open(sample_path, 'r') as file:
        sample_qs=json.loads(file.read())
        file.close()
    
    sample_qs=random.sample(sample_qs, VERIFIED_QUERY_NUMBER)
    verified_queries=[]
    for sample in sample_qs:
        verified_queries.append(semantic_model_pb2.VerifiedQuery(
            name=str(sample['id']),
            question=str(sample['question']),
            sql=str(sample['answer'])
        ))
    
    return verified_queries