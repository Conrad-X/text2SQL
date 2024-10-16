import json
import chromadb
import uuid

def vectorize_data_samples():
    # Load the JSON schema
    # TODO - This needs to be updated with variable paths
    with open('./data/sample_questions_and_queries/hotel_schema.json', 'r') as file:
        data = json.load(file)

    # Loop through the items
    questions = []
    answers = []
    ids = []
    for item in data:
        questions.append(item["question"])
        answers.append({ "query": item["answer"]})
        ids.append(str(uuid.uuid4()))

    # Initialize ChromaDB client
    chroma_client = chromadb.Client()
    chroma_client.reset() 
    
    # Create a collection
    collection = chroma_client.create_collection(name="unmasked_data_samples", metadata={"hnsw:space": "cosine"} )
    collection.add(
        documents=questions,
        metadatas=answers,
        ids=ids
    )

def fetch_few_shots(few_shot_count, query):
    few_shots_results = []

    # Initialize ChromaDB client
    chroma_client = chromadb.Client()
    collection = chroma_client.get_collection(name="unmasked_data_samples")

    results = collection.query(
        query_texts=[query], 
        n_results=few_shot_count
    )

    print(len(results["metadatas"][0]))

    for index, item in enumerate(results["metadatas"][0]):
        few_shots_results.append({
            "question": results["documents"][0][index],
            "answer": item["query"],
            "distance": results["distances"][0][index]
        })

    print(few_shots_results) 
    return few_shots_results
       
