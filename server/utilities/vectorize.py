import json
import os
import pandas as pd
from chromadb.errors import InvalidCollectionException
import sqlite3
import uuid
import math
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

from utilities.config import PATH_CONFIG, ChromadbClient
from utilities.utility_functions import get_table_names
from utilities.logging_utils import setup_logger

logger = setup_logger(__name__)

# Constants
MAX_WORKER = 10
BATCH_SIZE = 100


def vectorize_data(documents, metadatas, ids, collection_name, space="cosine"):
    """
    Vectorizes the documents and adds them to the collection
    """
    chroma_client = ChromadbClient.CHROMADB_CLIENT
    collection = chroma_client.create_collection(
        name=collection_name, metadata={"hnsw:space": space}
    )

    batch_size = min(BATCH_SIZE, len(documents))  
    total_docs = len(documents)
    num_batches = math.ceil(total_docs / batch_size)
    
    def add_batch(i):
        start = i * batch_size
        end = min(start + batch_size, total_docs)

        batch_docs = documents[start:end]
        batch_metadatas = metadatas[start:end]
        batch_ids = ids[start:end]
        
        collection.add(documents=batch_docs, metadatas=batch_metadatas, ids=batch_ids)

    max_workers = min(MAX_WORKER, num_batches)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(add_batch, i): i for i in range(num_batches)}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Adding batches in parallel"):
            future.result()


def get_sample_questions(sample_questions_path):
    """
    Returns the question as documents, answers and question ids as metadatas from the sample questions file
    """
    with open(sample_questions_path, "r") as file:
        data = json.load(file)

    documents = [item["question"] for item in data]
    metadatas = [{"query": item["SQL"], "question_id": item["question_id"], "db_id": item["db_id"], "schema_used": json.dumps(item['schema_used']), "evidence":item['evidence']} for item in data]
    ids = [str(uuid.uuid4()) for _ in data]

    return documents, metadatas, ids


def make_samples_collection():
    """
    Creates vector database of sample questions
    """

    chroma_client = ChromadbClient.CHROMADB_CLIENT
    collection_name  = ""

    # Check if collection already exists 
    if PATH_CONFIG.dataset_dir != PATH_CONFIG.sample_dataset_type:
        collection_name = "unmasked_data_samples"

    elif PATH_CONFIG.dataset_dir == PATH_CONFIG.sample_dataset_type:
        database_name = PATH_CONFIG.database_name
        collection_name = f"{database_name}_unmasked_data_samples"

    try:
        # Check if collection already exists 
        collection = chroma_client.get_collection(name=collection_name)

    except InvalidCollectionException:
        documents, metadatas, ids = get_sample_questions(
            PATH_CONFIG.processed_train_path()
        )
        vectorize_data(
            documents,
            metadatas,
            ids,
            collection_name,
            space="cosine",
        )
        collection = chroma_client.get_collection(name=collection_name)

    return collection



def get_database_schema(sqlite_database_path, database_description_dir):
    """
    Returns the documents, metadatas, and ids that have column names and decriptions
    This will only work with BIRD Datasets as we only have descriptions for BIRD
    """

    connection = sqlite3.connect(sqlite_database_path)
    tables = get_table_names(connection)

    documents, metadatas, ids = [], [], []

    for table_csv in os.listdir(database_description_dir):
        table_name = os.path.splitext(table_csv)[0]
        if table_name in tables:
            table_column_df = pd.read_csv(
                os.path.join(database_description_dir, table_csv)
            )
            for _, row in table_column_df.iterrows():
                documents.append(row["improved_column_description"])
                metadatas.append(
                    {"table": table_name, "name": row["original_column_name"]}
                )
                ids.append(str(uuid.uuid4()))

    connection.close()
    return documents, metadatas, ids


def fetch_few_shots(few_shot_count: int, query: str):
    """
    Fetches similar sample quries for the given query
    """
    few_shots_results = []

    # Initialize ChromaDB Collection
    collection = make_samples_collection()

    # Query the collection
    results = collection.query(query_texts=[query], n_results=few_shot_count + 1)

    for index, item in enumerate(results["metadatas"][0]):
        if not results["documents"][0][index] == query:
            few_shots_results.append(
                {
                    "question": results["documents"][0][index],
                    "answer": item["query"],
                    "question_id": item["question_id"],
                    "db_id": item["db_id"],
                    "distance": results["distances"][0][index],
                    "schema_used": item["schema_used"],
                    "evidence":item["evidence"],
                }
            )

    return few_shots_results[:few_shot_count]


def make_column_description_collection():
    """
    Creates vector database of column descriptions of the current database
    """
    chroma_client = ChromadbClient.CHROMADB_CLIENT
    database_name = PATH_CONFIG.database_name
    
    try:
        # Check if collection already exists 
        collection = chroma_client.get_collection(name=f"{database_name}_column_descriptions")
        
    except InvalidCollectionException:
        documents, metadatas, ids = get_database_schema(
            PATH_CONFIG.sqlite_path(database_name=database_name),
            PATH_CONFIG.description_dir(database_name=database_name),
        )

        # Vectorize the data
        vectorize_data(
            documents,
            metadatas,
            ids,
            f"{database_name}_column_descriptions",
            space="cosine",
        )
        collection = chroma_client.get_collection(name=f"{database_name}_column_descriptions")

    return collection


def fetch_similar_columns(
    n_results: int,
    keywords: list,
    database_name: str = None,
):
    """
    Fetches similar columns that the given keyword might be related to
    """

    if not database_name:
        database_name = PATH_CONFIG.database_name

    schema = {}

    # Initialize ChromaDB Collection
    collection = make_column_description_collection()

    # Query the collection
    for keyword in keywords:
        result = collection.query(query_texts=[keyword], n_results=n_results + 1)

        for item in result["metadatas"][0]:
            if item["table"] not in schema:
                schema[item["table"]] = []
            schema[item["table"]].append(item["name"])

    # Optionally limit the number of results per table if desired
    return dict(list(schema.items())[:n_results])
