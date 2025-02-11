import json
import os

import pandas as pd
from chromadb.errors import InvalidCollectionException
import sqlite3
import uuid

from utilities.config import (
    DatabaseConfig,
    ChromadbClient,
    UNMASKED_SAMPLE_DATA_FILE_PATH,
    DATABASE_SQLITE_PATH,
    DATASET_DESCRIPTION_PATH,
)
from utilities.utility_functions import get_table_names


def vectorize_data(documents, metadatas, ids, collection_name, space="cosine"):
    """
    Vectorizes the documents and adds them to the collection
    """
    chroma_client = ChromadbClient.CHROMADB_CLIENT
    collection = chroma_client.create_collection(
        name=collection_name, metadata={"hnsw:space": space}
    )

    collection.add(documents=documents, metadatas=metadatas, ids=ids)


def get_sample_questions(sample_questions_path):
    """
    Returns the question as documents, answers and question ids as metadatas from the sample questions file
    """
    with open(sample_questions_path, "r") as file:
        data = json.load(file)

    documents = [item["question"] for item in data]
    metadatas = [{"query": item["answer"], "question_id": item["id"]} for item in data]
    ids = [str(uuid.uuid4()) for _ in data]

    return documents, metadatas, ids

def make_samples_collection():
    database_name = DatabaseConfig.ACTIVE_DATABASE
    chroma_client = ChromadbClient.CHROMADB_CLIENT
    chroma_client.reset()
    documents, metadatas, ids = get_sample_questions(
            UNMASKED_SAMPLE_DATA_FILE_PATH.format(database_name=database_name)
    )

    vectorize_data(
        documents,
        metadatas,
        ids,
        f"{database_name}_unmasked_data_samples",
        space="cosine",
    )

def get_database_schema(sqlite_database_path, database_description_path):
    """
    Returns the documents, metadatas, and ids that have column names and decriptions
    This will only work with BIRD Datasets as we only have descriptions for BIRD
    """

    connection = sqlite3.connect(sqlite_database_path)
    tables = get_table_names(connection)

    documents, metadatas, ids = [], [], []

    for table_csv in os.listdir(database_description_path):
        table_name = os.path.splitext(table_csv)[0]
        if table_name in tables:
            table_column_df = pd.read_csv(
                os.path.join(database_description_path, table_csv)
            )
            for _, row in table_column_df.iterrows():
                documents.append(row["improved_column_description"])
                metadatas.append(
                    {"table": table_name, "name": row["original_column_name"]}
                )
                ids.append(str(uuid.uuid4()))

    connection.close()
    return documents, metadatas, ids


def fetch_few_shots(
    few_shot_count: int, query: str, database_name: str = None
):
    """
    Fetches similar sample quries for the given query
    """
    if not database_name:
        database_name = DatabaseConfig.ACTIVE_DATABASE

    few_shots_results = []

    # Initialize ChromaDB client
    chroma_client = ChromadbClient.CHROMADB_CLIENT
    try:
        collection = chroma_client.get_collection(
            name=f"{database_name}_unmasked_data_samples"
        )
    except InvalidCollectionException:
        # Reset the client if the collection does not exist
        chroma_client.reset()

        # Get the sample questions as documents and answers/gold sql as metadata
        documents, metadatas, ids = get_sample_questions(
            UNMASKED_SAMPLE_DATA_FILE_PATH.format(database_name=database_name)
        )

        # Vectorize the data
        vectorize_data(
            documents,
            metadatas,
            ids,
            f"{database_name}_unmasked_data_samples",
            space="cosine",
        )

        # Get the collection
        collection = chroma_client.get_collection(
            name=f"{database_name}_unmasked_data_samples"
        )

    # Query the collection
    results = collection.query(query_texts=[query], n_results=few_shot_count + 1)

    for index, item in enumerate(results["metadatas"][0]):
        if not results["documents"][0][index] == query:
            few_shots_results.append(
                {
                    "question": results["documents"][0][index],
                    "answer": item["query"],
                    "question_id": item["question_id"],
                    "distance": results["distances"][0][index],
                }
            )

    return few_shots_results[:few_shot_count]


def fetch_similar_columns(
    n_results: int,
    keywords: list,
    database_name: str = None,
):
    """
    Fetches similar columns that the given keyword might be related to
    """

    if not database_name:
        database_name = DatabaseConfig.ACTIVE_DATABASE
        
    schema = {}

    # Initialize ChromaDB client
    chroma_client = ChromadbClient.CHROMADB_CLIENT
    try:
        collection = chroma_client.get_collection(
            name=f"{database_name}_column_descriptions"
        )
    except InvalidCollectionException:
        # Reset the client if the collection does not exist
        chroma_client.reset()

        # Get the column descriptions as documents and schema as metadata
        documents, metadatas, ids = get_database_schema(
            DATABASE_SQLITE_PATH.format(database_name=database_name),
            DATASET_DESCRIPTION_PATH.format(database_name=database_name),
        )

        # Vectorize the data
        vectorize_data(
            documents,
            metadatas,
            ids,
            f"{database_name}_column_descriptions",
            space="cosine",
        )

        # Get the collection
        collection = chroma_client.get_collection(
            name=f"{database_name}_column_descriptions"
        )

    # Query the collection
    for keyword in keywords:
        result = collection.query(query_texts=[keyword], n_results=n_results + 1)

        for item in result["metadatas"][0]:
            if item["table"] not in schema:
                schema[item["table"]] = []
            schema[item["table"]].append(item["name"])

    # Optionally limit the number of results per table if desired
    return dict(list(schema.items())[:n_results])
