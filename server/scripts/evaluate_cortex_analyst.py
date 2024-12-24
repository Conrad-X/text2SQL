import json
import os
from datetime import datetime
from functools import partial
from multiprocessing import Pool, cpu_count
import sqlite3
import logging
import requests
import snowflake.connector
from tqdm import tqdm
import pandas as pd
from dotenv import load_dotenv
from utilities.config import TEST_DATA_FILE_PATH, DATASET_TYPE, DATABASE_SQLITE_PATH
from utilities.constants.script_constants import (
    GENERATE_BATCH_SCRIPT_PATH,
    FORMATTED_PRED_FILE,
    BIRD_EVAL_FOLDER,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
logging.getLogger("tqdm").setLevel(logging.WARNING)
logging.getLogger("snowflake.connector").setLevel(logging.WARNING)

load_dotenv()


def create_snowflake_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        host=os.getenv("SNOWFLAKE_HOST"),
        port=443,
        warehouse="COMPUTE_WH",
        role="ACCOUNTADMIN",
    )


def process_question(
    nlp_question, local_file_name, database_name, snowflake_connection
):
    """Sends request to Cortex Analyst and resturn it's response"""

    stage_path = (
        f"@{DATASET_TYPE.value}.{database_name.upper()}.{database_name.upper()}"
    )
    semantic_model_file = f"{stage_path}/{local_file_name}"

    # Construct the request body
    request_body = {
        "messages": [
            {"role": "user", "content": [{"type": "text", "text": nlp_question}]}
        ],
        "semantic_model_file": semantic_model_file,
    }

    # Send the request to the Snowflake API
    response = requests.post(
        url=f"https://{os.getenv('SNOWFLAKE_HOST')}/api/v2/cortex/analyst/message",
        json=request_body,
        headers={
            "Authorization": f'Snowflake Token="{snowflake_connection.rest.token}"',
            "Content-Type": "application/json",
        },
    )

    request_id = response.headers.get("X-Snowflake-Request-Id")
    if response.status_code > 400:
        raise Exception(
            f"Failed request (id: {request_id}) with status {response.status_code}: {response.text}"
        )

    response_content = response.json().get("message", {}).get("content", [])

    # Extract the SQL statement if it exists
    sql_item = next(
        (
            item
            for item in response_content
            if item.get("type") == "sql" and "statement" in item
        ),
        None,
    )
    if sql_item:
        return sql_item["statement"]

    # Extract the text response as a fallback
    text_item = next(
        (
            item
            for item in response_content
            if item.get("type") == "text" and "text" in item
        ),
        None,
    )
    if text_item:
        return text_item["text"]

    return "No valid response generated."


def upload_file_to_stage(snowflake_connection, semantic_model_file_path, database_name):
    """Uploads a local file to a specified Snowflake stage."""

    cursor = snowflake_connection.cursor()
    try:
        # Define stage name and extract file name
        stage_name = f"{DATASET_TYPE.value.upper()}.{database_name.upper()}.{database_name.upper()}"

        # Create the stage if it doesn't exist
        cursor.execute(f"CREATE STAGE IF NOT EXISTS {stage_name} ;")

        # Remove the file from the stage
        cursor.execute(f"""REMOVE @{stage_name}; """)

        # Upload the file to the stage
        cursor.execute(
            f"PUT 'file://{semantic_model_file_path}' @{stage_name} AUTO_COMPRESS=FALSE"
        )

        logger.info(f"File uploaded to stage {stage_name} successfully.")
    finally:
        cursor.close()


def generate_files(database_name: str, semantic_model_file_path: str):
    """Generates formatted files for predicted and gold SQLs."""

    snowflake_connection = create_snowflake_connection()

    upload_file_to_stage(snowflake_connection, semantic_model_file_path, database_name)

    test_file_path = TEST_DATA_FILE_PATH.format(database_name=database_name)
    with open(test_file_path, "r") as file:
        test_questions = json.load(file)

    predicted_scripts = {}
    gold_items = []

    semantic_model_file = os.path.basename(semantic_model_file_path)

    pred_path = f"{GENERATE_BATCH_SCRIPT_PATH}{database_name}/{FORMATTED_PRED_FILE}_{database_name}.json"
    gold_sql_path = f"{GENERATE_BATCH_SCRIPT_PATH}{database_name}/gold_{database_name}.sql"

    # Load intermediary results if they exist
    if os.path.exists(pred_path):
        with open(pred_path, "r") as pred_file:
            if os.path.getsize(pred_path) > 0:
                predicted_scripts = json.load(pred_file)
                if os.path.exists(gold_sql_path):
                    with open(gold_sql_path, "r") as gold_file:
                        gold_items = gold_file.readlines() if os.path.getsize(gold_sql_path) > 0 else []
            else:
                predicted_scripts = {}
                gold_items = []

    # Identify already processed question IDs
    processed_ids = set(predicted_scripts.keys())

    for item in tqdm(test_questions, desc="Predicting Queries", unit="item"):
        question_id = str(item["question_id"])

        # Skip already processed items
        if question_id in processed_ids:
            continue

        database = item["db_id"]
        try:
            prompt_sql = process_question(
                item["question"], semantic_model_file, database_name, snowflake_connection
            )

            predicted_scripts[question_id] = f"{prompt_sql}\t----- bird -----\t{database}"
            gold_items.append(f"{item['SQL']}\t{database}\n")

            # Save intermediary results after processing each item
            with open(pred_path, "w") as pred_file:
                json.dump(predicted_scripts, pred_file, indent=4)

            with open(gold_sql_path, "w") as gold_file:
                gold_file.writelines(gold_items)

        except Exception as e:
            logger.error(f"Error processing question ID {question_id}: {e}")
            continue

    logger.info("Generated files successfully.")
    return pred_path, gold_sql_path, test_questions


def compare_results(query_pair, database_name):
    """Compares predicted SQL results with ground truth."""

    predicted_sql, ground_truth, idx = query_pair

    snowflake_connection = create_snowflake_connection()
    snowflake_cursor = snowflake_connection.cursor()

    sqlite_connection = sqlite3.connect(
        DATABASE_SQLITE_PATH.format(database_name=database_name)
    )
    sqlite_cursor = sqlite_connection.cursor()

    try:
        pred_result = snowflake_cursor.execute(predicted_sql).fetchall()
        truth_result = sqlite_cursor.execute(ground_truth).fetchall()

        match = 1 if set(pred_result) == set(truth_result) else 0
        return {"sql_idx": idx, "res": match, "error": None}
    except Exception as e:
        print(f"Error processing query pair {idx}: {e}")
        return {
            "sql_idx": idx,
            "res": 0,
            "error": {"query": predicted_sql, "message": str(e)},
        }
    finally:
        snowflake_connection.close()
        sqlite_connection.close()


def process_queries_parallel(pred_sqls, gold_sqls, database_name):
    """Processes SQL queries in parallel for evaluation."""

    query_pairs = [(pred_sqls[i], gold_sqls[i], i) for i in range(len(pred_sqls))]

    compare_results_with_db = partial(compare_results, database_name=database_name)
    results = []
    
    with Pool(cpu_count()) as pool:
        results = list(tqdm(pool.imap(compare_results_with_db, query_pairs), desc="Evaluating Queries", total=len(query_pairs)))
    
    return results


def calculate_accuracy(results):
    """Calculates the accuracy of predicted SQL results."""

    correct = sum(res["res"] == 1 for res in results)
    mismatched = sum(res["res"] == 0 and res["error"] is None for res in results)
    errors = sum(res["res"] == 0 and res["error"] is not None for res in results)

    total = len(results)
    accuracy = (correct / total) * 100 if total > 0 else 0
    return accuracy, mismatched, errors


def package_sqls(filepath, is_json=True):
    """Packages SQLs from a file into a list."""

    with open(filepath, "r") as file:
        if is_json:
            return list(json.load(file).values())
        return [line.split("\t")[0].strip() for line in file.readlines()]


if __name__ == "__main__":
    """
    To run this script:

    1. Set up environment variables:
        - Ensure the following environment variables are defined in the `.env` file:
            - SNOWFLAKE_USER
            - SNOWFLAKE_PASSWORD
            - SNOWFLAKE_ACCOUNT
            - SNOWFLAKE_HOST

    2. Prepare the necessary files:
        - Ensure that the database is correctly migrated to Snowflake, and the corresponding tables and columns exist in both Snowflake and SQLite.
        - Make sure that the table and column names in Snowflake are capitalized, as Snowflake expects them to be in uppercase.
        - Set the correct `DATASET_TYPE` in `utilities.config` to the appropriate dataset type (e.g., `DatasetType.BIRD_TRAIN` for training data).

    3. Run the script:
        - Navigate to the project folder and run the script using:
            python3 -m script.evaluate_cortex_analyst

    4. Expected Output:
        - The script will log the accuracy of the predicted SQL queries compared to the ground truth.
        - A summary file containing the results will be saved to `BIRD_EVAL_FOLDER`, including accuracy, mismatched queries, and error details.

    Other info:
        - The comparisons between predicted and ground truth SQLs are performed using SQLite for ground truth and Snowflake for predicted results.
    """

    # Inputs
    database_name = "retails"
    semantic_model_file_path = "./RETAILS.yaml"

    # Generate files
    pred_path, gold_path, test_questions = generate_files(database_name, semantic_model_file_path)

    # Load queries
    pred_queries = package_sqls(pred_path, is_json=True)
    gold_queries = package_sqls(gold_path, is_json=False)

    if len(pred_queries) != len(gold_queries):
        logger.error(
            "Mismatched query counts between predicted and ground truth."
        )

    # Run evaluation
    query_results = process_queries_parallel(pred_queries, gold_queries, database_name)
    accuracy, mismatched, errors = calculate_accuracy(query_results)

    logger.info(f"Accuracy: {accuracy:.2f}%")
    logger.info(f"Mismatched Queries: {mismatched}")
    logger.info(f"Total Errors: {errors}")

    # Save the results
    timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    os.makedirs(BIRD_EVAL_FOLDER, exist_ok=True)

    error_details = {
        result["sql_idx"]: result["error"]
        for result in query_results
        if result["res"] == 0 and result["error"] is not None
    }

    file_path = os.path.join(BIRD_EVAL_FOLDER, f"cortex_analyst_{timestamp}.txt")
    with open(file_path, "w") as file:
        file.write(f"Database: {database_name}\n")
        file.write(f"Accuracy: {accuracy:.2f}%\n")
        file.write(f"Mismatched Queries: {mismatched}\n")
        file.write(f"Total Errors: {errors}\n")

        file.write("\nErrors:\n")
        for sql_idx, error in error_details.items():
            file.write(f"SQL Index: {sql_idx}\n")
            file.write(f"Error Details: {error}\n")
            file.write("\n")

    logger.info(f"Results saved to: {file_path}")
    
    # Check and empty formatted file if condition matches
    if len(test_questions) == len(pred_queries):
        logger.info("Test data length matches the formatted queries length. Emptying the formatted file.")
        with open(pred_path, "w") as pred_file:
            json.dump({}, pred_file)
        with open(gold_path, "w") as gold_file:
            gold_file.write("")
    else:
        logger.warning("Mismatch between test data length and formatted queries length. Check the evaluation run, something is not right.")
