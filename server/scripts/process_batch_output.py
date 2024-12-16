import json
import sqlite3
from typing import List, Dict
from tqdm import tqdm
from utilities.utility_functions import execute_sql_query
from utilities.config import (
    BATCH_OUTPUT_FILE_PATH,
    DATABASE_SQLITE_PATH,
    JUDGE_BATCH_OUTPUT_FILE_PATH,
)
from utilities.constants.script_constants import (
    GENERATE_BATCH_SCRIPT_PATH,
    FORMATTED_PRED_FILE,
    BATCH_JOB_METADATA_DIR,
    BatchFileStatus,
)


def calculate_score(prediction: Dict, judge_data: List[Dict]) -> int:
    """Calculates the score for a given prediction using judge data."""

    score = 0
    for judge in judge_data:
        if judge["custom_id"] == prediction["custom_id"]:
            for choice in judge["response"]["body"]["choices"]:
                try:
                    parsed_content = json.loads(choice["message"]["content"])
                    score += sum(
                        1 for value in parsed_content.values() if value is True
                    )
                except json.JSONDecodeError:
                    print(f"Invalid JSON content: {choice['message']['content']}")
                break
    return score


def validate_sql_execution(sql: str, database: str) -> int:
    """Validates the SQL execution and assigns additional score for successful queries."""

    connection = sqlite3.connect(DATABASE_SQLITE_PATH.format(database_name=database))
    try:
        execute_sql_query(connection, sql)
        return 5
    except RuntimeError as e:
        print(f"SQL execution error: {str(e)}")
        return 0
    finally:
        connection.close()


def process_single_candidate(
    database: str, batch_output_data: List[Dict], test_data: List[Dict]
) -> Dict:
    """Processes a single candidate without scoring or grouping."""

    predicted_scripts = {}
    gold_items = []

    for prediction in batch_output_data:
        id = int(prediction["custom_id"].split("-")[-1])
        sql = prediction["response"]["body"]["choices"][0]["message"]["content"]
        if sql.startswith("```sql") and sql.endswith("```"):
            sql = sql.strip("```sql\n").strip("```")

        predicted_scripts[int(id)] = f"{sql}\t----- bird -----\t{database}"

        for item in test_data:
            if int(id) == item["question_id"]:
                gt_sql = item["SQL"]
                gold_items.append(f"{gt_sql}\t{database}")

    return predicted_scripts, gold_items


def process_candidates_with_scoring(
    database: str,
    batch_output_data: List[Dict],
    test_data: List[Dict],
    judge_data: List[Dict],
) -> Dict:
    """Groups candidates, calculates scores, and selects the highest-scoring candidate."""

    grouped_candidates = {}
    predicted_scripts = {}
    gold_items = []

    for prediction in batch_output_data:
        custom_id = prediction["custom_id"]
        custom_id_number = int(custom_id.split("-")[-1])

        if custom_id_number not in grouped_candidates:
            grouped_candidates[custom_id_number] = []

        sql = prediction["response"]["body"]["choices"][0]["message"]["content"]
        if sql.startswith("```sql") and sql.endswith("```"):
            sql = sql.strip("```sql\n").strip("```")

        score = calculate_score(prediction, judge_data)
        score += validate_sql_execution(sql, database)

        grouped_candidates[custom_id_number].append(
            {"custom_id": custom_id, "sql": sql, "score": score}
        )

    for question_id, candidates in grouped_candidates.items():
        chosen_candidate = max(candidates, key=lambda x: x["score"])

        pred_sql = chosen_candidate["sql"]
        predicted_scripts[question_id] = f"{pred_sql}\t----- bird -----\t{database}"

        for item in test_data:
            if question_id == item["question_id"]:
                gold_items.append(f"{item['SQL']}\t{database}")

    return predicted_scripts, gold_items


def format_batch_output_files(metadata_path: str) -> None:
    """Processes and formats batch output files based on metadata."""

    with open(metadata_path, "r") as file:
        metadata = json.load(file)

    # Check if we need to run this script
    status_actions = {
        BatchFileStatus.DOWNLOADED.value: (
            "Judging required: Multiple candidates are available. Run 'download_batch_output' to proceed with judging."
            if len(metadata["batch_info"]["candidates"]) > 1
            else None
        ),
        BatchFileStatus.JUDGING_CANDIDATES.value: (
            "Judged batch output pending: Download the judged batch output by running 'download_batch_output'."
        ),
        BatchFileStatus.FORMATTED_PRED_FILE.value: (
            "Processing complete: Files are already formatted. Run 'bird_eval' for evaluation."
        ),
    }
    
    action_message = status_actions.get(metadata["batch_info"]["overall_status"])
    if action_message:
        print(f"Status: {metadata['batch_info']['overall_status']}\nAction Required: {action_message}")
        return


    batch_jobs = metadata.get("databases", {})
    formatted_count = 0

    for database, batch_job_data in tqdm(
        batch_jobs.items(), desc="Formatting batch output files"
    ):
        print(f"Processing database: {database}")
        if batch_job_data["state"] == BatchFileStatus.FORMATTED_PRED_FILE.value:
            formatted_count += 1
            continue

        batch_output_path = BATCH_OUTPUT_FILE_PATH.format(database_name=database)
        with open(batch_output_path, "r") as file:
            batch_output_data = [json.loads(line) for line in file]

        test_file_path = f"{GENERATE_BATCH_SCRIPT_PATH}{database}/test_{database}.json"
        with open(test_file_path, "r") as file:
            test_data = json.loads(file.read())

        if (
            batch_job_data["state"] == BatchFileStatus.DOWNLOADED.value
            and len(metadata["batch_info"]["candidates"]) == 1
        ):
            predicted_scripts, gold_items = process_single_candidate(
                database, batch_output_data, test_data
            )
        elif batch_job_data["state"] == BatchFileStatus.JUDGED_AND_DOWNLOADED.value:
            with open(
                JUDGE_BATCH_OUTPUT_FILE_PATH.format(database_name=database), "r"
            ) as file:
                judge_data = [json.loads(line) for line in file]
            predicted_scripts, gold_items = process_candidates_with_scoring(
                database, batch_output_data, test_data, judge_data
            )

        formatted_pred_path = f"{GENERATE_BATCH_SCRIPT_PATH}{database}/{FORMATTED_PRED_FILE}_{database}.json"
        with open(formatted_pred_path, "w") as file:
            json.dump(predicted_scripts, file)

        gold_sql_path = f"{GENERATE_BATCH_SCRIPT_PATH}{database}/gold_{database}.sql"
        with open(gold_sql_path, "w") as file:
            for line in gold_items:
                file.write(f"{line}\n")

        batch_job_data["state"] = BatchFileStatus.FORMATTED_PRED_FILE.value
        metadata["databases"][database] = batch_job_data

        with open(metadata_path, "w") as file:
            json.dump(metadata, file, indent=4)

        print(f"Formatted database: {database}")
        formatted_count += 1

    if formatted_count == len(batch_jobs):
        metadata["batch_info"][
            "overall_status"
        ] = BatchFileStatus.FORMATTED_PRED_FILE.value
        with open(metadata_path, "w") as file:
            json.dump(metadata, file, indent=4)


if __name__ == "__main__":
    """
    To run this script:

    1. Ensure that batch output files and test files are available:
       - The batch output files should be in the directory specified by `GENERATE_BATCH_SCRIPT_PATH` for each database.
       - Test files for each database should be located in the same directory with the filename format `test_{database}.json`.
       - The batch job metadata file should be present in the directory specified by `BATCH_JOB_METADATA_DIR` with the time stamp format `YYYY-MM-DD_HH-MM-SS.json`.

    2. Run the script:
       - In the terminal, run `python3 -m scripts.process_batch_output`.
       - The script will create two files for each database:
         - A JSON file with the formatted predictions (`FORMATTED_PRED_FILE_{database}.json`).
         - A SQL file with the gold queries (`gold_{database}.sql`).

    Expected Output:
       - The metadata file will be updated with the new state for each batch job, marking it as formatted.
       - The script will skip any jobs that are already formatted or not yet downloaded.
    """

    # Inputs
    time_stamp = "2024-12-16_12:04:34.json"
    metadata_path = f"{BATCH_JOB_METADATA_DIR}{time_stamp}"

    format_batch_output_files(metadata_path)
