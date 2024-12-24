import json
import time
from tqdm import tqdm
from utilities.judge_llm import generate_judge_batch_input
from utilities.constants.LLM_enums import ModelType
from utilities.constants.script_constants import (
    BatchFileStatus,
    BATCH_JOB_METADATA_DIR,
)
from utilities.config import (
    BATCH_OUTPUT_FILE_PATH,
    TEST_DATA_FILE_PATH,
    JUDGE_BATCH_OUTPUT_FILE_PATH,
    JUDGE_BATCH_INPUT_FILE_PATH,
)
from utilities.batch_job import download_batch_job_output_file, upload_and_run_batch_job


def generate_judge_batch_input_files(batch_jobs, metadata, metadata_path):
    """Generate input files for judge batch jobs, update metadata, and upload batch jobs."""
    
    judging_candidates_count = 0
    for database_name, batch_job_data in tqdm(
        batch_jobs.items(), desc="Generating judge batch input files"
    ):
        # Read batch output
        with open(BATCH_OUTPUT_FILE_PATH.format(database_name=database_name), "r") as f:
            batch_output = [json.loads(line) for line in f]

        if batch_job_data["state"] == BatchFileStatus.JUDGING_CANDIDATES.value:
            judging_candidates_count += 1
            continue

        # Read test data
        with open(BATCH_OUTPUT_FILE_PATH.format(database_name=database_name), "r") as f:
            batch_output = [json.loads(line) for line in f]

        test_data = []
        with open(TEST_DATA_FILE_PATH.format(database_name=database_name), "r") as f:
            test_data = json.load(f)

        # Generate judge batch input items
        judge_batch_input = generate_judge_batch_input(
            batch_output=batch_output,
            test_data=test_data,
            model=ModelType.OPENAI_GPT4_O_MINI.value,
        )

        with open(
            JUDGE_BATCH_INPUT_FILE_PATH.format(database_name=database_name), "w"
        ) as f:
            for item in judge_batch_input:
                json.dump(item, f)
                f.write("\n")

        # Upload and run Judge batch input file
        _, batch_job_id = upload_and_run_batch_job(
            upload_file_path=JUDGE_BATCH_INPUT_FILE_PATH.format(
                database_name=database_name
            ),
        )

        batch_job_data["judge_batch_job_id"] = batch_job_id
        batch_job_data["state"] = BatchFileStatus.JUDGING_CANDIDATES.value

        # Update metadata
        metadata["databases"][database_name] = batch_job_data
        with open(metadata_path, "w") as file:
            json.dump(metadata, file, indent=4)
            
        judging_candidates_count += 1

    # Update overall status if all are judging candidates
    if judging_candidates_count == len(batch_jobs):
        metadata["batch_info"][
            "overall_status"
        ] = BatchFileStatus.JUDGING_CANDIDATES.value
        with open(metadata_path, "w") as file:
            json.dump(metadata, file, indent=4)


def download_batch_output_files(
    metadata_path: str, batch_output_path: str, judge_batch_output_path: str
):
    """Download all batch jobs output files from OpenAI corresponding to the given meta data file."""

    with open(metadata_path, "r") as file:
        metadata = json.load(file)
        
    batch_jobs = metadata.get("databases", {})

    file_path_format = ""
    batch_job_id = ""
    nextStatus = ""
    
    if not metadata["batch_info"]["overall_status"] == BatchFileStatus.DOWNLOADED.value:

        if metadata["batch_info"]["overall_status"] == BatchFileStatus.UPLOADED.value:
            file_path_format = batch_output_path
            batch_job_id = "batch_job_id"
            nextStatus = BatchFileStatus.DOWNLOADED.value

        elif (
            metadata["batch_info"]["overall_status"]
            == BatchFileStatus.JUDGING_CANDIDATES.value
        ):
            file_path_format = judge_batch_output_path
            batch_job_id = "judge_batch_job_id"
            nextStatus = BatchFileStatus.JUDGED_AND_DOWNLOADED.value

        # Retry until all batch jobs are downloaded
        with tqdm(total=len(batch_jobs), desc="Downloading batch jobs") as progress_bar:
            progress_bar.n = sum(
                1
                for batch_job_data in batch_jobs.values()
                if batch_job_data["state"] == nextStatus
            )
            progress_bar.refresh()

            retry_count = 0
            while progress_bar.n < len(batch_jobs):
                tqdm.write(f"Try: {retry_count}")
                for database, batch_job_data in batch_jobs.items():
                    # Skip already downloaded jobs
                    if batch_job_data["state"] == nextStatus:
                        continue

                    try:
                        tqdm.write(f"Downloading: {batch_job_data[batch_job_id]}")
                        download_batch_job_output_file(
                            batch_job_id=batch_job_data[batch_job_id],
                            download_file_path=file_path_format.format(
                                database_name=database
                            ),
                        )

                        # Update the state to downloaded
                        batch_job_data["state"] = nextStatus
                        metadata["databases"][database] = batch_job_data
                        with open(metadata_path, "w") as file:
                            json.dump(metadata, file, indent=4)
                        
                        progress_bar.update(1)

                    except Exception as e:
                        tqdm.write(str(e))

                if progress_bar.n < len(batch_jobs):
                    time.sleep(10)  # retry after 10 secs

                retry_count += 1

            if progress_bar.n == len(batch_jobs):
                metadata["batch_info"]["overall_status"] = nextStatus
                with open(metadata_path, "w") as file:
                    json.dump(metadata, file, indent=4)

    # If there is more than one candidate, we need to generate judge batch input files
    if (
        len(metadata["batch_info"]["candidates"]) > 1
        and metadata["batch_info"]["overall_status"] == BatchFileStatus.DOWNLOADED.value
    ):
        generate_judge_batch_input_files(batch_jobs, metadata, metadata_path)


if __name__ == "__main__":
    """
    To run this script:

    1. Ensure the correct metadata file for batch jobs is available:
       - The metadata file should be in the directory specified by `BATCH_JOB_METADATA_DIR` with the time stamp format `YYYY-MM-DD_HH-MM-SS.json`.

    2. Run the script:
       - In the terminal, run `python3 -m scripts.download_batch_jobs`.

    Batch Job States and Processing:
        - UPLOADED:
            - Downloads batch output files for each database.
            - If multiple candidates are available, generates judge batch input files and uploads them as judge batch jobs.

        - JUDGING_CANDIDATES:
            - Downloads judge batch output files if judge batch jobs have been initiated.

        - JUDGED_AND_DOWNLOADED
            - Does not do anything.

    Expected Output:
        - Batch output files are downloaded, judge batch input files are generated if needed, and metadata is updated.
        - Retries every 10 seconds for failed downloads.
        - Downloads judge batch output files if available.
    """

    # Inputs
    time_stamp = "2024-12-16_12:04:34.json"
    metadata_path = f"{BATCH_JOB_METADATA_DIR}{time_stamp}"

    download_batch_output_files(
        metadata_path, BATCH_OUTPUT_FILE_PATH, JUDGE_BATCH_OUTPUT_FILE_PATH
    )
