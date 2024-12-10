import json
import time
from tqdm import tqdm
from utilities.constants.script_constants import (
    BatchFileStatus,
    BATCH_JOB_METADATA_DIR,
)
from utilities.batch_job import download_batch_job_output_file


def download_batch_output_files(metadata_path: str):
    """ Download all batch jobs output files from OpenAI corresponding to the given meta data file. """

    with open(metadata_path, "r") as file:
        batch_jobs = json.load(file)

    count = 0

    # Retry until all batch jobs are downloaded
    with tqdm(total=len(batch_jobs), desc="Downloading batch jobs") as progress_bar:
        progress_bar.n = sum(
            1
            for job_data in batch_jobs.values()
            if job_data["state"] == BatchFileStatus.DOWNLOADED.value
        )
        progress_bar.refresh()

        while progress_bar.n < len(batch_jobs):
            tqdm.write(f"Try: {count}")
            for batch_job_id, job_data in batch_jobs.items():
                # Skip already downloaded jobs
                if job_data["state"] == BatchFileStatus.DOWNLOADED.value:
                    continue

                try:
                    tqdm.write(f"Downloading: {batch_job_id}")
                    download_batch_job_output_file(
                        batch_job_id=batch_job_id, database_name=job_data["database"]
                    )

                    # Write back the updated status to the file immediately
                    job_data["state"] = BatchFileStatus.DOWNLOADED.value
                    with open(metadata_path, "w") as file:
                        json.dump(batch_jobs, file, indent=4)

                    progress_bar.update(1)

                except Exception as e:
                    tqdm.write(str(e))

            time.sleep(10)
            count += 1


if __name__ == "__main__":
    """
    To run this script:
    
    1. Ensure the correct metadata file for batch jobs is available:
       - The metadata file should be in the directory specified by `BATCH_JOB_METADATA_DIR` with the time stamp format `YYYY-MM-DD_HH-MM-SS.json`.

    2. Run the script:
       - In the terminal, run `python3 -m scripts.download_batch_jobs`.

    Expected Output:
       - Batch job output files will be downloaded to the appropriate directories, and the metadata file will be updated to reflect the download status.
       - If any jobs fail to download, the script will retry the download every 10 seconds.
    """
    
    # Inputs
    time_stamp = "2024-12-10_14:34:28.json"
    metadata_path = f"{BATCH_JOB_METADATA_DIR}{time_stamp}"
    
    download_batch_output_files(metadata_path)
