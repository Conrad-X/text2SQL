import json
import time

from tqdm import tqdm
from utilities.batch_job import download_batch_job_output_file
from utilities.config import PATH_CONFIG
from utilities.constants.script_constants import BatchFileStatus


def download_batch_output_files(metadata_path: str):
    """Download all batch jobs output files from OpenAI corresponding to the given meta data file."""

    with open(metadata_path, "r") as file:
        metadata = json.load(file)

    count = 0
    batch_jobs = metadata.get("databases", {})

    # Retry until all batch jobs are downloaded
    with tqdm(total=len(batch_jobs), desc="Downloading batch jobs") as progress_bar:
        progress_bar.n = sum(
            1
            for batch_job_data in batch_jobs.values()
            if batch_job_data["state"] == BatchFileStatus.DOWNLOADED.value
        )
        progress_bar.refresh()

        while progress_bar.n < len(batch_jobs):
            tqdm.write(f"Try: {count}")
            for database, batch_job_data in batch_jobs.items():
                # Skip already downloaded jobs
                if batch_job_data["state"] == BatchFileStatus.DOWNLOADED.value:
                    continue

                try:
                    tqdm.write(f"Downloading: {batch_job_data['batch_job_id']}")
                    download_batch_job_output_file(
                        batch_job_id=batch_job_data["batch_job_id"],
                        download_file_path=PATH_CONFIG.batch_output_path(database_name=database),
                    )

                    # Update the state to downloaded
                    batch_job_data["state"] = BatchFileStatus.DOWNLOADED.value
                    metadata["databases"][database] = batch_job_data
                    with open(metadata_path, "w") as file:
                        json.dump(metadata, file, indent=4)

                    progress_bar.update(1)

                except Exception as e:
                    tqdm.write(str(e))

            if progress_bar.n < len(batch_jobs):
                time.sleep(10)  # retry after 10 secs

            count += 1


if __name__ == "__main__":
    """
    To run this script:

    1. Ensure the correct metadata file for batch jobs is available:
       - The metadata file should be in the directory specified by `PATH_CONFIG.batch_job_metadata_dir()` with the time stamp format `YYYY-MM-DD_HH-MM-SS.json`.

    2. Run the script:
       - In the terminal, run `python3 -m scripts.download_batch_jobs`.

    Expected Output:
       - Batch job output files will be downloaded to the appropriate directories, and the metadata file will be updated to reflect the download status.
       - If any jobs fail to download, the script will retry the download every 10 seconds.
    """

    # Inputs
    time_stamp = "2024-12-11_17:01:02.json"
    metadata_path = f"{PATH_CONFIG.batch_job_metadata_dir}/{time_stamp}"

    download_batch_output_files(metadata_path)
