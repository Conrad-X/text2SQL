import json
from tqdm import tqdm
from utilities.config import PATH_CONFIG
from utilities.constants.script_constants import BatchFileStatus


def format_batch_output(database, batch_output_data, test_data):
    """ Formats the batch output for BIRD evaluation. """
    
    predicted_scripts = {}
    gold_items = [] 
    grouped_candidates = {}
    
    # Group candidates by question_id
    for prediction in batch_output_data:
        custom_id = prediction['custom_id'][8:]
        custom_id_number = int(custom_id.split('-')[-1])
        
        if custom_id_number not in grouped_candidates:
            grouped_candidates[custom_id_number] = []

        grouped_candidates[custom_id_number].append(prediction)

    for question_id, candidates in grouped_candidates.items():
        # TODO: Implement the judge LLM logic to select the best candidate based on some criteria.
        chosen_candidate = candidates[0] 
        
        # Get the SQL prediction from the chosen candidate
        pred_sql = chosen_candidate['response']['body']['choices'][0]['message']['content']

        # Clean up SQL predictions
        if pred_sql.startswith('```sql') and pred_sql.endswith('```'):
            pred_sql = pred_sql.strip('```sql\n').strip('```')

        predicted_scripts[question_id] = f'{pred_sql}\t----- bird -----\t{database}'

        # Match gold query for the prediction
        for item in test_data:
            if question_id == item['question_id']:
                gt_sql = item['SQL']
                gold_items.append(f'{gt_sql}\t{database}')
                
    formatted_pred_path = PATH_CONFIG.formatted_predictions_path(database_name=database)
    with open(formatted_pred_path, 'w') as file:
        json.dump(predicted_scripts, file)

    gold_sql_path = PATH_CONFIG.test_gold_path(database_name=database)
    with open(gold_sql_path, 'w') as file:
        for item in gold_items:
            file.write(f'{item}\n')
            

def format_batch_output_files(metadata_path: str):
        
    # Load batch job metadata
    with open(metadata_path, "r") as file:
        metadata = json.load(file)

    batch_jobs = metadata.get("databases", {})

    for database, batch_job_data in tqdm(batch_jobs.items(), desc="Formatting batch output files"):
        if batch_job_data["state"] == BatchFileStatus.FORMATTED_PRED_FILE.value:
            continue
        
        if batch_job_data["state"] != BatchFileStatus.DOWNLOADED.value:
            tqdm.write(f"Skipping {batch_job_data['database']} because is it not downloaded")
            continue
        
        batch_output_path = PATH_CONFIG.batch_output_path(database_name=database)

        # Load batch output
        with open(batch_output_path, 'r') as file:
            batch_output_data = [json.loads(line) for line in file]

        # Load test file
        test_file_path = PATH_CONFIG.processed_test_path(database_name=database)
        with open(test_file_path, 'r') as file:
            test_data = json.loads(file.read())

        # Format batch output and generate gold queries
        format_batch_output(database, batch_output_data, test_data)

        # Update the state to formatted
        batch_job_data["state"] = BatchFileStatus.FORMATTED_PRED_FILE.value
        metadata["databases"][database] = batch_job_data
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
       - In the terminal, run `python3 -m scripts.format_batch_output`.
       - The script will create two files for each database:
         - A JSON file with the formatted predictions (`FORMATTED_PRED_FILE_{database}.json`).
         - A SQL file with the gold queries (`gold_{database}.sql`).

    Expected Output:
       - The metadata file will be updated with the new state for each batch job, marking it as formatted.
       - The script will skip any jobs that are already formatted or not yet downloaded.
    """
    # Inputs
    time_stamp = "2024-12-11_17:01:02.json"
    metadata_path = f"{PATH_CONFIG.batch_job_metadata_dir()}/{time_stamp}"
    
    format_batch_output_files(metadata_path)
