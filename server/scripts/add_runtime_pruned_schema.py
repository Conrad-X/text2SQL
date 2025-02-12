
from collections import defaultdict
import json
import os
from concurrent.futures import as_completed, ThreadPoolExecutor
from tqdm import tqdm

from app.db import set_database
from utilities.logging_utils import setup_logger
from utilities.constants.LLM_enums import LLMType, ModelType
from utilities.config import DATASET_DIR, TEST_DATA_FILE_PATH
from utilities.schema_linking.schema_linking_utils import select_relevant_schema
from services.client_factory import ClientFactory

logger = setup_logger(__name__)

# Constants
MAX_WORKERS = 4

def process_test_data_item(database, data, pipeline_args,schema_selector_client):
    """
    Process one test data item by computing its runtime schema.
    Returns the updated data item.
    """

    if "runtime_schema_used" in data:
        return data  # Skip processing if already done


    schema = select_relevant_schema(database, data["question"], data["evidence"], schema_selector_client, pipeline_args)

    data["runtime_schema_used"] = schema
    return data

def process_all_databases(dataset_dir, pipeline_args, schema_selector_client):
    """
    Process all test data items in all databases.
    """

    databases = [
        d for d in os.listdir(dataset_dir)
        if os.path.isdir(os.path.join(dataset_dir, d))
    ]

    for database in tqdm(databases, desc=f"Processing databases:"):
        set_database(database)
        file_path = TEST_DATA_FILE_PATH.format(database_name=database)
        
        with open(file_path, "r") as file:
            test_data = json.load(file)
        
        # running the first example w/o threading to make lsh and minhashes
        first_res =  process_test_data_item(database, test_data[0], pipeline_args, schema_selector_client)
        test_data[0] = first_res

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(process_test_data_item, database, data, pipeline_args, schema_selector_client): idx
                for idx, data in enumerate(test_data[1:])
            }
            for future in tqdm(as_completed(futures), total=len(futures), desc=f"Processing test item for db: {database}"):
                idx = futures[future]
                try:
                    test_data[idx] = future.result()
                except Exception as e:
                    logger.error(f"Error processing item {idx} in database {database}: {e}")

                with open(file_path, "w") as file:
                    json.dump(test_data, file, indent=4)
        
        with open(file_path, "w") as file:
            json.dump(test_data, file, indent=4)

def process_test_file(test_file, pipeline_args, schema_selector_client):
    """
    Process all test data items in a single test file.
    """

    with open(test_file, "r") as file:
        test_data = json.load(file)

    grouped_data = defaultdict(list)
    for idx, data in enumerate(test_data):
        grouped_data[data["db_id"]].append((idx, data))

    for db_id, items in tqdm(grouped_data.items(),desc=f"Processing database"):
        set_database(db_id)

        #running the first example w/o threading to make lsh and minhashes
        first_res =  process_test_data_item(db_id, test_data[0], pipeline_args, schema_selector_client)
        test_data[0] = first_res
    
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(process_test_data_item, db_id, data, pipeline_args, schema_selector_client): idx
                for idx, data in items[1:]
            }
            for future in tqdm(as_completed(futures), total=len(futures), desc=f"Processing test items for db: {db_id}"):
                idx = futures[future]
                try:
                    test_data[idx] = future.result()
                except Exception as e:
                    logger.error(f"Error processing item {idx} in database {db_id}: {e}")
    
        with open(test_file, "w") as file:
            json.dump(test_data, file, indent=4)


if __name__ == '__main__':
    """
    To run this script:

    1. Ensure you have set the correct `DATASET_TYPE` in `utilities.config`:
       - Set `DATASET_TYPE` to DatasetType.BIRD_TRAIN for training data.
       - Set `DATASET_TYPE` to DatasetType.BIRD_DEV for development data.

    2. Data Preparation:
       - For single-file processing (default mode):
           • Make sure that the test data is stored in the following file: <DATASET_DIR>/processed_test.json
       - For processing all databases:
           • Ensure each database has its own subdirectory under DATASET_DIR.
           • Each subdirectory must include a test data file following the path defined by TEST_DATA_FILE_PATH.

    3. Configuration Options:
       - In the script, three key flags control the processing mode:
           • test_file_for_all_databases (bool):
               - Set to False to process a single test file.
               - Set to True to process test data for all databases found in DATASET_DIR.
           • use_llm_only (bool):
               - Set to True if you wish to only prune the schema using LLM.
               - If it is set to False, adjust the pipeline_args dictionary as needed.
               - The pipeline_args dictionary also includes parameters:
                   • n_description: Number of top column description matches.
                   • n_value: Number of top value matches using LSH.
                   • llm_config: Configuration for the LLM.
           • use_llm_for_keyword_extraction (bool):
               - Set to True if you wish to enable LLM-based keyword extraction and schema selection.
               - If enabled, adjust the llm_config dictionary (including LLMType, ModelType, temperature, and max_tokens) as needed.

    4. Running the Script:
       - Open your terminal and navigate to the directory containing this script.
       - Run the script with:
             python3 -m scripts.add_runtime_pruned_schema

    5. Expected Outputs:
       - The test data JSON file(s) will be updated in-place with an added "runtime_schema_used" field for each processed item.
       - Progress and any errors will be logged to the console.
    """

    # Inputs 
    seperate_test_file_for_databases = True

    use_schema_selector_only = True 
    pipeline_args = None

    # LLM Config for Schema Selector
    llm_type = LLMType.GOOGLE_AI
    model_type = ModelType.GOOGLEAI_GEMINI_2_0_FLASH_EXP
    temperature = 0.2
    max_tokens = 8000

    schema_selector_client = ClientFactory.get_client(llm_type,model_type,temperature, max_tokens)

    if not use_schema_selector_only:
        use_llm_for_keyword_extraction = True
        keyword_extraction_client = None

        if use_llm_for_keyword_extraction:
            # LLM Config for keyword extraction from target question
            llm_type = LLMType.GOOGLE_AI
            model_type = ModelType.GOOGLEAI_GEMINI_2_0_FLASH_EXP
            temperature = 0.2
            max_tokens = 8000

            keyword_extraction_client = ClientFactory.get_client(llm_type, model_type,temperature, max_tokens)

        pipeline_args = {
            "n_description": 6,  # For top k in column description matches
            "n_value": 6, # For top k in value matches using LSH
            "keyword_extraction_client": keyword_extraction_client
        }
    
    if not seperate_test_file_for_databases:
        test_file = f'{DATASET_DIR}/processed_test.json'
        process_test_file(test_file, pipeline_args, schema_selector_client)
    else:
        process_all_databases(DATASET_DIR, pipeline_args, schema_selector_client)
