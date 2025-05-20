import concurrent.futures
import json
import os
import time

from alive_progress import alive_bar
from app import db
from services.clients.client_factory import ClientFactory
from utilities.candidate_selection import xiyan_basic_llm_selector
from utilities.config import PATH_CONFIG
from utilities.constants.services.llm_enums import LLMType, ModelType
from utilities.constants.prompts_enums import (FormatType, PromptType,
                                               RefinerPromptType)
from utilities.logging_utils import setup_logger
from utilities.prompts.prompt_factory import PromptFactory
from utilities.sql_improvement import improve_sql_query
from utilities.utility_functions import format_sql_response
from utilities.vectorize import make_samples_collection

logger = setup_logger(__name__)

MAX_THREADS = 6

def generate_sql(candidate, item, database):
    """
    Prompts the LLM to generate an SQL query and optionally improves it.
    """

    try:
        # Get the client for the candidate model
        client = ClientFactory.get_client(*candidate['model'], candidate['temperature'], candidate['max_tokens'])

        # Create the prompt for the candidate
        prompt = PromptFactory.get_prompt_class(
            prompt_type=candidate['prompt_config']['type'],
            target_question=item['question'],
            shots=candidate['prompt_config']['shots'],
            schema_format=candidate['prompt_config']['format_type'],
            schema=item['runtime_schema_used'] if candidate['prune_schema'] else None,
            evidence=item['evidence'] if candidate['add_evidence'] else None
        )

        # Generate the SQL query using the LLM
        sql = format_sql_response(client.execute_prompt(prompt=prompt))

        # Improve the SQL query if improvement configuration is provided
        if candidate.get("improve_config"):
            try:
                improve_config = candidate["improve_config"]
                improv_client = ClientFactory.get_client(*improve_config['model'], improve_config['temperature'], improve_config['max_tokens'])

                sql = improve_sql_query(
                    sql=sql,
                    max_improve_sql_attempts=improve_config['max_attempts'],
                    database_name=database,
                    client=improv_client,
                    target_question=item['question'],
                    shots=improve_config['prompt_config']['shots'],
                    schema_used=item['runtime_schema_used'] if improve_config['prune_schema'] else None,
                    evidence=item['evidence'] if improve_config['add_evidence'] else None,
                    refiner_prompt_type=improve_config['prompt_config']['type'],
                    chat_mode=improve_config['prompt_config']['chat_mode']
                )
            except Exception as e:
                logger.error(f"Error improving SQL query: {str(e)}")
                raise

        return [sql, candidate['candidate_id']]
    except Exception as e:
        logger.error(f"Error processing candidate {candidate['candidate_id']}: {str(e)}")
        raise

def process_test_file(candidates, selector_model=None):
    """
    Processes the test file to generate SQL queries for each test item.
    """

    test_file = PATH_CONFIG.bird_file_path()
    processed_test_file = PATH_CONFIG.processed_test_path(global_file=True)
    pred_path = PATH_CONFIG.formatted_predictions_path(global_file=True)

    try:
        # Load test data and processed test data
        with open(test_file, "r") as file:
            test_data = json.load(file)

        with open(processed_test_file, "r") as file:
            processed_test_data = json.load(file)

        if len(test_data) != len(processed_test_data):
            logger.error("Mismatch in test and processed data lengths.")
            return

        predicted_queries = {}
        if os.path.exists(pred_path) and os.path.getsize(pred_path) > 0:
            with open(pred_path, "r") as file:
                predicted_queries = json.load(file)

        predicted_ids = set(predicted_queries.keys())

        current_database = test_data[0]['db_id']
        db.set_database(current_database)

        # Create samples collection if any candidate uses shots
        if any(candidate['prompt_config']['shots'] > 0 for candidate in candidates):
            make_samples_collection()

        # Get the selector client if multiple candidates are used
        selector_client = (ClientFactory.get_client(*selector_model['model'], selector_model['temperature'], selector_model['max_tokens'])
                           if len(candidates) > 1 else None)

        with alive_bar(len(test_data), bar='fish', spinner='fish2', title='Processing Questions', length=30) as bar:
            for test_item, processed_test_item in zip(test_data, processed_test_data):
                if str(test_item['question_id']) in predicted_ids:
                    bar()
                    continue

                if current_database != test_item['db_id']:
                    current_database = test_item['db_id']
                    db.set_database(current_database)

                item = next((q for q in processed_test_data if q['question_id'] == test_item['question_id']), processed_test_item)

                all_results = []
                with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
                    futures = [executor.submit(generate_sql, candidate, item, current_database) for candidate in candidates]
                    for future in concurrent.futures.as_completed(futures):
                        all_results.append(future.result())

                if len(all_results) > 1 and selector_client:
                    sql, _ = xiyan_basic_llm_selector(all_results, item['question'], selector_client, current_database, item['runtime_schema_used'], item['evidence'])
                else:
                    sql, _ = all_results[0]

                predicted_queries[int(item['question_id'])] = f"{sql}\t----- bird -----\t{current_database}"

                if len(predicted_queries) % 10 == 0:
                    with open(pred_path, "w") as file:
                        json.dump(predicted_queries, file)

                bar()

    except Exception as e:
        logger.error(f"Error processing test file: {str(e)}")
        raise

if __name__ == "__main__":
    """
    To run this script:

    1. Ensure you have set the correct DATASET_TYPE and SAMPLE_DATASET_TYPE in .env:
        - Set DATASET_TYPE to 'bird_train', 'bird_dev' or 'bird_test'
        - Set SAMPLE_DATASET_TYPE to to 'bird_train' or 'bird_dev'

    2. Adjust Input Variables:
        - Ensure all input variables, such as LLM configurations, and prompt configurations, are correctly defined.
        - To enable improvement update each configs 'improve' key with the improver model, prompt_type (xiyan or basic), shots and max attempts to improve
        - To disable improvement set 'improve' to False or None. 
        - To use pruned schema set 'prune_schema' to True
        - To use evidence in the prompts set 'add_evidence' to True

    3. Expected Outputs:
        - Formatted Predictions: Predictions for each processed database are saved in predict_{dataset_type}.json

    6. Additional Notes:
        - Processing includes formatting predictions, executing LLM prompts, and saving results. The script pauses for a short delay between processing to manage API rate limits.
    """

    selector_model = {
        "model": [LLMType.GOOGLE_AI, ModelType.GOOGLEAI_GEMINI_2_0_FLASH_THINKING_EXP_0121],
        "temperature": 0.2,
        "max_tokens": 8192,
    }

    candidates = [
        {
            "candidate_id": 1,
            "model": [LLMType.GOOGLE_AI, ModelType.GOOGLEAI_GEMINI_2_0_PRO_EXP],
            "temperature": 0.2,
            "max_tokens": 8192,
            "prompt_config": {
                "type": PromptType.SEMANTIC_FULL_INFORMATION,
                "shots": 7,
                "format_type": FormatType.M_SCHEMA,
            },
            "prune_schema": True,
            "add_evidence": True,
            "improve_config": {
                "model": [LLMType.GOOGLE_AI, ModelType.GOOGLEAI_GEMINI_2_0_FLASH],
                "temperature": 0.2,
                "max_tokens": 8192,
                "max_attempts": 5,
                "prompt_config": {
                    "type": RefinerPromptType.XIYAN,
                    "shots": 7,
                    "chat_mode": False
                },
                "prune_schema": True,
                "add_evidence": True,
            }
        }
    ]

    if len(candidates) > 1 and not selector_model:
        logger.error("A selector model is required when using multiple candidates.")
    else:
        try:
            process_test_file(candidates=candidates, selector_model=selector_model)
        except Exception as e:
            logger.error(f"Error predicting SQLs from file: {str(e)}")