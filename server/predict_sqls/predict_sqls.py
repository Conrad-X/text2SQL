import json
import os
import time
import concurrent.futures
from alive_progress import alive_bar
from app import db

from utilities.config import PATH_CONFIG
from utilities.logging_utils import setup_logger
from utilities.utility_functions import format_sql_response
from utilities.constants.LLM_enums import LLMType, ModelType
from utilities.constants.prompts_enums import FormatType, PromptType, RefinerPromptType
from utilities.constants.script_constants import GOOGLE_RESOURCE_EXHAUSTED_EXCEPTION_STR
from utilities.prompts.prompt_factory import PromptFactory
from services.client_factory import ClientFactory
from utilities.sql_improvement import improve_sql_query
from utilities.candidate_selection import xiyan_basic_llm_selector
from utilities.vectorize import make_samples_collection

logger = setup_logger(__name__)

# Constants
MAX_THREADS = 6

def prompt_llm(prompt, client, database, question, schema_used=None, evidence='', improve_config=None):
    """
    Prompts the LLM to generate an SQL query and optionally improves it.

    Args:
        prompt (str): The prompt to send to the LLM.
        client: The client to use for executing the prompt.
        database (str): The name of the database.
        question (str): The target question.
        schema_used (str, optional): The schema used. Defaults to None.
        evidence (str, optional): The evidence to include. Defaults to ''.
        improve_config (dict, optional): Configuration for improving the SQL query. Defaults to None.

    Returns:
        str: The generated and optionally improved SQL query.
    """
    sql = ""
    while sql == "":
        try:
            sql = format_sql_response(client.execute_prompt(prompt=prompt))
        except Exception as e:
            if GOOGLE_RESOURCE_EXHAUSTED_EXCEPTION_STR in str(e):
                logger.warning("Quota exhausted. Retrying in 5 seconds...")
                time.sleep(5)
            else:
                logger.error(f"Unhandled exception: {e}")
                raise

    if improve_config:
        try:
            if improve_config['client']:
                improv_client = ClientFactory.get_client(improve_config['client'][0], improve_config['client'][1], client.temperature, client.max_tokens)
            else:
                improv_client = client

            sql = improve_sql_query(
                sql=sql,
                max_improve_sql_attempts=improve_config['max_attempts'],
                database_name=database,
                client=improv_client,
                target_question=question,
                shots=improve_config['shots'],
                schema_used=schema_used,
                evidence=evidence,
                refiner_prompt_type=improve_config['prompt'],
                chat_mode=False
            )
        except Exception as e:
            logger.error(f"Error improving SQL query: {e}")
            raise

    return sql

def process_config(config, item, database):
    """
    Processes a configuration to generate an SQL query.

    Args:
        config (dict): The configuration for the LLM.
        item (dict): The item containing the question and schema.
        database (str): The name of the database.

    Returns:
        list: A list containing the generated SQL query and the configuration ID.
    """
    try:
        client = ClientFactory.get_client(config['model'][0], config['model'][1], config['temperature'], config['max_tokens'])

        prompt = PromptFactory.get_prompt_class(
                    prompt_type=config['prompt_config']['type'],
                    target_question=item["question"],
                    shots=config['prompt_config']['shots'],
                    schema_format=config['prompt_config']['format_type'],
                    schema=item['runtime_schema_used'] if config['prune_schema'] else None,
                    evidence=item['evidence'] if config['add_evidence'] else None,
                )

        sql = prompt_llm(
            prompt=prompt,
            client=client,
            database=database,
            question=item["question"],
            schema_used=item["schema_used"],
            evidence=item["evidence"],
            improve_config=config["improve"],
        )

        return [sql, config['config_id']]
    except Exception as e:
        logger.error(f"Error processing config {config['config_id']}: {e}")
        raise

def process_test_file(run_config, selector_model=None):
    """
    Processes the test file to generate SQL queries for each test item.

    Args:
        run_config (list): List of configurations to run.
        selector_model (dict, optional): The selector model configuration. Defaults to None.
    """
    test_file = PATH_CONFIG.bird_file_path()
    processed_test_file = PATH_CONFIG.processed_test_path(global_file=True)
    pred_path = PATH_CONFIG.formatted_predictions_path(global_file=True)

    try:
        with open(test_file, "r") as file:
            test_data = json.load(file)

        with open(processed_test_file, 'r') as file:
            processed_test_data = json.load(file)

        # Check if files have the same number of items
        if len(test_data) != len(processed_test_data):
            logger.error(f"Test data ({len(test_data)} items) and processed data ({len(processed_test_data)} items) have different lengths")
            logger.error(f"Please check the data files {test_file} and {processed_test_file}")
            return

        # Load predicted queries if available
        predicted_queries = {}
        if os.path.exists(pred_path):
            with open(pred_path, 'r') as file:
                if os.path.getsize(pred_path) > 0:
                    predicted_queries = json.load(file)

        predicted_ids = set(predicted_queries.keys())

        # Set current database
        current_database = test_data[0]['db_id']
        db.set_database(current_database)

        # Create samples collection for few shot prompts
        if any(config['prompt_config']['shots'] > 0 for config in run_config):
            make_samples_collection()

        # Initialize selector client if multiple run configs are used
        if len(run_config) > 1:
            selector_client = ClientFactory.get_client(selector_model['model'][0], selector_model['model'][1], selector_model['temperature'], selector_model['max_tokens'])

        with alive_bar(len(test_data), bar='fish', spinner='fish2', title=f'Processing Questions', length=30) as bar:
            for test_item, processed_test_item in zip(test_data, processed_test_data):
                if str(test_item['question_id']) in predicted_ids:
                    logger.info(f"Skipping already processed query {test_item['question_id']}")
                    bar()
                    continue

                if current_database != test_item['db_id']:
                    current_database = test_item['db_id']
                    db.set_database(current_database)

                if test_item['question_id'] == processed_test_item["question_id"]:
                    item = processed_test_item
                else:
                    item = next((q for q in processed_test_data if q["question_id"] == test_item["question_id"]), None)

                all_results = []

                with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
                    future_to_config = {executor.submit(process_config, config, item, current_database): config for config in run_config}
                    for future in concurrent.futures.as_completed(future_to_config):
                        all_results.append(future.result())

                if len(all_results) > 1:
                    sql, _ = xiyan_basic_llm_selector(all_results, item['question'], selector_client, current_database, item['schema_used'], item['evidence'])
                else:
                    sql, _ = all_results[0][0], all_results[0][1]

                predicted_queries[int(item["question_id"])] = (
                    f"{sql}\t----- bird -----\t{current_database}"
                )

                with open(pred_path, "w") as file:
                    json.dump(predicted_queries, file)

                bar()
                
    except Exception as e:
        logger.error(f"Error processing test file: {e}")
        raise

def validate_config(config, required_keys):
    """
    Validates the configuration to ensure it contains all required keys.

    Args:
        config (list): List of configurations to validate.
        required_keys (list): List of required keys.

    Returns:
        bool: True if all configurations contain the required keys, False otherwise.
    """
    required_keys_set = set(required_keys)
    return all(required_keys_set.issubset(d.keys()) for d in config)

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
    keys = [
        "model",
        "temperature",
        "max_tokens",
        "prompt_config",
        "prune_schema",
        "add_evidence",
        "improve",
        'config_id'
    ]

    # Initial variables
    selector_model = {
        "model":[LLMType.GOOGLE_AI, ModelType.GOOGLEAI_GEMINI_2_0_FLASH_THINKING_EXP_0121],
        "temperature": 0.2,
        "max_tokens": 8192,
    }

    config_options = [
        {
            'config_id': 1,
            "model": [LLMType.DASHSCOPE, ModelType.DASHSCOPE_QWEN_MAX],
            "temperature": 0.2,
            "max_tokens": 8192,
            "prompt_config": {
                "type": PromptType.SEMANTIC_FULL_INFORMATION,
                "shots": 7,
                "format_type": FormatType.M_SCHEMA,
            },
            "improve": {
                "client": [LLMType.GOOGLE_AI, ModelType.GOOGLEAI_GEMINI_2_0_FLASH],
                "prompt": RefinerPromptType.XIYAN,
                "max_attempts": 5,
                'shots': 5
            },
            "prune_schema": True,
            "add_evidence": True,
        },
        {
            'config_id': 2,
            "model": [LLMType.GOOGLE_AI, ModelType.GOOGLEAI_GEMINI_2_0_PRO_EXP],
            "temperature": 0.2,
            "max_tokens": 8192,
            "prompt_config": {
                "type": PromptType.SEMANTIC_FULL_INFORMATION,
                "shots": 7,
                "format_type": FormatType.M_SCHEMA,
            },
            "improve": {
                "client": [LLMType.GOOGLE_AI, ModelType.GOOGLEAI_GEMINI_2_0_FLASH],
                "prompt": RefinerPromptType.XIYAN,
                "max_attempts": 5,
                'shots': 5
            },
            "prune_schema": True,
            "add_evidence": True,
        }
    ]

    if not validate_config(config_options, keys):
        logger.error("Config Not Correctly Set")
        exit()

    try:
        process_test_file(
            run_config=config_options,
            selector_model=selector_model
        )
    except Exception as e:
        logger.error(f"Error predicting sqls from file: {e}")