import concurrent.futures
import json
import os
import time
from typing import Dict, List, Union

from app import db
from services.clients.client_factory import ClientFactory
from tqdm import tqdm
from utilities.candidate_selection import xiyan_basic_llm_selector
from utilities.config import PATH_CONFIG
from utilities.constants.services.llm_enums import LLMConfig, LLMType, ModelType
from utilities.constants.prompts_enums import (FormatType, PromptType,
                                               RefinerPromptType)
from utilities.logging_utils import setup_logger
from utilities.prompts.prompt_factory import PromptFactory
from utilities.selection_metadata_collection import SelectionMetadata
from utilities.sql_improvement import improve_sql_query
from utilities.utility_functions import check_config_types, format_sql_response
from utilities.vectorize import make_samples_collection

logger = setup_logger(__name__)

# Number of Workers to work on seperate candidates
MAX_CANDIDATE_WORKERS = 6

# Number of Workers to work on seperate databases
MAX_DATABASE_WORKERS = 2


def load_json_file(file_path: str):
    with open(file_path, "r") as file:
        file_data = json.load(file)
    return file_data


def generate_sql(candidate: Dict, item: Dict, database: str) -> List:
    """
    Prompts the LLM to generate an SQL query and optionally improves it.
    """

    try:
        # Get the client for the candidate model
        client = ClientFactory.get_client(candidate['llm_config'])

        # Create the prompt for the candidate
        prompt = PromptFactory.get_prompt_class(
            prompt_type=candidate["prompt_config"]["type"],
            target_question=item["question"],
            shots=candidate["prompt_config"]["shots"],
            schema_format=candidate["prompt_config"]["format_type"],
            schema=item["runtime_schema_used"] if candidate["prune_schema"] else None,
            evidence=item["evidence"] if candidate["add_evidence"] else None,
            database_name=database
        )

        # Generate the SQL query using the LLM
        sql = format_sql_response(client.execute_prompt(prompt=prompt))

        # Improve the SQL query if improvement configuration is provided
        if candidate.get("improve_config"):
            try:
                improve_config = candidate["improve_config"]
                improv_client = ClientFactory.get_client(improve_config["llm_config"])

                sql = improve_sql_query(
                    sql=sql,
                    max_improve_sql_attempts=improve_config["max_attempts"],
                    database_name=database,
                    client=improv_client,
                    target_question=item["question"],
                    shots=improve_config["prompt_config"]["shots"],
                    schema_used=(
                        item["runtime_schema_used"]
                        if improve_config["prune_schema"]
                        else None
                    ),
                    evidence=(
                        item["evidence"] if improve_config["add_evidence"] else None
                    ),
                    refiner_prompt_type=improve_config["prompt_config"]["type"],
                    chat_mode=improve_config["prompt_config"]["chat_mode"],
                )
            except Exception as e:
                logger.error(f"Error improving SQL query: {str(e)}")
                raise

        return [sql, candidate["candidate_id"]]
    except Exception as e:
        logger.error(
            f"Error processing candidate {candidate['candidate_id']}: {str(e)}"
        )
        raise


def process_database(
    database: str,
    candidates: List,
    selector_model=None,
    collect_data=False,
    selection_metadata: Union[SelectionMetadata, None] = None,
) -> None:
    """
    Main processing function for a single database.
    """

    db.set_database(database)

    formatted_pred_path = PATH_CONFIG.formatted_predictions_path(database_name=database)
    gold_sql_path = PATH_CONFIG.test_gold_path(database_name=database)

    predicted_scripts = {}
    gold_items = []

    # Load intermediary results if they exist
    if os.path.exists(formatted_pred_path):
        with open(formatted_pred_path, "r") as pred_file:
            if os.path.getsize(formatted_pred_path) > 0:
                predicted_scripts = json.load(pred_file)
                if os.path.exists(gold_sql_path):
                    with open(gold_sql_path, "r") as gold_file:
                        gold_items = [line.strip() for line in gold_file.readlines()]

    # Identify already processed question IDs
    processed_ids = set(predicted_scripts.keys())

    test_data = load_json_file(PATH_CONFIG.processed_test_path(database_name=database))

    if len(candidates) > 1:
        selector_client = ClientFactory.get_client(selector_model)

    for item in tqdm(test_data, desc=f"Processing {database}"):
        try:
            if str(item["question_id"]) in processed_ids:
                logger.info(f"Skipping already processed query {item['question_id']}")
                continue

            all_results = []

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=MAX_CANDIDATE_WORKERS
            ) as executor:
                future_to_config = {
                    executor.submit(generate_sql, config, item, database): config
                    for config in candidates
                }
                for future in concurrent.futures.as_completed(future_to_config):
                    try:
                        result = future.result()
                        all_results.append(result)
                    except Exception as e:
                        logger.error(f"Error processing candidate in database {database}: {e}", exc_info=True)
                        raise

            if len(all_results) > 1:
                sql, config_id = xiyan_basic_llm_selector(
                    all_results,
                    item["question"],
                    selector_client,
                    database,
                    item["runtime_schema_used"],
                    item["evidence"],
                )
            else:
                sql, config_id = all_results[0][0], all_results[0][1]

            if collect_data:
                selection_metadata.update_selection_metadata(
                    candidates=all_results,
                    gold_sql=item["SQL"],
                    database=database,
                    selected_config=config_id,
                )

            predicted_scripts[int(item["question_id"])] = (
                f"{sql}\t----- bird -----\t{database}"
            )
            gold_items.append(f"{item['SQL']}\t{database}")

            os.makedirs(os.path.dirname(formatted_pred_path), exist_ok=True)
            with open(formatted_pred_path, "w") as file:
                json.dump(predicted_scripts, file)

            with open(gold_sql_path, "w") as file:
                for line in gold_items:
                    file.write(f"{line}\n")

            if collect_data:
                selection_metadata.save_metadata()

        except Exception as e:
            logger.error(f"Exception in {e}", exc_info=True)
            raise

    logger.info(f"Processed {database}")


def process_all_databases(
    dataset_dir: str,
    candidates: List,
    selector_model: Dict = None,
    collect_data: bool = False,
    save_global_files: bool = True,
) -> None:
    """
    Process all databases in the specified directory.
    """

    if any(config["prompt_config"]["shots"] > 0 for config in candidates):
        make_samples_collection()

    databases = [
        d
        for d in os.listdir(dataset_dir)
        if os.path.isdir(os.path.join(dataset_dir, d))
    ]

    if collect_data:
        selection_metadata = SelectionMetadata(
            run_config=candidates,
            database=databases[0],
        )
    else:
        selection_metadata = None

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=MAX_DATABASE_WORKERS
    ) as executor:
        futures = {
            executor.submit(
                process_database,
                database=database,
                candidates=candidates,
                selector_model=selector_model,
                collect_data=collect_data,
                selection_metadata=selection_metadata,
            ): database
            for database in databases
        }
    for future in concurrent.futures.as_completed(futures):
        try:
            future.result()
        except Exception as e:
            db_name = futures[future]
            logger.error(f"Error processing {db_name}: {e}")

    if save_global_files:

        # Saving Global File
        global_predictions = {}
        for database in databases:
            database_predictions = load_json_file(
                PATH_CONFIG.formatted_predictions_path(database_name=database)
            )

            for prediction in database_predictions:
                global_predictions[prediction] = database_predictions[prediction]

        global_predictions = dict(
            sorted(global_predictions.items(), key=lambda item: int(item[0]))
        )
        with open(
            PATH_CONFIG.formatted_predictions_path(global_file=True), "w"
        ) as file:
            json.dump(global_predictions, file)


if __name__ == "__main__":
    """
    To run this script:

    1. Ensure you have set the correct `PATH_CONFIG.dataset_type` and `PATH_CONFIG.sample_dataset_type` in `utilities.config`:
       - Set `PATH_CONFIG.dataset_type` to DatasetType.BIRD_TRAIN for training data.
       - Set `PATH_CONFIG.dataset_type` to DatasetType.BIRD_DEV for development data.
       - Set `PATH_CONFIG.sample_dataset_type` to DatasetType.BIRD_DEV or DatasetType.BIRD_TRAIN.

    2. Metadata File Configuration:
        - If continuing previous processing, provide the timestamp of the existing metadata file.
        - If starting a new process, set the `metadata_file_path` to `None`.

    3. Adjust Input Variables:
        - Ensure all input variables, such as file paths, LLM configurations, and prompt configurations, are correctly defined.
        - To enable improvement update each configs 'improve' key with the improver model, prompt_type (xiyan or basic), shots and max attempts to improve
        - To disable improvement set 'improve' to False or None.
        - To test a number of variations simply add different configs in each list
        - To use pruned schema set 'prune_schema' to True
        - To use evidence in the prompts set 'add_evidence' to True
        - set collect_selection_data to true to log data about candidate selection and refiner module
        - set save_global_predictions to true to save a global file in the dataset root directory

    4. Run the Script:
        - Execute the following command in the terminal `python3 -m scripts.process_dataset_sequentially`

    5. Expected Outputs:
        - Formatted Predictions: Predictions for each processed database are saved.
        - Gold SQL Scripts: Gold standard SQL scripts are saved alongside predictions.

    6. Additional Notes:
        - Processing includes formatting predictions, executing LLM prompts, and saving results. The script pauses for a short delay between processing to manage API rate limits.
    """

    # Config Types
    gold_config = {
        "candidate_id": int,
        "llm_config": LLMConfig(
            llm_type=LLMType,
            model_type=ModelType,
            temperature=float,
            max_tokens=int
        ),
        "prompt_config": {
            "type": PromptType,
            "shots": int,
            "format_type": FormatType,
        },
        "prune_schema": bool,
        "add_evidence": bool,
        "improve_config": {
            "llm_config": LLMConfig(
                llm_type=LLMType,
                model_type=ModelType,
                temperature=float,
                max_tokens=int
            ),
            "max_attempts": int,
            "prompt_config": {
                "type": RefinerPromptType,
                "shots": int,
                "chat_mode": bool,
            },
            "prune_schema": bool,
            "add_evidence": bool,
        },
    }

    selector_model = LLMConfig(
        llm_type=LLMType.GOOGLE_AI,
        model_type=ModelType.GOOGLEAI_GEMINI_2_0_FLASH_THINKING_EXP_0121,
        temperature=0.2,
        max_tokens=8192,
    )

    candidates = [
        {
            "candidate_id": 1,
            "llm_config": LLMConfig(
                llm_type=LLMType.GOOGLE_AI,
                model_type=ModelType.GOOGLEAI_GEMINI_2_0_FLASH,
                temperature=0.2,
                max_tokens=8192
            ),
            "prompt_config": {
                "type": PromptType.ICL_XIYAN,
                "shots": 7,
                "format_type": FormatType.M_SCHEMA,
            },
            "prune_schema": True,
            "add_evidence": True,
            "improve_config": {
                "llm_config": LLMConfig(
                    llm_type=LLMType.GOOGLE_AI,
                    model_type=ModelType.GOOGLEAI_GEMINI_2_0_FLASH,
                    temperature=0.2,
                    max_tokens=8192
                ),
                "max_attempts": 5,
                "prompt_config": {
                    "type": RefinerPromptType.XIYAN,
                    "shots": 7,
                    "chat_mode": True,
                },
                "prune_schema": True,
                "add_evidence": True,
            },
        },
        {
            "candidate_id": 2,
            "llm_config": LLMConfig(
                llm_type=LLMType.GOOGLE_AI,
                model_type=ModelType.GOOGLEAI_GEMINI_2_0_FLASH,
                temperature=0.2,
                max_tokens=8192
            ),
            "prompt_config": {
                "type": PromptType.BASIC,
                "shots": 7,
                "format_type": FormatType.BASIC,
            },
            "prune_schema": True,
            "add_evidence": True,
            "improve_config": None,
        },
    ]

    collect_selection_data = False
    save_global_predictions = False

    # Config Validation
    candidate_errors = [check_config_types(i, gold_config) for i in candidates]

    for idx, error in enumerate(candidate_errors):
        if len(error) > 0:
            logger.error(f"Errors in Candidate {candidates[idx]['candidate_id']}:")
            for e in error:
                logger.error(f"{e}")

    if any(len(error) > 0 for error in candidate_errors):
        exit()

    # SQL Generation
    process_all_databases(
        dataset_dir=PATH_CONFIG.dataset_dir(),
        candidates=candidates,
        selector_model=selector_model,
        collect_data=(collect_selection_data and (len(candidates) > 1)),
        save_global_files=save_global_predictions,
    )