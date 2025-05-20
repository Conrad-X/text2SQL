import copy
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.db import set_database
from services.clients.base_client import Client
from services.clients.client_factory import ClientFactory
from tqdm import tqdm
from utilities.bird_utils import (ensure_global_bird_test_file_path,
                                  get_database_list,
                                  group_bird_items_by_database_name,
                                  load_json_from_file, save_json_to_file)
from utilities.config import PATH_CONFIG
from utilities.constants.bird_utils.indexing_constants import (
    EVIDENCE_KEY, QUESTION_KEY, RUNTIME_SCHEMA_USED_KEY)
from utilities.constants.services.llm_enums import LLMConfig, LLMType, ModelType
from utilities.constants.preprocess.add_runtime_pruned_schema.indexing_constants import (
    KEYWORD_EXTRACTION_CLIENT_KEY, LSH_KEY, MINHASH_KEY,
    TOP_K_COLUMN_DESCRIPTION_MATCHES_KEY, TOP_K_VALUE_MATCHES_KEY)
from utilities.constants.preprocess.add_runtime_pruned_schema.response_messages import (
    ERROR_FAILED_ITEM, ERROR_FAILED_TO_ADD_PRUNED_SCHEMA,
    ERROR_PROCESSING_DATABASE_GLOBAL_FILE, ERROR_PROCESSING_DATABASE_TEST_FILE,
    INFO_ADDING_PRUNED_SCHEMA, INFO_PROCESSING_DATABASES)
from utilities.logging_utils import setup_logger
from utilities.schema_linking.schema_linking_utils import \
    select_relevant_schema
from utilities.schema_linking.value_retrieval import (
    create_lsh_for_all_databases, load_db_lsh)
from utilities.vectorize import make_column_description_collection

# File Constants
MAX_THREAD_WORKERS = 4

# Configuration Constants
KEYWORD_EXTRACTION_CLIENT_CONFIG = LLMConfig(
    llm_type=LLMType.GOOGLE_AI,
    model_type=ModelType.GOOGLEAI_GEMINI_2_0_FLASH,
)
SCHEMA_SELECTOR_CLIENT_CONFIG = LLMConfig(
    llm_type=LLMType.GOOGLE_AI,
    model_type=ModelType.GOOGLEAI_GEMINI_2_5_PRO_PREVIEW,
)
TOP_K_DESCRIPTION_CONFIG = 6
TOP_K_VALUE_MATCHES_CONFIG = 6
UPDATE_DATABASE_SPECIFIC_TEST_FILES = False
USE_LLM_FOR_KEYWORD_EXTRACTION = False
USE_SCHEMA_SELECTOR_CLIENT_ONLY = True

logger = setup_logger(__name__)


def build_keyword_extraction_client() -> Optional[Any]:
    """
    Builds the keyword extraction client based on configuration.

    Returns:
        Optional[Any]: An instance of the keyword extraction client, or None if not used.
    """
    if not USE_LLM_FOR_KEYWORD_EXTRACTION:
        return None

    return ClientFactory.get_client(KEYWORD_EXTRACTION_CLIENT_CONFIG)


def build_pipeline_args_for_processing() -> Optional[Dict[str, Any]]:
    """
    Builds and returns arguments required for schema linking processing.

    This includes keyword extraction and top-K matching configurations
    for column descriptions and cell values. If the schema selector
    client is used exclusively, no additional processing arguments are needed.

    Returns:
        Optional[Dict[str, Any]]: Dictionary of pipeline arguments, or None.
    """
    if USE_SCHEMA_SELECTOR_CLIENT_ONLY:
        return None

    create_lsh_for_all_databases(dataset_dir=str(PATH_CONFIG.dataset_dir()))
    return {
        TOP_K_COLUMN_DESCRIPTION_MATCHES_KEY: TOP_K_DESCRIPTION_CONFIG,
        TOP_K_VALUE_MATCHES_KEY: TOP_K_VALUE_MATCHES_CONFIG,
        KEYWORD_EXTRACTION_CLIENT_KEY: build_keyword_extraction_client(),
    }


def update_pipeline_configuration(
    database_name: str, existing_pipeline_args: Optional[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    """
    Create and return a pipeline configuration if needed.
    If no pipeline args are provided, return None.

    Args:
        database_name (str): The name of the database.
        pipeline_args (Optional[Dict[str, Any]]): The current pipeline arguments.

    Returns:
        Optional[Dict[str, Any]]: Updated pipeline arguments including LSH and MinHash,
                                  or None if no args are provided.
    """
    if not existing_pipeline_args:
        return None

    make_column_description_collection()
    lsh, minhash = load_db_lsh(database_name)

    updated_args = copy.deepcopy(existing_pipeline_args)  # avoid side-effects
    updated_args[LSH_KEY] = lsh
    updated_args[MINHASH_KEY] = minhash

    return updated_args


def add_pruned_schema_to_bird_item(
    database_name: str,
    item: Dict[str, Any],
    pipeline_args: Optional[Dict[str, Any]],
    schema_selector_client: Client,
) -> Dict[str, Any]:
    """
    Returns a new item dict with a pruned schema attached under RUNTIME_SCHEMA_USED_KEY
    if it hasn't already been added.

    Parameters:
        database_name (str): Name of the database for which schema pruning is performed.
        item (Dict[str, Any]): The BIRD test item; must include a QUESTION_KEY.
        pipeline_args (Optional[Dict[str, Any]]): Additional arguments passed to schema selector.
        schema_selector_client (Client): External schema selector client.

    Returns:
        Dict[str, Any]: A (possibly modified) copy of the item with pruned schema added.

    Raises:
        Exception: If schema selection fails.
    """
    if RUNTIME_SCHEMA_USED_KEY in item:
        return item

    pruned_schema = select_relevant_schema(
        database_name=database_name,
        query=item[QUESTION_KEY],
        evidence=item.get(EVIDENCE_KEY, ""),
        schema_selector_client=schema_selector_client,
        pipeline_args=pipeline_args,
    )

    updated_item = copy.deepcopy(item)  # avoid side-effects
    updated_item[RUNTIME_SCHEMA_USED_KEY] = pruned_schema
    return updated_item


def process_items_with_pruned_schema_threaded(
    items: List[Dict[str, Any]],
    database_name: str,
    pipeline_args: Optional[Dict[str, Any]],
    schema_selector_client: Client,
) -> List[Dict[str, Any]]:
    """
    Process a list of items concurrently using threading.
    Each item is processed for a given database.

    Args:
        items (List[Dict[str, Any]]): The items to process.
        database_name (str): The name of the database.
        pipeline_args (Optional[Dict[str, Any]]): The pipeline arguments.
        schema_selector_client (Any): The client to interact with the selector.

    Returns:
        List[Dict[str, Any]]: A list of processed items.
    """
    # Set up the database and pipeline arguments
    set_database(database_name)
    updated_pipeline_args = update_pipeline_configuration(database_name, pipeline_args)

    # Use threading to process the items concurrently
    updated_items = copy.deepcopy(items)  # avoid side-effects
    with ThreadPoolExecutor(max_workers=MAX_THREAD_WORKERS) as executor:
        futures = {
            executor.submit(
                add_pruned_schema_to_bird_item,
                database_name,
                item,
                updated_pipeline_args,
                schema_selector_client,
            ): index
            for index, item in enumerate(items)
        }
        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc=INFO_ADDING_PRUNED_SCHEMA.format(database_name=database_name),
        ):
            index = futures[future]
            try:
                updated_items[index] = future.result()
            except Exception as e:
                logger.error(
                    ERROR_FAILED_ITEM.format(
                        index=index, database_name=database_name, error=str(e)
                    )
                )
    return updated_items


def process_each_database_test_file_with_pruned_schema(
    dataset_dir: Path,
    pipeline_args: Optional[Dict[str, Any]],
    schema_selector_client: Client,
) -> None:
    """
    Load, group, process, and save BIRD test items with pruned schema.
    Updates test files for each database.

    Args:
        dataset_dir (Path): Root directory containing database subdirectories.
        pipeline_args (Optional[Dict[str, Any]]): Optional pipeline configuration.
        schema_selector_client (Client): Client used for item selection and processing.
    """
    databases = get_database_list(dataset_directory=dataset_dir)

    for database_name in tqdm(databases, desc=INFO_PROCESSING_DATABASES):
        file_path = PATH_CONFIG.processed_test_path(database_name=database_name)
        try:
            items = load_json_from_file(file_path)
            items = process_items_with_pruned_schema_threaded(
                items, database_name, pipeline_args, schema_selector_client
            )
            save_json_to_file(file_path, items)
        except Exception as e:
            logger.error(
                ERROR_PROCESSING_DATABASE_TEST_FILE.format(
                    database_name=database_name, error=str(e)
                )
            )


def process_global_test_file_with_pruned_schema(
    test_file: Path,
    pipeline_args: Optional[Dict[str, Any]],
    schema_selector_client: Client,
) -> None:
    """
    Load, group, process, and save BIRD test items with pruned schema.
    Updates global test file.

    Args:
        test_file (Path): Path to the JSON test file containing BIRD examples from multiple DBs.
        pipeline_args (Optional[Dict[str, Any]]): Configuration for processing logic.
        schema_selector_client (Client): Client used for selecting relevant schema components.
    """
    bird_items = load_json_from_file(test_file)
    bird_items_grouped_by_db = group_bird_items_by_database_name(bird_items)

    for database_name, items in tqdm(
        bird_items_grouped_by_db.items(), desc=INFO_PROCESSING_DATABASES
    ):
        try:
            updated_items = process_items_with_pruned_schema_threaded(
                items, database_name, pipeline_args, schema_selector_client
            )
            # Save partial results after processing each database
            bird_items_grouped_by_db[database_name] = updated_items
            updated_bird_items = [
                item for items in bird_items_grouped_by_db.values() for item in items
            ]
            save_json_to_file(test_file, updated_bird_items)
        except Exception as e:
            logger.error(
                ERROR_PROCESSING_DATABASE_GLOBAL_FILE.format(
                    database_name=database_name, error=str(e)
                )
            )


def run_pruned_schema_annotation_pipeline() -> None:
    """
    Main pipeline execution entry point.
    Builds required clients, prepares arguments, and runs the configured test pipeline.
    """
    schema_selector_client = ClientFactory.get_client(SCHEMA_SELECTOR_CLIENT_CONFIG)
    pipeline_args = build_pipeline_args_for_processing()

    if UPDATE_DATABASE_SPECIFIC_TEST_FILES:
        dataset_dir = PATH_CONFIG.dataset_dir()
        process_each_database_test_file_with_pruned_schema(
            dataset_dir, pipeline_args, schema_selector_client
        )
    else:
        test_file = ensure_global_bird_test_file_path(
            PATH_CONFIG.processed_test_path(global_file=True)
        )
        process_global_test_file_with_pruned_schema(
            test_file, pipeline_args, schema_selector_client
        )


def main():
    """
    Main function to execute the script.
    It runs the pruned schema annotation pipeline.
    """
    try:
        run_pruned_schema_annotation_pipeline()
    except Exception as e:
        logger.error(ERROR_FAILED_TO_ADD_PRUNED_SCHEMA.format(error=str(e)))


if __name__ == "__main__":
    """
    To run this script:

    1. Ensure you have set the correct DATASET_TYPE in .env:
    - Set DATASET_TYPE to DatasetType.BIRD_TRAIN for training data.
    - Set DATASET_TYPE to DatasetType.BIRD_DEV for development data.

    2. Data Preparation:
    - For single-file processing (default mode):
        • Make sure that the test data is stored in the following file:
            <DATASET_DIR>/processed_test.json
    - For processing all databases:
        • Ensure each database has its own subdirectory under DATASET_DIR.
        • Each subdirectory must include a test data file following the path defined by:
            PATH_CONFIG.processed_test_file_path().

    3. Configuration Options:
    - In the script, several constants at the top of the file control the processing mode:
        • MAX_THREAD_WORKERS (int): Number of worker threads to use for concurrent processing.
        • SEPERATE_TEST_FILE_FOR_DATABASES (bool): Set to False to process a single test file.
          Set to True to process test data for all databases found in DATASET_DIR.
        • USE_SCHEMA_SELECTOR_CLIENT_ONLY (bool): Set to True if you wish to only prune the schema using LLM.
          If set to False, adjust the pipeline_args dictionary as needed.
        • USE_LLM_FOR_KEYWORD_EXTRACTION (bool): Set to True if you wish to enable LLM-based keyword
          extraction and schema selection.
        • SCHEMA_SELECTOR_CLIENT_CONFIG (LLMConfig): Configuration for the schema selector client.
        • KEYWORD_EXTRACTION_CLIENT_CONFIG (LLMConfig): Configuration for the keyword extraction client.
        • TOP_K_DESCRIPTION_CONFIG (int): Number of top column description matches.
        • TOP_K_VALUE_MATCHES_CONFIG (int): Number of top value matches using LSH.

    4. Expected Outputs:
    - The test data JSON file(s) will be updated in-place with an added "runtime_schema_used" field for
      each processed item.
    - Progress and any errors will be logged to the console.
    """

    main()
