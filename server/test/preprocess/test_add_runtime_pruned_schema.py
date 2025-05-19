import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from preprocess.add_runtime_pruned_schema import (
    add_pruned_schema_to_bird_item, build_keyword_extraction_client,
    build_pipeline_args_for_processing, main,
    process_each_database_test_file_with_pruned_schema,
    process_global_test_file_with_pruned_schema,
    process_items_with_pruned_schema_threaded,
    run_pruned_schema_annotation_pipeline, update_pipeline_configuration)
from utilities.constants.bird_utils.indexing_constants import (
    QUESTION_KEY, RUNTIME_SCHEMA_USED_KEY)
from utilities.constants.preprocess.add_runtime_pruned_schema.indexing_constants import (
    KEYWORD_EXTRACTION_CLIENT_KEY, LSH_KEY, MINHASH_KEY,
    TOP_K_COLUMN_DESCRIPTION_MATCHES_KEY, TOP_K_VALUE_MATCHES_KEY)
from utilities.constants.preprocess.add_runtime_pruned_schema.response_messages import (
    ERROR_FAILED_ITEM, ERROR_FAILED_TO_ADD_PRUNED_SCHEMA,
    ERROR_PROCESSING_DATABASE_GLOBAL_FILE, ERROR_PROCESSING_DATABASE_TEST_FILE)


class TestBuildKeywordExtractionClient(unittest.TestCase):
    """Tests for the build_keyword_extraction_client function."""

    @patch("preprocess.add_runtime_pruned_schema.USE_LLM_FOR_KEYWORD_EXTRACTION", False)
    def test_returns_none_when_llm_not_used(self):
        """Should return None when USE_LLM_FOR_KEYWORD_EXTRACTION is False."""

        # Call the function to test
        result = build_keyword_extraction_client()

        # Assertions
        self.assertIsNone(result)

    @patch("preprocess.add_runtime_pruned_schema.USE_LLM_FOR_KEYWORD_EXTRACTION", True)
    @patch("preprocess.add_runtime_pruned_schema.ClientFactory.get_client")
    def test_returns_client_when_llm_enabled(self, mock_get_client):
        """Should return a mocked client instance when USE_LLM_FOR_KEYWORD_EXTRACTION is True."""

        # Create a mock client object to return
        mock_client = MagicMock(name="Client")
        mock_get_client.return_value = mock_client

        # Call the function to test
        result = build_keyword_extraction_client()

        # Assertions
        mock_get_client.assert_called_once()
        self.assertEqual(result, mock_client)


class TestBuildPipelineArgsForProcessing(unittest.TestCase):
    """Tests for the build_pipeline_args_for_processing function."""

    @patch("preprocess.add_runtime_pruned_schema.USE_SCHEMA_SELECTOR_CLIENT_ONLY", True)
    def test_returns_none_when_schema_selector_only(self):
        """Should return None when USE_SCHEMA_SELECTOR_CLIENT_ONLY is True."""

        # Call the function to test
        result = build_pipeline_args_for_processing()

        # Assertions
        self.assertIsNone(result)

    @patch("preprocess.add_runtime_pruned_schema.TOP_K_VALUE_MATCHES_CONFIG", 7)
    @patch("preprocess.add_runtime_pruned_schema.TOP_K_DESCRIPTION_CONFIG", 6)
    @patch("preprocess.add_runtime_pruned_schema.build_keyword_extraction_client")
    @patch("preprocess.add_runtime_pruned_schema.create_lsh_for_all_databases")
    @patch(
        "preprocess.add_runtime_pruned_schema.USE_SCHEMA_SELECTOR_CLIENT_ONLY", False
    )
    def test_returns_pipeline_args_when_schema_selector_not_only(
        self, mock_create_lsh_for_all_databases, mock_build_keyword_extraction_client
    ):
        """Should return pipeline arguments when USE_SCHEMA_SELECTOR_CLIENT_ONLY is False."""

        # Create a mock client object to return
        mock_client = MagicMock(name="Client")
        mock_build_keyword_extraction_client.return_value = mock_client

        # Call the function to test
        result = build_pipeline_args_for_processing()

        # Assertions
        mock_create_lsh_for_all_databases.assert_called_once()
        mock_build_keyword_extraction_client.assert_called_once()
        self.assertEqual(
            result,
            {
                TOP_K_COLUMN_DESCRIPTION_MATCHES_KEY: 6,
                TOP_K_VALUE_MATCHES_KEY: 7,
                KEYWORD_EXTRACTION_CLIENT_KEY: mock_client,
            },
        )


class TestUpdatePipelineConfiguration(unittest.TestCase):
    """Tests for the update_pipeline_configuration function."""

    @patch("preprocess.add_runtime_pruned_schema.make_column_description_collection")
    @patch("preprocess.add_runtime_pruned_schema.load_db_lsh")
    def test_returns_none_when_no_existing_pipeline_args(
        self, mock_load_db_lsh, mock_make_column_description_collection
    ):
        """Should return None when no existing pipeline args are provided."""

        # Call the function to test
        result = update_pipeline_configuration("test_database_name", None)

        # Assertions
        self.assertIsNone(result)
        mock_make_column_description_collection.assert_not_called()
        mock_load_db_lsh.assert_not_called()

    @patch("preprocess.add_runtime_pruned_schema.make_column_description_collection")
    @patch("preprocess.add_runtime_pruned_schema.load_db_lsh")
    def test_returns_updated_args_when_existing_pipeline_args_provided(
        self, mock_load_db_lsh, mock_make_column_description_collection
    ):
        """Should return updated pipeline arguments when existing pipeline args are provided."""

        # Setup test data
        pipeline_args = {"key": "value"}

        # Mock the return values of the functions
        mock_lsh = MagicMock(name="LSH")
        mock_minhash = MagicMock(name="MinHash")
        mock_load_db_lsh.return_value = (mock_lsh, mock_minhash)

        # Call the function to test
        result = update_pipeline_configuration("test_database_name", pipeline_args)

        # Assertions
        mock_make_column_description_collection.assert_called_once()
        mock_load_db_lsh.assert_called_once_with("test_database_name")
        self.assertEqual(
            result,
            {
                "key": "value",
                LSH_KEY: mock_lsh,
                MINHASH_KEY: mock_minhash,
            },
        )


class TestAddPrunedSchemaToBirdItem(unittest.TestCase):
    """Tests for the add_pruned_schema_to_bird_item function."""

    @patch("preprocess.add_runtime_pruned_schema.select_relevant_schema")
    def test_returns_item_when_runtime_schema_used_key_exists(
        self, mock_select_relevant_schema
    ):
        """Should return the item unchanged if RUNTIME_SCHEMA_USED_KEY already exists."""
        # Setup test data
        item = {RUNTIME_SCHEMA_USED_KEY: "test_runtime_pruned_schema"}

        # Call the function to test
        result = add_pruned_schema_to_bird_item("test_database_name", item, None, None)

        # Assertions
        self.assertEqual(result, item)
        mock_select_relevant_schema.assert_not_called()

    @patch("preprocess.add_runtime_pruned_schema.select_relevant_schema")
    def test_adds_pruned_schema_when_runtime_schema_used_key_does_not_exist(
        self, mock_select_relevant_schema
    ):
        """Should add pruned schema to item if RUNTIME_SCHEMA_USED_KEY does not exist."""
        # Setup test data
        item = {QUESTION_KEY: "test_question"}

        # Mock the return value of select_relevant_schema
        mock_pruned_schema = MagicMock(name="PrunedSchema")
        mock_select_relevant_schema.return_value = mock_pruned_schema

        # Call the function to test
        result = add_pruned_schema_to_bird_item("test_database_name", item, None, None)

        # Assertions
        mock_select_relevant_schema.assert_called_once_with(
            database_name="test_database_name",
            query="test_question",
            evidence="",
            schema_selector_client=None,
            pipeline_args=None,
        )
        self.assertEqual(
            result,
            {
                QUESTION_KEY: "test_question",
                RUNTIME_SCHEMA_USED_KEY: mock_pruned_schema,
            },
        )


class TestProcessItemsWithPrunedSchemaThreaded(unittest.TestCase):
    """Tests for the process_items_with_pruned_schema_threaded function."""

    @patch("preprocess.add_runtime_pruned_schema.set_database")
    @patch("preprocess.add_runtime_pruned_schema.update_pipeline_configuration")
    @patch("preprocess.add_runtime_pruned_schema.add_pruned_schema_to_bird_item")
    @patch("preprocess.add_runtime_pruned_schema.logger")
    def test_processes_all_items_correctly(
        self,
        mock_logger,
        mock_add_pruned_schema_to_bird_item,
        mock_update_pipeline_configuration,
        mock_set_database,
    ):
        """Should process all items and return updated ones."""
        # Setup test data
        items = [
            {QUESTION_KEY: "test_question_1"},
            {QUESTION_KEY: "test_question_2"},
        ]
        processed_items = [
            {
                QUESTION_KEY: "test_question_1",
                RUNTIME_SCHEMA_USED_KEY: "test_runtime_pruned_schema_1",
            },
            {
                QUESTION_KEY: "test_question_2",
                RUNTIME_SCHEMA_USED_KEY: "test_runtime_pruned_schema_2",
            },
        ]

        # Mock the return value of add_pruned_schema_to_bird_item
        mock_add_pruned_schema_to_bird_item.side_effect = processed_items
        mock_update_pipeline_configuration.return_value = {"mock": "args"}

        # Call the function to test
        result = process_items_with_pruned_schema_threaded(
            items=items,
            database_name="test_database_name",
            pipeline_args={"key": "val"},
            schema_selector_client="client",
        )

        # Assertions
        mock_set_database.assert_called_once_with("test_database_name")
        mock_update_pipeline_configuration.assert_called_once_with(
            "test_database_name", {"key": "val"}
        )
        self.assertEqual(result, processed_items)
        self.assertEqual(mock_add_pruned_schema_to_bird_item.call_count, 2)
        mock_logger.error.assert_not_called()

    @patch("preprocess.add_runtime_pruned_schema.set_database")
    @patch("preprocess.add_runtime_pruned_schema.update_pipeline_configuration")
    @patch("preprocess.add_runtime_pruned_schema.add_pruned_schema_to_bird_item")
    @patch("preprocess.add_runtime_pruned_schema.logger")
    def test_logs_error_when_item_fails(
        self,
        mock_logger,
        mock_add_pruned_schema_to_bird_item,
        mock_update_pipeline_configuration,
        mock_set_database,
    ):
        """Should log error and leave original item when one fails."""

        # Setup test data
        items = [
            {QUESTION_KEY: "test_question_1"},
            {QUESTION_KEY: "test_question_2"},
        ]
        success_item = {
            QUESTION_KEY: "test_question_1",
            RUNTIME_SCHEMA_USED_KEY: "test_runtime_pruned_schema_1",
        }
        expected_error_msg = ERROR_FAILED_ITEM.format(
            index=1, database_name="test_database_name", error="Test failure"
        )

        # Mock the return value of add_pruned_schema_to_bird_item
        mock_add_pruned_schema_to_bird_item.side_effect = [
            success_item,
            Exception("Test failure"),
        ]
        mock_update_pipeline_configuration.return_value = {}

        # Call the function to test
        result = process_items_with_pruned_schema_threaded(
            items=items,
            database_name="test_database_name",
            pipeline_args=None,
            schema_selector_client="client",
        )

        # Assertions
        self.assertEqual(result[0], success_item)
        self.assertEqual(result[1], items[1])
        mock_set_database.assert_called_once_with("test_database_name")
        self.assertEqual(mock_logger.error.call_count, 1)
        self.assertEqual(mock_logger.error.call_args[0][0], expected_error_msg)


class TestProcessEachDatabaseTestFileWithPrunedSchema(unittest.TestCase):
    """Tests for the process_each_database_test_file_with_pruned_schema function."""

    @patch("preprocess.add_runtime_pruned_schema.logger")
    @patch("preprocess.add_runtime_pruned_schema.save_json_to_file")
    @patch(
        "preprocess.add_runtime_pruned_schema.process_items_with_pruned_schema_threaded"
    )
    @patch("preprocess.add_runtime_pruned_schema.load_json_from_file")
    @patch("preprocess.add_runtime_pruned_schema.PATH_CONFIG.processed_test_path")
    @patch("preprocess.add_runtime_pruned_schema.get_database_list")
    def test_successful_processing(
        self,
        mock_get_database_list,
        mock_processed_test_path,
        mock_load_json,
        mock_process_items,
        mock_save_json,
        mock_logger,
    ):
        """Should process each database test file and save the results."""

        # Setup test data
        dataset_dir = Path("test_dataset_dir")

        # Mock function calls
        mock_get_database_list.return_value = ["test_database_1", "test_database_2"]
        mock_processed_test_path.side_effect = lambda database_name: Path(
            f"/fake/path/{database_name}.json"
        )
        mock_load_json.return_value = [{QUESTION_KEY: "test_question_1"}]
        mock_process_items.return_value = [
            {
                QUESTION_KEY: "test_question_1",
                RUNTIME_SCHEMA_USED_KEY: "test_runtime_pruned_schema_1",
            }
        ]

        # Call the function to test
        process_each_database_test_file_with_pruned_schema(
            dataset_dir,
            pipeline_args={"key": "val"},
            schema_selector_client="fake_client",
        )

        # Assertions
        self.assertEqual(mock_save_json.call_count, 2)
        mock_process_items.assert_any_call(
            [{QUESTION_KEY: "test_question_1"}],
            "test_database_1",
            {"key": "val"},
            "fake_client",
        )
        mock_logger.error.assert_not_called()

    @patch("preprocess.add_runtime_pruned_schema.logger")
    @patch("preprocess.add_runtime_pruned_schema.load_json_from_file")
    @patch("preprocess.add_runtime_pruned_schema.PATH_CONFIG.processed_test_path")
    @patch("preprocess.add_runtime_pruned_schema.get_database_list")
    def test_error_logged_on_failure(
        self,
        mock_get_database_list,
        mock_processed_test_path,
        mock_load_json,
        mock_logger,
    ):
        """Should log error when processing fails."""

        # Setup test data
        expected_error_msg = ERROR_PROCESSING_DATABASE_TEST_FILE.format(
            database_name="database_fail", error="Load failed"
        )

        # Mock function calls
        mock_get_database_list.return_value = ["database_fail"]
        mock_processed_test_path.return_value = Path("/fake/path/database_fail.json")
        mock_load_json.side_effect = Exception("Load failed")

        # Call the function to test
        process_each_database_test_file_with_pruned_schema(
            Path("irrelevant"), pipeline_args=None, schema_selector_client=None
        )

        # Assertions
        mock_logger.error.assert_called_once()
        self.assertEqual(mock_logger.error.call_args[0][0], expected_error_msg)


class TestProcessGlobalTestFileWithPrunedSchema(unittest.TestCase):
    """Tests for the process_global_test_file_with_pruned_schema function."""

    @patch("preprocess.add_runtime_pruned_schema.logger")
    @patch("preprocess.add_runtime_pruned_schema.save_json_to_file")
    @patch(
        "preprocess.add_runtime_pruned_schema.process_items_with_pruned_schema_threaded"
    )
    @patch("preprocess.add_runtime_pruned_schema.group_bird_items_by_database_name")
    @patch("preprocess.add_runtime_pruned_schema.load_json_from_file")
    def test_successful_grouped_processing(
        self,
        mock_load_json,
        mock_group_by_db,
        mock_process_items,
        mock_save_json,
        mock_logger,
    ):
        """Should process each database test file and save the results."""

        # Setup test data
        test_file = Path("/fake/test_file.json")

        # Mock function calls
        mock_load_json.return_value = ["mocked_raw_items"]
        mock_group_by_db.return_value = {
            "database_1": [{QUESTION_KEY: "test_question_1"}],
            "database_2": [{QUESTION_KEY: "test_question_2"}],
        }
        mock_process_items.side_effect = [
            [
                {
                    QUESTION_KEY: "test_question_1",
                    RUNTIME_SCHEMA_USED_KEY: "runtime_pruned_schema_1",
                }
            ],
            [
                {
                    QUESTION_KEY: "test_question_2",
                    RUNTIME_SCHEMA_USED_KEY: "runtime_pruned_schema_2",
                }
            ],
        ]

        # Call the function to test
        process_global_test_file_with_pruned_schema(
            test_file, pipeline_args={"key": "val"}, schema_selector_client="client"
        )

        # Assertions
        self.assertEqual(mock_process_items.call_count, 2)
        self.assertEqual(mock_save_json.call_count, 2)
        mock_logger.error.assert_not_called()

    @patch("preprocess.add_runtime_pruned_schema.logger")
    @patch(
        "preprocess.add_runtime_pruned_schema.process_items_with_pruned_schema_threaded",
        side_effect=Exception("Load failed"),
    )
    @patch("preprocess.add_runtime_pruned_schema.group_bird_items_by_database_name")
    @patch("preprocess.add_runtime_pruned_schema.load_json_from_file")
    def test_error_logged_on_processing_failure(
        self, mock_load_json, mock_group_by_db, mock_process_items, mock_logger
    ):
        """Should log error when processing fails."""

        # Setup test data
        test_file = Path("/fake/test_file.json")
        expected_error_msg = ERROR_PROCESSING_DATABASE_GLOBAL_FILE.format(
            database_name="database_1", error="Load failed"
        )

        # Mock function calls
        mock_load_json.return_value = ["fake_item"]
        mock_group_by_db.return_value = {
            "database_1": [{QUESTION_KEY: "test_question_1"}],
        }

        # Call the function to test
        process_global_test_file_with_pruned_schema(
            test_file=test_file, pipeline_args=None, schema_selector_client=None
        )

        # Assertions
        mock_logger.error.assert_called_once()
        self.assertEqual(mock_logger.error.call_args[0][0], expected_error_msg)


class TestRunPrunedSchemaAnnotationPipeline(unittest.TestCase):
    """Tests for the run_pruned_schema_annotation_pipeline function."""

    @patch("preprocess.add_runtime_pruned_schema.PATH_CONFIG.dataset_dir")
    @patch(
        "preprocess.add_runtime_pruned_schema.UPDATE_DATABASE_SPECIFIC_TEST_FILES", True
    )
    @patch(
        "preprocess.add_runtime_pruned_schema.SCHEMA_SELECTOR_CLIENT_CONFIG.to_client_args"
    )
    @patch("preprocess.add_runtime_pruned_schema.ClientFactory.get_client")
    @patch("preprocess.add_runtime_pruned_schema.build_pipeline_args_for_processing")
    @patch("preprocess.add_runtime_pruned_schema.ensure_global_bird_test_file_path")
    @patch(
        "preprocess.add_runtime_pruned_schema.process_global_test_file_with_pruned_schema"
    )
    @patch(
        "preprocess.add_runtime_pruned_schema.process_each_database_test_file_with_pruned_schema"
    )
    def test_runs_pipeline_with_update_database_specific_test_files(
        self,
        mock_process_each_database,
        mock_process_global,
        mock_ensure_global_test_file_path,
        mock_build_args,
        mock_get_client,
        mock_to_client_args,
        mock_dataset_dir,
    ):
        """Should run the pipeline with UPDATE_DATABASE_SPECIFIC_TEST_FILES = True."""

        # Setup test data
        pipeline_args = {"key": "value"}
        dataset_dir = Path("test_dataset_dir")

        # Mock function calls
        mock_client = MagicMock(name="Client")
        mock_to_client_args.return_value = {"client_arg1": "client_value1"}
        mock_get_client.return_value = mock_client
        mock_build_args.return_value = pipeline_args
        mock_dataset_dir.return_value = dataset_dir

        # Call the function to test
        run_pruned_schema_annotation_pipeline()

        # Assertions
        mock_to_client_args.assert_called_once()
        mock_get_client.assert_called_once_with(client_arg1="client_value1")
        mock_build_args.assert_called_once()
        mock_dataset_dir.assert_called_once()
        mock_process_each_database.assert_called_once_with(
            dataset_dir, pipeline_args, mock_client
        )
        mock_ensure_global_test_file_path.assert_not_called()
        mock_process_global.assert_not_called()

    @patch(
        "preprocess.add_runtime_pruned_schema.process_each_database_test_file_with_pruned_schema"
    )
    @patch(
        "preprocess.add_runtime_pruned_schema.process_global_test_file_with_pruned_schema"
    )
    @patch("preprocess.add_runtime_pruned_schema.ensure_global_bird_test_file_path")
    @patch("preprocess.add_runtime_pruned_schema.build_pipeline_args_for_processing")
    @patch("preprocess.add_runtime_pruned_schema.ClientFactory.get_client")
    @patch(
        "preprocess.add_runtime_pruned_schema.SCHEMA_SELECTOR_CLIENT_CONFIG.to_client_args"
    )
    @patch(
        "preprocess.add_runtime_pruned_schema.UPDATE_DATABASE_SPECIFIC_TEST_FILES",
        False,
    )
    @patch("preprocess.add_runtime_pruned_schema.PATH_CONFIG.processed_test_path")
    def test_runs_pipeline_without_update_database_specific_test_files(
        self,
        mock_processed_test_path,
        mock_to_client_args,
        mock_get_client,
        mock_build_pipeline_args,
        mock_ensure_global_test_file_path,
        mock_process_global_test_file,
        mock_process_each_database_test_file,
    ):
        """Should run the pipeline without update database specific test files enabled."""

        # Setup test data
        processed_test_path = Path("test_processed_test_path")
        global_test_file = "test_global_test_file"
        pipeline_args = {"key": "value"}

        # Mock function calls
        mock_client = MagicMock(name="Client")
        mock_get_client.return_value = mock_client
        mock_build_pipeline_args.return_value = pipeline_args
        mock_ensure_global_test_file_path.return_value = global_test_file
        mock_to_client_args.return_value = {"client_arg1": "client_value1"}
        mock_processed_test_path.return_value = processed_test_path

        # Call the function to test
        run_pruned_schema_annotation_pipeline()

        # Assertions
        mock_to_client_args.assert_called_once()
        mock_get_client.assert_called_once_with(client_arg1="client_value1")
        mock_build_pipeline_args.assert_called_once()
        mock_processed_test_path.assert_called_once_with(global_file=True)
        mock_ensure_global_test_file_path.assert_called_once_with(processed_test_path)
        mock_process_global_test_file.assert_called_once_with(
            global_test_file, pipeline_args, mock_client
        )
        mock_process_each_database_test_file.assert_not_called()


class TestMainFunction(unittest.TestCase):
    """Tests for the main function."""

    @patch("preprocess.add_runtime_pruned_schema.logger")
    @patch("preprocess.add_runtime_pruned_schema.run_pruned_schema_annotation_pipeline")
    def test_main_success(self, mock_run_pipeline, mock_logger):
        """Should run the pipeline without errors."""

        # Mock pipeline function
        mock_run_pipeline.return_value = None

        # Call the function to test
        main()

        # Assertions
        mock_run_pipeline.assert_called_once()
        mock_logger.error.assert_not_called()

    @patch("preprocess.add_runtime_pruned_schema.logger")
    @patch("preprocess.add_runtime_pruned_schema.run_pruned_schema_annotation_pipeline")
    def test_main_failure(self, mock_run_pipeline, mock_logger):
        """Should log error when pipeline fails."""

        # Mock pipeline function to raise an exception
        mock_run_pipeline.side_effect = Exception("Test Exception")

        # Call the function to test
        main()

        # Assertions
        mock_run_pipeline.assert_called_once()
        mock_logger.error.assert_called_once_with(
            ERROR_FAILED_TO_ADD_PRUNED_SCHEMA.format(error="Test Exception")
        )
