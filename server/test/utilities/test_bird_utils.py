import json
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from utilities.bird_utils import (JSON_FILE_ENCODING,
                                  add_sequential_ids_to_questions,
                                  create_and_copy_test_file,
                                  ensure_global_bird_test_file_path,
                                  get_database_list,
                                  group_bird_items_by_database_name,
                                  load_json_from_file, save_json_to_file)
from utilities.constants.bird_utils.indexing_constants import (DB_ID_KEY,
                                                               QUESTION_ID_KEY,
                                                               QUESTION_KEY)
from utilities.constants.bird_utils.response_messages import (
    ERROR_EMPTY_BIRD_ITEMS_LIST, ERROR_FILE_DECODE, ERROR_FILE_NOT_FOUND,
    ERROR_FILE_READ, ERROR_FILE_SAVE, ERROR_JSON_DECODE, ERROR_MISSING_DB_ID,
    ERROR_PATH_NOT_DIRECTORY, ERROR_PATH_NOT_EXIST)
from utilities.constants.database_enums import DatasetType


class TestLoadJsonFromFile(unittest.TestCase):
    """Test suite for load_json_from_file function."""

    def setUp(self):
        self.mock_path = Path("test.json")

    @patch("pathlib.Path.read_text")
    def test_loads_valid_json_successfully(self, mock_read_text):
        """Should return parsed JSON when file content is valid."""

        # Set up data
        json_data = [{QUESTION_KEY: "test question"}]

        # Mock the read_text method to return valid JSON
        mock_read_text.return_value = json.dumps(json_data)

        # Call the function
        result = load_json_from_file(self.mock_path)

        # Assertions
        self.assertEqual(result, json_data)
        mock_read_text.assert_called_once_with(encoding=JSON_FILE_ENCODING)

    @patch(
        "pathlib.Path.read_text",
        side_effect=json.JSONDecodeError("Expecting value", "doc", 0),
    )
    def test_raises_value_error_on_invalid_json(self, mock_read_text):
        """Should raise ValueError when JSON is invalid."""

        # Mock the read_text method to return invalid JSON
        with self.assertRaises(ValueError) as context:
            load_json_from_file(self.mock_path)

        # Assertions
        self.assertIsInstance(context.exception, ValueError)
        self.assertIn(
            ERROR_JSON_DECODE.format(file_path=self.mock_path), str(context.exception)
        )

    @patch("pathlib.Path.read_text", side_effect=FileNotFoundError)
    def test_raises_file_not_found_error(self, mock_read_text):
        """Should raise FileNotFoundError when file doesn't exist."""

        # Mock the read_text method to raise FileNotFoundError
        with self.assertRaises(FileNotFoundError) as context:
            load_json_from_file(self.mock_path)

        # Assertions
        self.assertIsInstance(context.exception, FileNotFoundError)
        self.assertIn(
            ERROR_FILE_NOT_FOUND.format(file_path=self.mock_path),
            str(context.exception),
        )

    @patch(
        "pathlib.Path.read_text",
        side_effect=UnicodeDecodeError("utf-8", b"", 0, 1, "error reason"),
    )
    def test_raises_unicode_decode_error(self, mock_read_text):
        """Should raise UnicodeDecodeError when encoding fails."""

        # Mock the read_text method to raise UnicodeDecodeError
        with self.assertRaises(ValueError) as context:
            load_json_from_file(self.mock_path)

        # Assertions
        self.assertIsInstance(context.exception, ValueError)
        self.assertIn(
            ERROR_FILE_DECODE.format(file_path=self.mock_path, error="error reason"),
            str(context.exception),
        )

    @patch("pathlib.Path.read_text", side_effect=RuntimeError("some other error"))
    def test_raises_generic_exception(self, mock_read_text):
        """Should raise generic Exception on unexpected error."""

        # Mock the read_text method to raise a generic exception
        with self.assertRaises(RuntimeError) as context:
            load_json_from_file(self.mock_path)

        # Assertions
        self.assertIsInstance(context.exception, RuntimeError)
        self.assertIn(
            ERROR_FILE_READ.format(file_path=self.mock_path, error="some other error"),
            str(context.exception),
        )


class TestSaveJsonToFile(unittest.TestCase):
    """Test suite for save_json_to_file function."""

    def setUp(self):
        self.mock_path = Path("test.json")
        self.mock_data = [{QUESTION_KEY: "test question"}]

    @patch("pathlib.Path.write_text")
    def test_saves_json_successfully(self, mock_write_text):
        """Should write JSON data to file successfully."""

        # Call the function
        save_json_to_file(self.mock_path, self.mock_data)

        # Assertions
        mock_write_text.assert_called_once_with(
            json.dumps(self.mock_data, indent=4), encoding=JSON_FILE_ENCODING
        )

    @patch("pathlib.Path.write_text", side_effect=OSError("disk full"))
    def test_raises_runtime_error_on_os_error(self, mock_write_text):
        """Should raise RuntimeError if write_text throws an OSError."""

        # Mock the write_text method to raise OSError
        with self.assertRaises(RuntimeError) as context:
            save_json_to_file(self.mock_path, self.mock_data)

        # Assertions
        self.assertIn(
            ERROR_FILE_SAVE.format(file_path=self.mock_path, error="disk full"),
            str(context.exception),
        )

    @patch("json.dumps", side_effect=TypeError("data not serializable"))
    def test_raises_runtime_error_on_type_error(self, mock_write_text):
        """Should raise RuntimeError if data cannot be serialized."""

        # Mock the write_text method
        with self.assertRaises(RuntimeError) as context:
            save_json_to_file(self.mock_path, self.mock_data)

        # Assertions
        self.assertIn(
            ERROR_FILE_SAVE.format(
                file_path=self.mock_path, error="data not serializable"
            ),
            str(context.exception),
        )


class TestAddSequentialIdsToQuestions(unittest.TestCase):
    """Test suite for add_sequential_ids_to_questions function."""

    def setUp(self):
        self.mock_path = Path("test.json")

    @patch("utilities.bird_utils.save_json_to_file")
    @patch("utilities.bird_utils.load_json_from_file")
    def test_adds_sequential_ids_successfully(self, mock_load, mock_save):
        """Should annotate and save list with sequential question_ids."""

        # Set up data
        input_data = [
            {QUESTION_KEY: "test_question_1"},
            {QUESTION_KEY: "test_question_2"},
        ]
        expected_output = [
            {QUESTION_ID_KEY: 0, QUESTION_KEY: "test_question_1"},
            {QUESTION_ID_KEY: 1, QUESTION_KEY: "test_question_2"},
        ]

        # Mock return value
        mock_load.return_value = input_data

        # Call function
        add_sequential_ids_to_questions(self.mock_path)

        # Assert save called with annotated output
        mock_save.assert_called_once_with(self.mock_path, expected_output)

    @patch("utilities.bird_utils.load_json_from_file", return_value=[])
    def test_raises_value_error_on_empty_list(self, mock_load):
        """Should raise ValueError if input list is empty."""

        # Mock return value
        with self.assertRaises(ValueError) as context:
            add_sequential_ids_to_questions(self.mock_path)

        # Assertions
        self.assertIsInstance(context.exception, ValueError)
        self.assertIn(ERROR_EMPTY_BIRD_ITEMS_LIST, str(context.exception))

    @patch(
        "utilities.bird_utils.load_json_from_file",
        side_effect=RuntimeError("load failed"),
    )
    def test_propagates_errors_from_loader(self, mock_load):
        """Should re-raise errors from load_json_from_file."""

        # Mock return value
        with self.assertRaises(RuntimeError) as context:
            add_sequential_ids_to_questions(self.mock_path)

        # Assertions
        self.assertIsInstance(context.exception, RuntimeError)
        self.assertIn("load failed", str(context.exception))

    @patch(
        "utilities.bird_utils.save_json_to_file",
        side_effect=RuntimeError("save failed"),
    )
    @patch(
        "utilities.bird_utils.load_json_from_file",
        return_value=[{QUESTION_KEY: "test_question_1"}],
    )
    def test_propagates_errors_from_saver(self, mock_load, mock_save):
        """Should re-raise errors from save_json_to_file."""

        # Mock return value
        with self.assertRaises(RuntimeError) as context:
            add_sequential_ids_to_questions(self.mock_path)

        # Assertions
        self.assertIsInstance(context.exception, RuntimeError)
        self.assertIn("save failed", str(context.exception))


class TestGroupBirdItemsByDatabaseName(unittest.TestCase):
    """Test suite for group_bird_items_by_database_name function."""

    def test_groups_items_by_db_id(self):
        """Should group items correctly based on db_id."""

        # Set up data
        bird_items = [
            {DB_ID_KEY: "database_1", QUESTION_KEY: "test_question_1"},
            {DB_ID_KEY: "database_2", QUESTION_KEY: "test_question_2"},
            {DB_ID_KEY: "database_1", QUESTION_KEY: "test_question_3"},
        ]

        expected = {
            "database_1": [
                {DB_ID_KEY: "database_1", QUESTION_KEY: "test_question_1"},
                {DB_ID_KEY: "database_1", QUESTION_KEY: "test_question_3"},
            ],
            "database_2": [{DB_ID_KEY: "database_2", QUESTION_KEY: "test_question_2"}],
        }

        # Call function
        result = group_bird_items_by_database_name(bird_items)

        # Assertions
        self.assertEqual(result, expected)

    def test_raises_error_on_empty_list(self):
        """Should raise ValueError if bird_items is empty."""

        # Mock return value and Call function
        with self.assertRaises(ValueError) as context:
            group_bird_items_by_database_name([])

        # Assertions
        self.assertIn(ERROR_EMPTY_BIRD_ITEMS_LIST, str(context.exception))

    def test_raises_error_if_item_missing_db_id(self):
        """Should raise ValueError if any item is missing db_id."""

        # Set up data
        bird_items = [
            {DB_ID_KEY: "database_1", QUESTION_KEY: "test_question_1"},
            {QUESTION_KEY: "test_question_2"},  # Missing db_id
        ]

        # Mock return value and Call function
        with self.assertRaises(ValueError) as context:
            group_bird_items_by_database_name(bird_items)

        # Assertions
        self.assertIn(ERROR_MISSING_DB_ID.format(index=1), str(context.exception))


class TestGetDatabaseList(unittest.TestCase):
    """Test suite for get_database_list function."""

    def setUp(self):
        self.mock_dir = Path("/mock/dataset")

    @patch("pathlib.Path.iterdir")
    @patch("pathlib.Path.is_dir", return_value=True)
    @patch("pathlib.Path.exists", return_value=True)
    def test_returns_database_list(self, mock_exists, mock_is_dir, mock_iterdir):
        """Should return names of subdirectories."""

        # Set up mock subdirectories
        subdir1 = MagicMock(spec=Path)
        subdir1.name = "database_1"
        subdir1.is_dir.return_value = True

        subdir2 = MagicMock(spec=Path)
        subdir2.name = "database_2"
        subdir2.is_dir.return_value = True

        mock_iterdir.return_value = [subdir1, subdir2]

        # Call function
        result = get_database_list(self.mock_dir)

        # Assertions
        self.assertEqual(result, ["database_1", "database_2"])
        mock_exists.assert_called_once()
        mock_is_dir.assert_called_once()
        mock_iterdir.assert_called_once()

    @patch("pathlib.Path.exists", return_value=False)
    def test_raises_if_path_does_not_exist(self, mock_exists):
        """Should raise ValueError if path does not exist."""

        # Mock return value and Call function
        with self.assertRaises(ValueError) as context:
            get_database_list(self.mock_dir)

        # Assertions
        self.assertIn(
            ERROR_PATH_NOT_EXIST.format(dataset_directory=self.mock_dir),
            str(context.exception),
        )

    @patch("pathlib.Path.exists", return_value=True)
    @patch("pathlib.Path.is_dir", return_value=False)
    def test_raises_if_path_is_not_directory(self, mock_is_dir, mock_exists):
        """Should raise ValueError if path is not a directory."""

        # Mock return value and Call function
        with self.assertRaises(ValueError) as context:
            get_database_list(self.mock_dir)

        # Assertions
        self.assertIn(
            ERROR_PATH_NOT_DIRECTORY.format(dataset_directory=self.mock_dir),
            str(context.exception),
        )

    @patch("pathlib.Path.iterdir")
    @patch("pathlib.Path.is_dir", return_value=True)
    @patch("pathlib.Path.exists", return_value=True)
    def test_ignores_files_and_returns_only_dirs(
        self, mock_exists, mock_is_dir, mock_iterdir
    ):
        """Should skip files and return only subdirectory names."""

        # Set up mock subdirectories and files
        dir_entry = MagicMock(spec=Path)
        dir_entry.name = "database_directory"
        dir_entry.is_dir.return_value = True

        file_entry = MagicMock(spec=Path)
        file_entry.name = "not_a_databse_directory.txt"
        file_entry.is_dir.return_value = False

        mock_iterdir.return_value = [dir_entry, file_entry]

        # Call function
        result = get_database_list(self.mock_dir)

        # Assertions
        self.assertEqual(result, ["database_directory"])


class TestEnsureGlobalBirdTestFilePath(unittest.TestCase):
    """Test suite for ensure_global_bird_test_file_path function."""

    def setUp(self):
        self.test_file = Path("/mock/path/to/test_file.json")

    @patch("pathlib.Path.exists", return_value=True)
    def test_returns_path_if_file_exists(self, mock_exists):
        """Should return the file path as-is if the file already exists."""

        # Call the function
        result = ensure_global_bird_test_file_path(self.test_file)

        # Assertions
        self.assertEqual(result, self.test_file)
        mock_exists.assert_called_once()

    @patch("utilities.bird_utils.create_and_copy_test_file")
    @patch("pathlib.Path.exists", return_value=False)
    def test_creates_and_returns_path_if_missing(
        self, mock_exists, mock_create_and_copy
    ):
        """Should call create_and_copy_test_file if file does not exist."""

        # Mock return value
        mock_create_and_copy.return_value = self.test_file

        # Call the function
        result = ensure_global_bird_test_file_path(self.test_file)

        # Assertions
        mock_exists.assert_called_once()
        mock_create_and_copy.assert_called_once_with(self.test_file)
        self.assertEqual(result, self.test_file)


class TestCreateAndCopyTestFile(unittest.TestCase):
    """Test suite for create_and_copy_test_file function."""

    def setUp(self):
        self.test_file = Path("/mock/test/file.json")
        self.mock_source = Path("/mock/source/file.json")

    @patch("utilities.bird_utils.shutil.copy")
    @patch("utilities.bird_utils.Path.mkdir")
    @patch("utilities.bird_utils.PATH_CONFIG")
    def test_copies_file_successfully_and_returns_path(
        self, mock_config, mock_mkdir, mock_copy
    ):
        """Should copy from source to test file and return path."""
        # Mock return value
        mock_config.bird_file_path.return_value = self.mock_source
        mock_config.sample_dataset_type = DatasetType.BIRD_DEV

        # Call the function
        result = create_and_copy_test_file(self.test_file)

        # Assertions
        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
        mock_copy.assert_called_once_with(self.mock_source, self.test_file)
        self.assertEqual(result, self.test_file)

    @patch("utilities.bird_utils.add_sequential_ids_to_questions")
    @patch("utilities.bird_utils.shutil.copy")
    @patch("utilities.bird_utils.Path.mkdir")
    @patch("utilities.bird_utils.PATH_CONFIG")
    def test_adds_sequential_ids_if_dataset_is_train(
        self, mock_config, mock_mkdir, mock_copy, mock_annotate
    ):
        """Should annotate file if dataset type is BIRD_TRAIN."""

        # Mock return value
        mock_config.bird_file_path.return_value = self.mock_source
        mock_config.sample_dataset_type = DatasetType.BIRD_TRAIN

        # Call the function
        result = create_and_copy_test_file(self.test_file)

        # Assertions
        mock_annotate.assert_called_once_with(self.test_file)
        self.assertEqual(result, self.test_file)

    @patch("utilities.bird_utils.add_sequential_ids_to_questions")
    @patch("utilities.bird_utils.shutil.copy")
    @patch("utilities.bird_utils.Path.mkdir")
    @patch("utilities.bird_utils.PATH_CONFIG")
    def test_skips_annotation_if_dataset_is_not_train(
        self, mock_config, mock_mkdir, mock_copy, mock_annotate
    ):
        """Should not annotate file if dataset type is not BIRD_TRAIN."""

        # Mock return value
        mock_config.bird_file_path.return_value = self.mock_source
        mock_config.sample_dataset_type = DatasetType.BIRD_DEV

        # Call the function
        result = create_and_copy_test_file(self.test_file)

        # Assertions
        mock_annotate.assert_not_called()
        self.assertEqual(result, self.test_file)
