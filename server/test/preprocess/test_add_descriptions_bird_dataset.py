import json
import os
import sqlite3
import unittest
from pathlib import Path
from unittest.mock import MagicMock, call, mock_open, patch

import pandas as pd
# Import the functions from the main script
from preprocess.add_descriptions_bird_dataset import (
    DESCRIPTION_FILE_EXTENSION, SQL_GET_TABLE_INFO, SQL_SELECT_FIRST_ROW,
    TABLE_DESCRIPTION_PLACEHOLDER, add_database_descriptions,
    create_database_tables_csv, ensure_description_files_exist,
    extract_column_type_from_schema, get_improved_column_description,
    get_table_description, get_table_description_df,
    get_table_first_row_values, improve_column_descriptions,
    improve_column_descriptions_for_table, initialize_column_descriptions,
    is_column_description_file, read_csv, table_in_db_check,
    update_column_descriptions)
from preprocess.AddDescriptionErrorLogs import AddDescriptionErrorLogs
from services.base_client import Client
from utilities.constants.database_enums import DatasetType
from utilities.constants.LLM_enums import LLMType, ModelType
from utilities.constants.preprocess.add_descriptions_bird_dataset.indexing_constants import (
    COLUMN_DESCRIPTION_COL, DATA_FORMAT_COL, IMPROVED_COLUMN_DESCRIPTIONS_COL,
    ORIG_COLUMN_NAME_COL, TABLE_DESCRIPTION_COL, TABLE_NAME_COL)
from utilities.constants.preprocess.add_descriptions_bird_dataset.response_messages import (
    ERROR_COLUMN_DOES_NOT_EXIST, ERROR_COLUMN_MEANING_FILE_NOT_FOUND,
    ERROR_SQLITE_EXECUTION_ERROR, ERROR_UPDATING_DESCRIPTION_FILES,
    INFO_COLUMN_ALREADY_HAS_DESCRIPTIONS)
from utilities.constants.script_constants import UNKNOWN_COLUMN_DATA_TYPE_STR
from utilities.prompts.prompt_templates import (
    COLUMN_DESCRIPTION_PROMPT_TEMPLATE, TABLE_DESCRIPTION_PROMPT_TEMPLATE)


class TestAddDescriptionErrorLogs(unittest.TestCase):
    """Tests for the AddDescriptionErrorLogs singleton class."""

    def test_a_singleton_instance(self):
        """Test that only one instance of AddDescriptionErrorLogs is created."""
        instance1 = AddDescriptionErrorLogs()
        instance2 = AddDescriptionErrorLogs()
        self.assertIs(instance1, instance2)

    def test_b_errors_list_initialization(self):
        """Test that the errors list is initialized correctly."""
        instance = AddDescriptionErrorLogs()
        self.assertEqual(instance.errors, [])

    def test_c_add_errors(self):
        """Test that errors can be added to the errors list."""
        instance = AddDescriptionErrorLogs()
        error1 = {"message": "Error 1"}
        error2 = {"message": "Error 2"}
        instance.errors.append(error1)
        instance.errors.append(error2)
        self.assertEqual(instance.errors, [error1, error2])


class TestReadCsv(unittest.TestCase):
    """Tests for the read_csv function."""

    @patch("pandas.read_csv")
    def test_read_csv_success_utf8(self, mock_read_csv):
        """Test successful reading of CSV with utf-8-sig encoding."""
        mock_read_csv.return_value = pd.DataFrame()
        file_path = "test.csv"
        result = read_csv(file_path)
        mock_read_csv.assert_called_once_with(file_path, encoding="utf-8-sig")
        self.assertIsInstance(result, pd.DataFrame)

    @patch("pandas.read_csv")
    def test_read_csv_success_iso(self, mock_read_csv):
        """Test successful reading of CSV with ISO-8859-1 encoding."""
        mock_read_csv.side_effect = [
            UnicodeDecodeError("utf-8-sig", b"", 0, 1, ""),
            pd.DataFrame(),
        ]
        file_path = "test.csv"
        result = read_csv(file_path)
        self.assertEqual(mock_read_csv.call_count, 2)
        self.assertEqual(
            mock_read_csv.mock_calls[1], call(file_path, encoding="ISO-8859-1")
        )
        self.assertIsInstance(result, pd.DataFrame)

    @patch("pandas.read_csv")
    def test_read_csv_failure(self, mock_read_csv):
        """Test failure when all encodings fail."""
        mock_read_csv.side_effect = UnicodeDecodeError(
            "unknown", b"", 0, 1, "")
        file_path = "test.csv"
        with self.assertRaises(ValueError) as context:
            read_csv(file_path)
        self.assertIn(
            f"All encoding attempts failed for {file_path}", str(
                context.exception)
        )


class TestExtractColumnTypeFromSchema(unittest.TestCase):
    """Tests for the extract_column_type_from_schema function."""

    def setUp(self):
        """Set up a mock database connection."""
        self.connection = MagicMock()
        self.cursor = MagicMock()
        self.connection.cursor.return_value = self.cursor

    def test_column_found(self):
        """Test when the column is found in the schema."""
        self.cursor.fetchall.return_value = [
            (0, "col1", "TEXT", 0, None, 0),
            (1, "col2", "INTEGER", 1, None, 0),
        ]
        column_type = extract_column_type_from_schema(
            self.connection, "table1", "col2")
        self.assertEqual(column_type, "INTEGER")
        column_type = extract_column_type_from_schema(
            self.connection, "table1", "col1")
        self.assertEqual(column_type, "TEXT")

    def test_column_not_found(self):
        """Test when the column is not found in the schema."""
        self.cursor.fetchall.return_value = [
            (0, "col1", "TEXT", 0, None, 0),
            (1, "col2", "INTEGER", 1, None, 0),
        ]
        column_type = extract_column_type_from_schema(
            self.connection, "table1", "col3")
        self.assertEqual(column_type, UNKNOWN_COLUMN_DATA_TYPE_STR)

    def test_column_name_case_insensitive(self):
        """Test case-insensitive column name matching."""
        self.cursor.fetchall.return_value = [(0, "Col1", "TEXT", 0, None, 0)]
        column_type = extract_column_type_from_schema(
            self.connection, "table1", "col1")
        self.assertEqual(column_type, "TEXT")

    def test_cursor_closed(self):
        """Test that the cursor is closed."""
        self.cursor.fetchall.return_value = [(0, "col1", "TEXT", 0, None, 0)]
        extract_column_type_from_schema(self.connection, "table1", "col1")
        self.cursor.close.assert_called_once()

    def test_sqlite_error(self):
        """Test when a sqlite3.Error occurs during query execution."""
        self.cursor.execute.side_effect = sqlite3.Error("SQLite error")
        with self.assertRaises(RuntimeError) as context:
            extract_column_type_from_schema(self.connection, "table1", "col1")

        self.assertEqual(
            ERROR_SQLITE_EXECUTION_ERROR.format(
                sql=SQL_GET_TABLE_INFO.format(table_name="table1"),
                error="SQLite error",
            ),
            str(context.exception),
        )


class TestGetTableFirstRowValues(unittest.TestCase):
    """Tests for the get_table_first_row function."""

    def setUp(self):
        """Set up a mock database connection."""
        self.connection = MagicMock()
        self.cursor = MagicMock()
        self.connection.cursor.return_value = self.cursor

    def test_first_row_exists(self):
        """Test when the table has data."""
        self.cursor.fetchone.return_value = [1, "abc", None, 3.14]
        first_row = get_table_first_row_values(self.connection, "table1")
        self.assertEqual(first_row, ["1", "abc", "N/A", "3.14"])

    def test_table_empty(self):
        """Test when the table is empty."""
        self.cursor.fetchone.return_value = None
        first_row = get_table_first_row_values(self.connection, "table1")
        self.assertEqual(first_row, [])

    def test_cursor_closed(self):
        """Test that the cursor is closed."""
        self.cursor.fetchone.return_value = [1, "abc"]
        get_table_first_row_values(self.connection, "table1")
        self.cursor.close.assert_called_once()

    @patch("sqlite3.Connection")
    def test_get_table_first_row_sqlite_error(self, MockConnection):
        """
        Test that get_table_first_row raises a RuntimeError when a sqlite3.Error occurs.
        """
        mock_connection = MockConnection.return_value
        mock_cursor = mock_connection.cursor.return_value

        error = "Simulated SQLite error"
        mock_cursor.execute.side_effect = sqlite3.Error(error)

        table_name = "test_table"

        with self.assertRaises(RuntimeError) as context:
            get_table_first_row_values(mock_connection, table_name)

        self.assertEqual(
            str(context.exception),
            ERROR_SQLITE_EXECUTION_ERROR.format(
                sql=SQL_SELECT_FIRST_ROW.format(table_name=table_name), error=error
            ),
        )

    @patch("sqlite3.Connection")
    def test_cursor_closed_on_error(self, MockConnection):
        """
        Test that cursor.close() is called when a sqlite3.Error occurs.
        """
        mock_connection = MockConnection.return_value
        mock_cursor = mock_connection.cursor.return_value
        mock_cursor.execute.side_effect = sqlite3.Error(
            "Simulated SQLite error")

        table_name = "test_table"

        with self.assertRaises(RuntimeError) as context:
            get_table_first_row_values(mock_connection, table_name)

        self.assertIn(
            ERROR_SQLITE_EXECUTION_ERROR.format(
                sql=SQL_SELECT_FIRST_ROW.format(table_name=table_name),
                error="Simulated SQLite error",
            ),
            str(context.exception),
        )
        mock_cursor.close.assert_called_once()


class TestGetImprovedColumnDescription(unittest.TestCase):
    """Tests for the get_improved_coloumn_description function."""

    def setUp(self):
        """Set up mock objects and data."""
        self.mock_connection = MagicMock()
        self.mock_client = MagicMock()
        self.table_name = "test_table"
        self.table_row = ["1", "test_value"]
        self.database_name = "test_db"
        self.table_description = "This is a test table."

        self.sample_row = pd.Series(
            {
                ORIG_COLUMN_NAME_COL: "test_column",
                DATA_FORMAT_COL: "TEXT",
                COLUMN_DESCRIPTION_COL: "Original description.",
            }
        )

        error_logs = AddDescriptionErrorLogs()
        error_logs.errors = []

    def test_successful_description_generation(self):
        """Test successful generation of an improved description."""
        self.mock_client.execute_prompt.return_value = "Improved description."
        improved_description = get_improved_column_description(
            self.sample_row,
            self.table_name,
            self.table_row,
            self.mock_connection,
            self.mock_client,
            self.table_description,
            self.database_name,
        )
        self.assertEqual(improved_description, "Improved description.")
        self.mock_client.execute_prompt.assert_called_once()

    def test_column_type_from_schema(self):
        """Test fetching column type from schema when DATA_FORMAT is missing."""
        row = pd.Series(
            {
                ORIG_COLUMN_NAME_COL: "test_column",
                DATA_FORMAT_COL: None,
                COLUMN_DESCRIPTION_COL: "Original description.",
            }
        )
        self.mock_client.execute_prompt.return_value = "Improved description."
        with patch(
            "preprocess.add_descriptions_bird_dataset.extract_column_type_from_schema"
        ) as mock_extract_type:
            mock_extract_type.return_value = "INTEGER"
            get_improved_column_description(
                row,
                self.table_name,
                self.table_row,
                self.mock_connection,
                self.mock_client,
                self.table_description,
                self.database_name,
            )
            mock_extract_type.assert_called_once_with(
                self.mock_connection, self.table_name, "test_column"
            )

    def test_empty_column_description(self):
        """Test handling of an empty original column description."""
        row = pd.Series(
            {
                ORIG_COLUMN_NAME_COL: "test_column",
                DATA_FORMAT_COL: "TEXT",
                COLUMN_DESCRIPTION_COL: None,
            }
        )
        self.mock_client.execute_prompt.return_value = "Improved description."
        get_improved_column_description(
            row,
            self.table_name,
            self.table_row,
            self.mock_connection,
            self.mock_client,
            self.table_description,
            self.database_name,
        )
        self.mock_client.execute_prompt.assert_called_once()

    def test_prompt_content(self):
        """Test that the prompt is constructed correctly."""
        self.mock_client.execute_prompt.return_value = "Improved description."
        get_improved_column_description(
            self.sample_row,
            self.table_name,
            self.table_row,
            self.mock_connection,
            self.mock_client,
            self.table_description,
            self.database_name,
        )
        expected_prompt = COLUMN_DESCRIPTION_PROMPT_TEMPLATE.format(
            table_name=self.table_name,
            table_description=self.table_description,
            table_first_row_values=self.table_row,
            column_name="test_column",
            datatype="TEXT",
            column_comment_part="Column description: Original description.\n",
        )
        self.mock_client.execute_prompt.assert_called_once_with(
            expected_prompt)

    def test_error_handling(self):
        """Test error handling during prompt execution."""
        self.mock_client.execute_prompt.side_effect = Exception(
            "Prompt execution failed."
        )
        errors_to_fix = AddDescriptionErrorLogs()  # Reset the errors list
        improved_description = get_improved_column_description(
            self.sample_row,
            self.table_name,
            self.table_row,
            self.mock_connection,
            self.mock_client,
            self.table_description,
            self.database_name,
        )
        self.assertEqual(improved_description, "")  # Ensure a default return
        self.assertEqual(len(errors_to_fix.errors), 1)
        self.assertIn("Prompt execution failed.",
                      errors_to_fix.errors[0]["error"])


class TestTableInDbCheck(unittest.TestCase):
    """Tests for the table_in_db_check function."""

    def setUp(self):
        """Set up test data."""
        self.base_path = Path("/test/path")
        self.tables_in_database = ["table1", "table2"]
        self.database_name = "test_db"

    def test_table_exists(self):
        """Test when the table exists in the database."""
        result = table_in_db_check(
            "table1.csv", self.base_path, self.tables_in_database, self.database_name
        )
        self.assertIsNone(result)

    def test_table_does_not_exist(self):
        """Test when the table does not exist in the database."""
        result = table_in_db_check(
            "table3.csv", self.base_path, self.tables_in_database, self.database_name
        )
        self.assertIsNotNone(result)
        self.assertEqual(result["database"], self.database_name)
        self.assertIn("Table 'table3' does not exist", result["error"])

    def test_non_csv_file(self):
        """Test with a non-CSV file."""
        result = table_in_db_check(
            "table1.txt", self.base_path, self.tables_in_database, self.database_name
        )
        self.assertIsNone(result)


class TestIsColumnDescriptionFile(unittest.TestCase):
    """Tests for the should_process_file function."""

    def test_csv_file_not_tables_csv(self):
        """Test with a CSV file that is not the tables CSV."""
        self.assertTrue(is_column_description_file("data.csv", "test_db"))

    def test_csv_file_is_tables_csv(self):
        """Test with the database_name_tables.csv file."""
        self.assertFalse(is_column_description_file(
            "test_db_tables.csv", "test_db"))

    def test_non_csv_file(self):
        """Test with a non-CSV file."""
        self.assertFalse(is_column_description_file("data.txt", "test_db"))

    def test_empty_file_name(self):
        self.assertFalse(is_column_description_file("", "test_db"))


class TestGetTableDescriptionDf(unittest.TestCase):
    """Tests for the get_table_description_df function."""

    @patch("preprocess.add_descriptions_bird_dataset.read_csv")
    @patch(
        "preprocess.add_descriptions_bird_dataset.PATH_CONFIG.table_description_file"
    )
    def test_successful_retrieval(self, mock_table_description_file, mock_read_csv):
        """Test when the table description file is successfully read."""
        database_name = "test_db"
        file_path = "path/to/table_descriptions.csv"
        mock_table_description_file.return_value = file_path
        mock_df = pd.DataFrame(
            {"table_name": ["table1", "table2"],
                "description": ["desc1", "desc2"]}
        )
        mock_read_csv.return_value = mock_df

        result_df = get_table_description_df(database_name)

        self.assertTrue(result_df.equals(mock_df))
        mock_table_description_file.assert_called_once_with(
            database_name=database_name)
        mock_read_csv.assert_called_once_with(file_path)

    @patch(
        "preprocess.add_descriptions_bird_dataset.PATH_CONFIG.table_description_file"
    )
    def test_file_not_found(self, mock_table_description_file):
        """Test when the table description file is not found."""
        database_name = "test_db"
        file_path = "path/to/nonexistent_file.csv"
        mock_table_description_file.return_value = file_path

        with self.assertRaises(FileNotFoundError) as context:
            get_table_description_df(database_name)

        self.assertEqual(
            str(
                context.exception), f"Table description file not found: {file_path}"
        )
        mock_table_description_file.assert_called_once_with(
            database_name=database_name)

    @patch("preprocess.add_descriptions_bird_dataset.read_csv")
    @patch(
        "preprocess.add_descriptions_bird_dataset.PATH_CONFIG.table_description_file"
    )
    def test_empty_file(self, mock_table_description_file, mock_read_csv):
        """Test when the table description file is empty."""
        database_name = "test_db"
        file_path = "path/to/empty_file.csv"
        mock_table_description_file.return_value = file_path
        mock_read_csv.side_effect = pd.errors.EmptyDataError

        with self.assertRaises(ValueError) as context:
            get_table_description_df(database_name)

        self.assertEqual(
            str(
                context.exception), f"Table description file is empty: {file_path}"
        )
        mock_table_description_file.assert_called_once_with(
            database_name=database_name)
        mock_read_csv.assert_called_once_with(file_path)


class TestGetTableDescription(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        data = {
            TABLE_NAME_COL: ["birds", "animals", "plants"],
            TABLE_DESCRIPTION_COL: [
                "Birds dataset",
                "Animals dataset",
                "Plants dataset",
            ],
        }
        cls.sample_table_description_df = pd.DataFrame(data)

    def test_get_table_description_existing_table(self):
        table_name = "birds"
        expected_description = "Birds dataset"

        self.assertEqual(
            get_table_description(
                table_name, self.sample_table_description_df),
            expected_description,
        )

    def test_get_table_description_non_existing_table(self):
        table_name = "insects"
        expected_description = TABLE_DESCRIPTION_PLACEHOLDER

        self.assertEqual(
            get_table_description(
                table_name, self.sample_table_description_df),
            expected_description,
        )

    def test_get_table_description_empty_dataframe(self):
        table_name = "birds"
        empty_df = pd.DataFrame(
            columns=[TABLE_NAME_COL, TABLE_DESCRIPTION_COL])
        expected_description = TABLE_DESCRIPTION_PLACEHOLDER

        self.assertEqual(
            get_table_description(table_name, empty_df), expected_description
        )


class TestImproveColumnDescriptionsForTable(unittest.TestCase):

    @patch("preprocess.add_descriptions_bird_dataset.get_table_columns")
    @patch("preprocess.add_descriptions_bird_dataset.get_table_description")
    @patch("preprocess.add_descriptions_bird_dataset.get_table_first_row_values")
    @patch("preprocess.add_descriptions_bird_dataset.get_improved_column_description")
    @patch("preprocess.add_descriptions_bird_dataset.logger")
    def test_improve_column_descriptions_for_table(
        self,
        mock_logger,
        mock_get_improved_column_description,
        mock_get_table_first_row_values,
        mock_get_table_description,
        mock_get_table_columns,
    ):
        # Mock data
        table_df = pd.DataFrame(
            {
                ORIG_COLUMN_NAME_COL: ["col1", "col2", "col3"],
                IMPROVED_COLUMN_DESCRIPTIONS_COL: [None, "desc2", None],
            }
        )
        connection = MagicMock(spec=sqlite3.Connection)
        table_name = "test_table"
        database_name = "test_db"
        improvement_client = MagicMock()
        table_description_df = pd.DataFrame(
            {
                TABLE_NAME_COL: ["test_table"],
                TABLE_DESCRIPTION_COL: ["table description"],
            }
        )

        # Mock return values
        mock_get_table_columns.return_value = ["col1", "col2", "col3"]
        mock_get_table_description.return_value = "table description"
        mock_get_table_first_row_values.return_value = ["val1", "val2", "val3"]
        mock_get_improved_column_description.return_value = "improved description"

        # Call the function
        result_df = improve_column_descriptions_for_table(
            table_df,
            connection,
            table_name,
            database_name,
            improvement_client,
            table_description_df,
        )

        # Assertions
        self.assertEqual(
            result_df.loc[0,
                          IMPROVED_COLUMN_DESCRIPTIONS_COL], "improved description"
        )
        self.assertEqual(
            result_df.loc[1, IMPROVED_COLUMN_DESCRIPTIONS_COL], "desc2")
        self.assertEqual(
            result_df.loc[2,
                          IMPROVED_COLUMN_DESCRIPTIONS_COL], "improved description"
        )

        # Check if logger was called correctly
        mock_logger.info.assert_called_with(
            INFO_COLUMN_ALREADY_HAS_DESCRIPTIONS.format(column_name="col2")
        )
        mock_logger.error.assert_not_called()

    @patch("preprocess.add_descriptions_bird_dataset.get_table_columns")
    @patch("preprocess.add_descriptions_bird_dataset.get_table_description")
    @patch("preprocess.add_descriptions_bird_dataset.get_table_first_row_values")
    @patch("preprocess.add_descriptions_bird_dataset.get_improved_column_description")
    @patch("preprocess.add_descriptions_bird_dataset.logger")
    def test_improve_column_descriptions_for_table_column_not_exist(
        self,
        mock_logger,
        mock_get_improved_column_description,
        mock_get_table_first_row_values,
        mock_get_table_description,
        mock_get_table_columns,
    ):
        # Mock data
        table_df = pd.DataFrame(
            {
                ORIG_COLUMN_NAME_COL: ["col1", "col4"],
                IMPROVED_COLUMN_DESCRIPTIONS_COL: [None, None],
            }
        )
        connection = MagicMock(spec=sqlite3.Connection)
        table_name = "test_table"
        database_name = "test_db"
        improvement_client = MagicMock()
        table_description_df = pd.DataFrame(
            {
                TABLE_NAME_COL: ["test_table"],
                TABLE_DESCRIPTION_COL: ["table description"],
            }
        )

        # Mock return values
        mock_get_table_columns.return_value = ["col1", "col2", "col3"]
        mock_get_table_description.return_value = "table description"
        mock_get_table_first_row_values.return_value = ["val1", "val2", "val3"]
        mock_get_improved_column_description.return_value = "improved description"

        error_log_store = AddDescriptionErrorLogs()

        # Call the function
        result_df = improve_column_descriptions_for_table(
            table_df,
            connection,
            table_name,
            database_name,
            improvement_client,
            table_description_df,
        )

        # Assertions
        self.assertEqual(
            result_df.loc[0,
                          IMPROVED_COLUMN_DESCRIPTIONS_COL], "improved description"
        )
        self.assertIsNone(result_df.loc[1, IMPROVED_COLUMN_DESCRIPTIONS_COL])
        self.assertEqual(len(error_log_store.errors), 1)
        self.assertEqual(
            error_log_store.errors[0]["error"],
            ERROR_COLUMN_DOES_NOT_EXIST.format(
                column_name="col4", table_name=table_name
            ),
        )

        # Check if logger was called correctly
        mock_logger.error.assert_called_with(
            ERROR_COLUMN_DOES_NOT_EXIST.format(
                column_name="col4", table_name=table_name
            )
        )
        mock_logger.info.assert_not_called()


class TestImproveColumnDescriptions(unittest.TestCase):

    @patch("pandas.DataFrame.to_csv")
    @patch(
        "preprocess.add_descriptions_bird_dataset.improve_column_descriptions_for_table"
    )
    @patch("preprocess.add_descriptions_bird_dataset.read_csv")
    @patch("preprocess.add_descriptions_bird_dataset.table_in_db_check")
    @patch("preprocess.add_descriptions_bird_dataset.is_column_description_file")
    @patch("preprocess.add_descriptions_bird_dataset.os.listdir")
    @patch("preprocess.add_descriptions_bird_dataset.get_table_description_df")
    @patch("preprocess.add_descriptions_bird_dataset.get_table_names")
    @patch("preprocess.add_descriptions_bird_dataset.PATH_CONFIG.description_dir")
    def test_improve_column_descriptions(
        self,
        mock_description_dir,
        mock_get_table_names,
        mock_get_table_description_df,
        mock_listdir,
        mock_is_column_description_file,
        mock_table_in_db_check,
        mock_read_csv,
        mock_improve_column_descriptions_for_table,
        mock_to_csv,
    ):
        # Mock return values
        mock_description_dir.return_value = "/mock/path"
        mock_get_table_names.return_value = ["table1", "table2"]
        mock_get_table_description_df.return_value = pd.DataFrame()
        mock_listdir.return_value = ["table1.csv",
                                     "table2.csv", "test_db_tables.csv"]
        mock_is_column_description_file.side_effect = [True, True, False]
        mock_table_in_db_check.return_value = None
        mock_read_csv.return_value = pd.DataFrame({"column": ["col1", "col2"]})
        mock_improve_column_descriptions_for_table.return_value = pd.DataFrame(
            {"column": ["col1", "col2"]}
        )
        mock_to_csv.return_value = None

        # Mock database connection
        connection = MagicMock(spec=sqlite3.Connection)

        # Mock improvement client
        improvement_client = MagicMock(spec=Client)

        # Call the function
        improve_column_descriptions(
            database_name="test_db",
            dataset_type=DatasetType.BIRD_DEV,
            improvement_client=improvement_client,
            connection=connection,
        )

        # Assertions
        mock_description_dir.assert_called_once_with(
            database_name="test_db", dataset_type=DatasetType.BIRD_DEV
        )
        mock_get_table_names.assert_called_once_with(connection)
        mock_get_table_description_df.assert_called_once_with(
            database_name="test_db")
        mock_listdir.assert_called_once_with("/mock/path")
        self.assertEqual(mock_is_column_description_file.call_count, 3)
        self.assertEqual(mock_table_in_db_check.call_count, 2)
        self.assertEqual(mock_read_csv.call_count, 2)
        self.assertEqual(
            mock_improve_column_descriptions_for_table.call_count, 2)

    @patch("preprocess.add_descriptions_bird_dataset.PATH_CONFIG")
    @patch("preprocess.add_descriptions_bird_dataset.get_table_names")
    @patch("preprocess.add_descriptions_bird_dataset.get_table_description_df")
    @patch("preprocess.add_descriptions_bird_dataset.os.listdir")
    @patch("preprocess.add_descriptions_bird_dataset.is_column_description_file")
    @patch("preprocess.add_descriptions_bird_dataset.table_in_db_check")
    @patch("preprocess.add_descriptions_bird_dataset.read_csv")
    @patch(
        "preprocess.add_descriptions_bird_dataset.improve_column_descriptions_for_table"
    )
    def test_improve_column_descriptions_with_error(
        self,
        mock_improve_column_descriptions_for_table,
        mock_read_csv,
        mock_table_in_db_check,
        mock_is_column_description_file,
        mock_listdir,
        mock_get_table_description_df,
        mock_get_table_names,
        mock_PATH_CONFIG,
    ):
        # Mock return values
        mock_PATH_CONFIG.description_dir.return_value = "/mock/path"
        mock_get_table_names.return_value = ["table1", "table2"]
        mock_get_table_description_df.return_value = pd.DataFrame()
        mock_listdir.return_value = ["table1.csv", "table2.csv"]
        mock_listdir.side_effect = FileNotFoundError(
            "Simulated file not found error")
        mock_is_column_description_file.return_value = True
        mock_table_in_db_check.return_value = {
            "error": "Table not in database"}
        mock_read_csv.return_value = pd.DataFrame({"column": ["col1", "col2"]})
        mock_improve_column_descriptions_for_table.return_value = pd.DataFrame(
            {"column": ["col1", "col2"]}
        )

        # Mock database connection
        connection = MagicMock(spec=sqlite3.Connection)

        # Mock improvement client
        improvement_client = MagicMock(spec=Client)

        # Call the function and expect an exception
        with self.assertRaises(RuntimeError):
            improve_column_descriptions(
                database_name="test_db",
                dataset_type=DatasetType.BIRD_DEV,
                improvement_client=improvement_client,
                connection=connection,
            )

        # Assertions
        mock_PATH_CONFIG.description_dir.assert_called_once_with(
            database_name="test_db", dataset_type=DatasetType.BIRD_DEV
        )
        mock_get_table_names.assert_called_once_with(connection)
        mock_get_table_description_df.assert_called_once_with(
            database_name="test_db")
        mock_listdir.assert_called_once_with("/mock/path")


class TestCreateDatabaseTablesCsv(unittest.TestCase):
    """Tests for the create_database_tables_csv function."""

    def setUp(self):
        """Set up mock objects and data."""
        self.mock_connection = MagicMock()
        self.mock_client = MagicMock()
        self.database_name = "test_db"
        self.dataset_type = DatasetType.BIRD_DEV

        # Mock PATH_CONFIG.table_description_file
        self.patch_table_description_file = patch(
            "utilities.config.PATH_CONFIG.table_description_file",
            return_value="/test/path/test_db_tables.csv",
        )
        self.patch_table_description_file.start()
        self.addCleanup(self.patch_table_description_file.stop)

        # Mock get_table_names
        self.mock_get_table_names = MagicMock(
            return_value=["table1", "table2"])
        self.patch_get_table_names = patch(
            "preprocess.add_descriptions_bird_dataset.get_table_names",
            self.mock_get_table_names,
        )
        self.patch_get_table_names.start()
        self.addCleanup(self.patch_get_table_names.stop)

        # Mock get_table_ddl and get_table_first_row
        self.mock_get_table_ddl = MagicMock(side_effect=["DDL1", "DDL2"])
        self.patch_get_table_ddl = patch(
            "preprocess.add_descriptions_bird_dataset.get_table_ddl",
            self.mock_get_table_ddl,
        )
        self.patch_get_table_ddl.start()
        self.addCleanup(self.patch_get_table_ddl.stop)

        # Mock format_schema
        self.mock_format_schema = MagicMock(return_value="mocked_schema_ddl")
        self.patch_format_schema = patch(
            "preprocess.add_descriptions_bird_dataset.format_schema",
            self.mock_format_schema,
        )
        self.patch_format_schema.start()
        self.addCleanup(self.patch_format_schema.stop)

        # Mock get_table_first_row_values
        self.mock_get_table_first_row = MagicMock(
            side_effect=[["val1_1", "val1_2"], ["val2_1", "val2_2"]]
        )
        self.patch_get_table_first_row = patch(
            "preprocess.add_descriptions_bird_dataset.get_table_first_row_values",
            self.mock_get_table_first_row,
        )
        self.patch_get_table_first_row.start()
        self.addCleanup(self.patch_get_table_first_row.stop)

        # Mock execute_prompt
        self.mock_execute_prompt = MagicMock(
            side_effect=["Description 1", "Description 2"]
        )
        self.mock_client.execute_prompt = self.mock_execute_prompt

        # Mock read_csv and to_csv
        self.mock_read_csv = MagicMock(
            return_value=pd.DataFrame(
                columns=["table_name", "table_description"])
        )
        self.patch_read_csv = patch(
            "preprocess.add_descriptions_bird_dataset.read_csv", self.mock_read_csv
        )
        self.patch_read_csv.start()
        self.addCleanup(self.patch_read_csv.stop)
        self.mock_to_csv = MagicMock()
        self.patch_to_csv = patch("pandas.DataFrame.to_csv", self.mock_to_csv)
        self.patch_to_csv.start()
        self.addCleanup(self.patch_to_csv.stop)
        global errors_to_fix
        errors_to_fix = AddDescriptionErrorLogs()
        errors_to_fix.errors = []  # Reset global errors

    def test_a_successful_execution_no_existing_file(self):
        """Test successful execution when no existing CSV file."""
        create_database_tables_csv(
            self.database_name,
            self.dataset_type,
            self.mock_client,
            self.mock_connection,
        )

        # Check that read_csv is called.
        self.mock_read_csv.assert_called_once_with(
            "/test/path/test_db_tables.csv")
        # Check that get_table_names is called.
        self.mock_get_table_names.assert_called_once_with(self.mock_connection)
        # Check that get_table_ddl and get_table_first_row are called for each table.
        self.assertEqual(self.mock_get_table_ddl.call_count, 2)
        self.assertEqual(self.mock_get_table_first_row.call_count, 2)
        self.mock_execute_prompt.assert_has_calls(
            [
                call(
                    TABLE_DESCRIPTION_PROMPT_TEMPLATE.format(
                        schema_ddl="mocked_schema_ddl",
                        ddl="DDL1",
                        first_row=["val1_1", "val1_2"],
                    )
                ),
                call(
                    TABLE_DESCRIPTION_PROMPT_TEMPLATE.format(
                        schema_ddl="mocked_schema_ddl",
                        ddl="DDL2",
                        first_row=["val2_1", "val2_2"],
                    )
                ),
            ]
        )
        # Check that to_csv is called once.
        self.assertEqual(self.mock_to_csv.call_count, 2)

    def test_b_successful_execution_with_existing_file(self):
        """Test successful execution with an existing CSV file."""
        existing_df = pd.DataFrame(
            {
                "table_name": ["table1"],
                "table_description": ["Existing Description"],
            }
        )
        self.mock_read_csv.return_value = existing_df
        self.mock_execute_prompt.side_effect = ["New Description"]
        create_database_tables_csv(
            self.database_name,
            self.dataset_type,
            self.mock_client,
            self.mock_connection,
        )
        self.assertEqual(self.mock_read_csv.call_count, 1)
        self.assertEqual(self.mock_to_csv.call_count, 1)

    def test_c_table_already_has_description(self):
        """Test when table description already exists in csv."""
        existing_df = pd.DataFrame(
            {
                "table_name": ["table1", "table2"],
                "table_description": ["Existing Description", "Existing Description 2"],
            }
        )
        self.mock_read_csv.return_value = existing_df
        create_database_tables_csv(
            self.database_name,
            self.dataset_type,
            self.mock_client,
            self.mock_connection,
        )
        self.mock_execute_prompt.assert_not_called()
        self.mock_to_csv.assert_not_called()

    def test_d_error_handling(self):
        """Test error handling during prompt execution."""
        self.mock_execute_prompt.side_effect = [
            Exception("Prompt error1"),
            Exception("Prompt error2"),
        ]

        errors_to_fix = AddDescriptionErrorLogs()

        create_database_tables_csv(
            self.database_name,
            self.dataset_type,
            self.mock_client,
            self.mock_connection,
        )
        self.assertEqual(len(errors_to_fix.errors), 2)
        self.assertIn("Prompt error1", errors_to_fix.errors[0]["error"])
        self.assertIn("Prompt error2", errors_to_fix.errors[1]["error"])

    def test_e_error_runtime(self):
        """Test error handling during runtime."""
        self.mock_to_csv.side_effect = Exception("Mock Runtime error")

        errors_to_fix = AddDescriptionErrorLogs()

        with self.assertRaises(Exception) as context:
            create_database_tables_csv(
                self.database_name,
                self.dataset_type,
                self.mock_client,
                self.mock_connection,
            )
        self.assertIn("Mock Runtime error", str(context.exception))
        self.assertEqual(len(errors_to_fix.errors), 1)
        self.assertIn("Mock Runtime error", errors_to_fix.errors[0]["error"])


class TestInitializeColumnDescriptions(unittest.TestCase):

    def setUp(self):
        self.skip_tear_down = False

        self.database_description_path = Path("/tmp/descriptions")
        self.database_name = "test_db"
        self.column_meaning = {
            "test_db|table1|col1": "#Description for col1",
            "test_db|table1|col2": "Description for col2",
            "test_db|table2|col3": "Description for col3",
            "another_db|table1|col1": "Description for another_db",
        }

        self.mock_extract_column_type_from_schema = MagicMock(
            return_value="MOCK TYPE")
        self.mock_extract_column_type_from_schema_patch = patch(
            "preprocess.add_descriptions_bird_dataset.extract_column_type_from_schema",
            self.mock_extract_column_type_from_schema,
        )
        self.mock_extract_column_type_from_schema_patch.start()
        self.addCleanup(self.mock_extract_column_type_from_schema_patch.stop)

        self.mock_connection = MagicMock(spec=sqlite3.Connection)

    def tearDown(self):
        if not self.skip_tear_down:
            import shutil

            shutil.rmtree(self.database_description_path)

    def test_initialize_column_descriptions(self):
        initialize_column_descriptions(
            self.database_description_path,
            self.column_meaning,
            self.database_name,
            self.mock_connection,
        )

        # Assert that the files were created
        table1_csv_path = self.database_description_path / "table1.csv"
        table2_csv_path = self.database_description_path / "table2.csv"
        assert table1_csv_path.exists()
        assert table2_csv_path.exists()

        # Assert the contents of table1.csv
        expected_table1_df = pd.DataFrame(
            {
                ORIG_COLUMN_NAME_COL: ["col1", "col2"],
                DATA_FORMAT_COL: ["MOCK TYPE", "MOCK TYPE"],
                COLUMN_DESCRIPTION_COL: [
                    "Description for col1",
                    "Description for col2",
                ],
            }
        )
        actual_table1_df = pd.read_csv(table1_csv_path)
        pd.testing.assert_frame_equal(actual_table1_df, expected_table1_df)

        # Assert the contents of table2.csv
        expected_table2_df = pd.DataFrame(
            {
                ORIG_COLUMN_NAME_COL: ["col3"],
                DATA_FORMAT_COL: ["MOCK TYPE"],
                COLUMN_DESCRIPTION_COL: ["Description for col3"],
            }
        )
        actual_table2_df = pd.read_csv(table2_csv_path)
        print(actual_table2_df)
        print(expected_table2_df)
        pd.testing.assert_frame_equal(actual_table2_df, expected_table2_df)

    def test_initialize_column_descriptions_no_matching_database(self):
        initialize_column_descriptions(
            self.database_description_path,
            self.column_meaning,
            "non_existent_db",
            self.mock_connection,
        )

        # Assert that no files were created
        assert not any(self.database_description_path.iterdir())

    def test_initialize_column_descriptions_empty_column_meaning(self):
        initialize_column_descriptions(
            self.database_description_path, {}, self.database_name, self.mock_connection
        )

        # Assert that no files were created
        assert not any(self.database_description_path.iterdir())

    @patch("os.makedirs")
    def test_initialize_column_descriptions_error_handling(self, mock_makedirs):
        # Simulate an error during directory creation
        mock_makedirs.side_effect = OSError("Simulated error")

        with self.assertRaises(RuntimeError) as context:
            initialize_column_descriptions(
                self.database_description_path,
                self.column_meaning,
                self.database_name,
                self.mock_connection,
            )

        self.assertIn("Simulated error", str(context.exception))
        self.skip_tear_down = True  # Skip tearDown since the directory was not created


class TestUpdateColumnDescriptions(unittest.TestCase):
    def setUp(self):
        self.database_description_path = Path("/tmp/descriptions")
        os.makedirs(self.database_description_path, exist_ok=True)
        self.database_name = "bird_db"
        self.table_name = "observations"
        self.table_file_name = f"{self.table_name}{DESCRIPTION_FILE_EXTENSION}"
        self.table_file_path = os.path.join(
            self.database_description_path, self.table_file_name
        )
        self.tables_file_name = (
            f"{self.database_name}_tables{DESCRIPTION_FILE_EXTENSION}"
        )
        self.tables_file_path = os.path.join(
            self.database_description_path, self.tables_file_name
        )

    def tearDown(self):
        # Clean up the created directory and files
        import shutil

        shutil.rmtree(self.database_description_path)

    def create_mock_csv(self, file_path, data):
        df = pd.DataFrame(data)
        df.to_csv(file_path, index=False)

    def test_update_column_descriptions_success(self):
        # Arrange
        existing_data = {
            ORIG_COLUMN_NAME_COL: ["id", "species", "date"],
            COLUMN_DESCRIPTION_COL: ["", "Name of species", ""],
        }
        self.create_mock_csv(self.table_file_path, existing_data)

        column_meaning = {
            f"{self.database_name}|{self.table_name}|id": "Unique identifier",
            f"{self.database_name}|{self.table_name}|date": "Date of observation",
        }

        # Act
        update_column_descriptions(
            self.database_description_path, column_meaning, self.database_name
        )

        # Assert
        updated_df = pd.read_csv(self.table_file_path)
        expected_data = {
            ORIG_COLUMN_NAME_COL: ["id", "species", "date"],
            COLUMN_DESCRIPTION_COL: [
                "Unique identifier",
                "Name of species",
                "Date of observation",
            ],
        }
        expected_df = pd.DataFrame(expected_data)

        pd.testing.assert_frame_equal(updated_df, expected_df)

    def test_update_column_descriptions_no_update(self):
        # Arrange
        existing_data = {
            ORIG_COLUMN_NAME_COL: ["id", "species", "date"],
            COLUMN_DESCRIPTION_COL: [
                "Unique identifier",
                "Name of species",
                "Date of observation",
            ],
        }
        self.create_mock_csv(self.table_file_path, existing_data)

        column_meaning = {}

        # Act
        update_column_descriptions(
            self.database_description_path, column_meaning, self.database_name
        )

        # Assert
        updated_df = pd.read_csv(self.table_file_path)
        expected_df = pd.DataFrame(existing_data)
        pd.testing.assert_frame_equal(updated_df, expected_df)

    def test_update_column_descriptions_longer_description(self):
        # Arrange
        existing_data = {
            ORIG_COLUMN_NAME_COL: ["id"],
            COLUMN_DESCRIPTION_COL: ["Short description"],
        }
        self.create_mock_csv(self.table_file_path, existing_data)

        column_meaning = {
            f"{self.database_name}|{self.table_name}|id": "This is a very long description"
        }

        # Act
        update_column_descriptions(
            self.database_description_path, column_meaning, self.database_name
        )

        # Assert
        updated_df = pd.read_csv(self.table_file_path)
        expected_data = {
            ORIG_COLUMN_NAME_COL: ["id"],
            COLUMN_DESCRIPTION_COL: ["This is a very long description"],
        }
        expected_df = pd.DataFrame(expected_data)
        pd.testing.assert_frame_equal(updated_df, expected_df)

    def test_update_column_descriptions_file_not_found(self):
        # Arrange
        column_meaning = {
            f"{self.database_name}|{self.table_name}|id": "Unique identifier"
        }
        non_existent_path = Path("/tmp/non_existent")

        # Act & Assert
        with self.assertRaisesRegex(
            RuntimeError, ERROR_UPDATING_DESCRIPTION_FILES.format(error=".*")
        ):
            update_column_descriptions(
                non_existent_path, column_meaning, self.database_name
            )

    def test_update_column_descriptions_empty_description(self):
        # Arrange
        existing_data = {
            ORIG_COLUMN_NAME_COL: ["id", "species", "date"],
            COLUMN_DESCRIPTION_COL: [
                "Some description",
                "Name of species",
                "Another Description",
            ],
        }
        self.create_mock_csv(self.table_file_path, existing_data)

        column_meaning = {
            f"{self.database_name}|{self.table_name}|id": "",
            f"{self.database_name}|{self.table_name}|date": "   ",
        }

        # Act
        update_column_descriptions(
            self.database_description_path, column_meaning, self.database_name
        )

        # Assert
        updated_df = pd.read_csv(self.table_file_path)
        expected_data = {
            ORIG_COLUMN_NAME_COL: ["id", "species", "date"],
            COLUMN_DESCRIPTION_COL: [
                "Some description",
                "Name of species",
                "Another Description",
            ],
        }
        expected_df = pd.DataFrame(expected_data)

        pd.testing.assert_frame_equal(updated_df, expected_df)

    def test_update_column_descriptions_strips_hash(self):
        # Arrange
        existing_data = {
            ORIG_COLUMN_NAME_COL: ["id"],
            COLUMN_DESCRIPTION_COL: ["initial"],
        }
        self.create_mock_csv(self.table_file_path, existing_data)

        column_meaning = {
            f"{self.database_name}|{self.table_name}|id": "#  New Description  #"
        }

        # Act
        update_column_descriptions(
            self.database_description_path, column_meaning, self.database_name
        )

        # Assert
        updated_df = pd.read_csv(self.table_file_path)
        print(self.table_file_path)
        expected_data = {
            ORIG_COLUMN_NAME_COL: ["id"],
            COLUMN_DESCRIPTION_COL: ["New Description"],
        }
        expected_df = pd.DataFrame(expected_data)
        pd.testing.assert_frame_equal(updated_df, expected_df)


class TestEnsureDescriptionFilesExist(unittest.TestCase):
    """Tests for the ensure_description_files_exist function."""

    def setUp(self):
        """Set up mock objects and data."""
        self.database_name = "test_db"
        self.dataset_type = DatasetType.BIRD_DEV
        self.mock_connection = MagicMock()

        # Mock PATH_CONFIG
        self.patch_description_dir = patch(
            "utilities.config.PATH_CONFIG.description_dir",
            return_value=Path("/test/path"),
        )
        self.patch_description_dir.start()
        self.addCleanup(self.patch_description_dir.stop)
        self.patch_column_meaning_path = patch(
            "utilities.config.PATH_CONFIG.column_meaning_path",
            return_value="/test/path/column_meaning.json",
        )
        self.patch_column_meaning_path.start()
        self.addCleanup(self.patch_column_meaning_path.stop)

        # Mock json.load
        self.mock_json_load = MagicMock(return_value={"key": "value"})
        self.patch_json_load = patch("json.load", self.mock_json_load)
        self.patch_json_load.start()
        self.addCleanup(self.patch_json_load.stop)

        # Mock initialize_column_descriptions and update_column_descriptions
        self.mock_initialize_column_descriptions = MagicMock()
        self.patch_initialize_column_descriptions = patch(
            "preprocess.add_descriptions_bird_dataset.initialize_column_descriptions",
            self.mock_initialize_column_descriptions,
        )
        self.patch_initialize_column_descriptions.start()
        self.addCleanup(self.patch_initialize_column_descriptions.stop)

        self.mock_update_column_descriptions = MagicMock()
        self.patch_update_column_descriptions = patch(
            "preprocess.add_descriptions_bird_dataset.update_column_descriptions",
            self.mock_update_column_descriptions,
        )
        self.patch_update_column_descriptions.start()
        self.addCleanup(self.patch_update_column_descriptions.stop)

    @patch("builtins.open", new_callable=mock_open, read_data='{"key": "value"}')
    @patch("os.path.exists")
    def test_files_do_not_exist(self, mock_os_path_exists, mock_open_file):
        """Test when description files do not exist and column_meaning.json exists."""

        mock_os_path_exists.side_effect = [True, False]

        ensure_description_files_exist(
            self.database_name, self.dataset_type, self.mock_connection
        )
        self.mock_initialize_column_descriptions.assert_called_once()
        self.mock_update_column_descriptions.assert_not_called()

    @patch("builtins.open", new_callable=mock_open, read_data='{"key": "value"}')
    @patch("os.path.exists")
    def test_files_exist(self, mock_os_path_exists, mock_open_file):
        """Test when description files exist and column_meaning.json exists."""

        mock_os_path_exists.side_effect = [
            True,
            True,
            True,
        ]  # Simulate both paths exist

        ensure_description_files_exist(
            self.database_name, self.dataset_type, self.mock_connection
        )
        self.mock_initialize_column_descriptions.assert_not_called()
        self.mock_update_column_descriptions.assert_called_once()

    @patch("os.path.exists")
    def test_column_meaning_missing(self, mock_os_path_exists):
        """Test when column_meaning.json does not exist."""
        mock_os_path_exists.side_effect = [False, False, False]
        with self.assertRaises(RuntimeError) as context:
            ensure_description_files_exist(
                self.database_name, self.dataset_type, self.mock_connection
            )
        self.assertEqual(
            ERROR_COLUMN_MEANING_FILE_NOT_FOUND.format(
                file_path="/test/path/column_meaning.json"
            ),
            str(context.exception),
        )
        self.mock_initialize_column_descriptions.assert_not_called()
        self.mock_update_column_descriptions.assert_not_called()

    @patch("builtins.open", new_callable=mock_open, read_data='{"key": "value"}')
    @patch("os.path.exists")
    def test_json_load_error(self, mock_os_path_exists, mock_open):
        """Test error handling for invalid JSON."""
        self.mock_json_load.side_effect = json.JSONDecodeError(
            "Invalid JSON", "", 0)
        mock_os_path_exists.side_effect = [True, True]
        with self.assertRaises(RuntimeError) as context:
            ensure_description_files_exist(
                self.database_name, self.dataset_type, self.mock_connection
            )
        self.assertIn("Invalid JSON", str(context.exception))


class TestAddDatabaseDescriptions(unittest.TestCase):
    """Tests for the add_database_descriptions function."""

    def setUp(self):
        """Set up mock objects and data."""
        self.dataset_type = DatasetType.BIRD_DEV
        self.llm_type = LLMType.GOOGLE_AI
        self.model = ModelType.GOOGLEAI_GEMINI_2_0_FLASH
        self.temperature = 0.7
        self.max_tokens = 500

        # Mock PATH_CONFIG
        self.patch_dataset_dir = patch(
            "utilities.config.PATH_CONFIG.dataset_dir", return_value="/test/dataset"
        )
        self.patch_dataset_dir.start()
        self.addCleanup(self.patch_dataset_dir.stop)
        self.patch_sqlite_path = patch(
            "utilities.config.PATH_CONFIG.sqlite_path",
            return_value="/test/dataset/test_db/test_db.sqlite",
        )
        self.patch_sqlite_path.start()
        self.addCleanup(self.patch_sqlite_path.stop)

        # Mock os.listdir
        self.mock_listdir = MagicMock(return_value=["test_db", "other_db"])
        self.patch_listdir = patch("os.listdir", self.mock_listdir)
        self.patch_listdir.start()
        self.addCleanup(self.patch_listdir.stop)

        # Mock os.path.isdir
        self.mock_os_path_isdir = MagicMock(
            side_effect=[True, False]
        )  # Only "test_db" is a dir
        self.patch_os_path_isdir = patch(
            "os.path.isdir", self.mock_os_path_isdir)
        self.patch_os_path_isdir.start()
        self.addCleanup(self.patch_os_path_isdir.stop)

        # Mock ensure_description_files_exist, create_database_tables_csv, improve_column_descriptions
        self.mock_ensure_description_files_exist = MagicMock()
        self.patch_ensure_description_files_exist = patch(
            "preprocess.add_descriptions_bird_dataset.ensure_description_files_exist",
            self.mock_ensure_description_files_exist,
        )
        self.patch_ensure_description_files_exist.start()
        self.addCleanup(self.patch_ensure_description_files_exist.stop)

        self.mock_create_database_tables_csv = MagicMock()
        self.patch_create_database_tables_csv = patch(
            "preprocess.add_descriptions_bird_dataset.create_database_tables_csv",
            self.mock_create_database_tables_csv,
        )
        self.patch_create_database_tables_csv.start()
        self.addCleanup(self.patch_create_database_tables_csv.stop)

        self.mock_improve_column_descriptions = MagicMock()
        self.patch_improve_column_descriptions = patch(
            "preprocess.add_descriptions_bird_dataset.improve_column_descriptions",
            self.mock_improve_column_descriptions,
        )
        self.patch_improve_column_descriptions.start()
        self.addCleanup(self.patch_improve_column_descriptions.stop)

        errors_to_fix = AddDescriptionErrorLogs()
        errors_to_fix.errors = []  # Reset global errors

    @patch("sqlite3.connect")
    @patch("services.client_factory.ClientFactory.get_client")
    def test_successful_execution(self, mock_get_client, mock_sqlite_connect):
        """Test successful execution of the function."""

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_connection = MagicMock(spec=sqlite3.Connection)
        mock_sqlite_connect.return_value.__enter__.return_value = mock_connection

        add_database_descriptions(
            self.dataset_type,
            self.llm_type,
            self.model,
            self.temperature,
            self.max_tokens,
        )

        # Check that ClientFactory.get_client is called.
        mock_get_client.assert_called_once()

        # Check that sqlite3.connect is called with the correct path.
        mock_sqlite_connect.assert_called_once_with(
            "/test/dataset/test_db/test_db.sqlite"
        )

        # Check that the three main functions are called in the correct order.
        self.mock_ensure_description_files_exist.assert_called_once_with(
            database_name="test_db",
            dataset_type=self.dataset_type,
            connection=mock_connection,
        )
        self.mock_create_database_tables_csv.assert_called_once_with(
            database_name="test_db",
            dataset_type=self.dataset_type,
            client=mock_client,
            connection=mock_connection,
        )
        self.mock_improve_column_descriptions.assert_called_once_with(
            database_name="test_db",
            dataset_type=self.dataset_type,
            improvement_client=mock_client,
            connection=mock_connection,
        )

    @patch("preprocess.add_descriptions_bird_dataset.logger")
    @patch("sqlite3.connect")
    @patch("services.client_factory.ClientFactory.get_client")
    def test_exception_handling(
        self, mock_get_client, mock_sqlite_connect, mock_logger
    ):
        """Test handling of exceptions during database processing."""

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_connection = MagicMock(spec=sqlite3.Connection)
        mock_sqlite_connect.return_value.__enter__.return_value = mock_connection

        self.mock_ensure_description_files_exist.side_effect = Exception(
            "Failed to process database"
        )
        add_database_descriptions(
            self.dataset_type,
            self.llm_type,
            self.model,
            self.temperature,
            self.max_tokens,
        )

        mock_logger.error.assert_called_once_with(
            f"Error processing database test_db: Failed to process database",
            exc_info=True,
        )

    @patch("sqlite3.connect")
    @patch("os.listdir")
    def test_no_databases_found(self, mock_listdir, mock_sqlite_connect):
        """Test when no databases are found."""
        mock_listdir.return_value = []  # Simulate no databases
        mock_connection = MagicMock(spec=sqlite3.Connection)
        mock_sqlite_connect.return_value.__enter__.return_value = mock_connection

        add_database_descriptions(
            self.dataset_type,
            self.llm_type,
            self.model,
            self.temperature,
            self.max_tokens,
        )
        mock_sqlite_connect.assert_not_called()
        self.mock_ensure_description_files_exist.assert_not_called()
        self.mock_create_database_tables_csv.assert_not_called()
        self.mock_improve_column_descriptions.assert_not_called()


if __name__ == "__main__":
    """
    To run all test cases run the following from the server directory:
        pytest test/preprocess/test_add_descriptions_bird_dataset.py
    
    To run a single test case:
        pytest test/preprocess/test_add_descriptions_bird_dataset.py::SingleTestCaseName
    """
    unittest.main()
