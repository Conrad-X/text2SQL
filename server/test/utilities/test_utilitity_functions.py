import datetime
import decimal
import unittest
from unittest.mock import patch

from utilities.constants.response_messages import (
    ERROR_FAILED_FETCH_COLUMN_TYPES, ERROR_FAILED_FETCH_COLUMN_VALUES)
from utilities.utility_functions import (GET_TABLE_COLUMN_VALUES_SQL,
                                         PRAGMA_TABLE_INFO_SQL,
                                         format_example_values,
                                         get_column_values,
                                         get_table_column_types, is_email)


class TestGetTableColumnTypes(unittest.TestCase):
    """Test suite for get_table_column_types function."""

    @patch("sqlite3.connect")
    def test_get_table_column_types_success(self, mock_connect):
        """Should return a dictionary of column names and types."""
        mock_connection = mock_connect.return_value
        mock_cursor = mock_connection.cursor.return_value
        mock_cursor.fetchall.return_value = [
            (0, "col1", "TEXT", 0, None, 0),
            (1, "col2", "INTEGER", 0, None, 0),
        ]

        expected_column_types = {"col1": "TEXT", "col2": "INTEGER"}
        column_types = get_table_column_types("test_table", mock_connection)
        self.assertEqual(column_types, expected_column_types)

        mock_cursor.execute.assert_called_once_with(
            PRAGMA_TABLE_INFO_SQL.format(table_name="test_table")
        )

    @patch("sqlite3.connect")
    def test_get_table_column_types_failure(self, mock_connect):
        """Should raise a RuntimeError when fetching column types fails."""
        mock_connection = mock_connect.return_value
        mock_cursor = mock_connection.cursor.return_value
        mock_cursor.execute.side_effect = Exception("Failed to fetch")

        with self.assertRaises(RuntimeError) as context:
            get_table_column_types("test_table", mock_connection)

        self.assertIn(ERROR_FAILED_FETCH_COLUMN_TYPES.split(" ")[0], str(context.exception).split(" ") [0])

class TestGetColumnValues(unittest.TestCase):
    """Test suite for get_column_values function."""

    @patch("sqlite3.connect")
    def test_get_column_values_success(self, mock_connect):
        """Should return a list of column values."""
        mock_connection = mock_connect.return_value
        mock_cursor = mock_connection.cursor.return_value
        mock_cursor.fetchall.return_value = [("value1",), ("value2",), ("value3",)]

        expected_values = ["value1", "value2", "value3"]
        values = get_column_values("test_column", "test_table", 3, mock_connection)
        self.assertEqual(values, expected_values)

        mock_cursor.execute.assert_called_once_with(
            GET_TABLE_COLUMN_VALUES_SQL.format(
                column_name="test_column", table_name="test_table", num_values=3
            )
        )

    @patch("sqlite3.connect")
    def test_get_column_values_failure(self, mock_connect):
        """Should raise a RuntimeError when fetching column values fails."""
        mock_connection = mock_connect.return_value
        mock_cursor = mock_connection.cursor.return_value
        mock_cursor.execute.side_effect = Exception("Failed to fetch")

        with self.assertRaises(RuntimeError) as context:
            get_column_values("test_column", "test_table", 3, mock_connection)

        self.assertIn(ERROR_FAILED_FETCH_COLUMN_VALUES.split(" ")[0], str(context.exception).split(" ")[0])


class TestIsEmail(unittest.TestCase):
    """Test suite for the is_email function."""

    def test_is_email_valid(self):
        """Should return True for valid email addresses."""
        self.assertTrue(is_email("test@example.com"))
        self.assertTrue(is_email("test.user@example.co.uk"))
        self.assertTrue(is_email("test_user123@example.net"))

    def test_is_email_invalid(self):
        """Should return False for invalid email addresses."""
        self.assertFalse(is_email("testexample.com"))
        self.assertFalse(is_email("test@example"))
        self.assertFalse(is_email("test@.com"))


class TestFormatExampleValues(unittest.TestCase):
    """Test suite for the format_example_values function."""

    def test_format_example_values_empty_list(self):
        """Should return an empty list when input is an empty list."""
        self.assertEqual(format_example_values([]), [])

    def test_format_example_values_with_dates(self):
        """Should return a list with the string representation of the date when dates are present."""
        dates = [datetime.date(2023, 1, 1), datetime.datetime(2023, 1, 2, 12, 0, 0)]
        self.assertEqual(format_example_values(dates), ['2023-01-01'])

    def test_format_example_values_with_decimals(self):
        """Should convert Decimal instances to floats."""
        decimals = [decimal.Decimal('3.14'), decimal.Decimal('2.71')]
        # Note: we expect the decimal values to be converted to floats and then strings, but
        # since the function modifies the values in place, we can only check for the float conversion here
        formatted_values = format_example_values(decimals)
        self.assertEqual(formatted_values, ['3.14', '2.71'])

    def test_format_example_values_with_emails_and_urls(self):
        """Should exclude emails and URLs."""
        values = ["test@example.com", "http://example.com", "https://example.com"]
        self.assertEqual(format_example_values(values), [])

    def test_format_example_values_with_none_and_empty_strings(self):
        """Should exclude None values and empty strings."""
        values = [None, "", "   ", "value"]
        self.assertEqual(format_example_values(values), ['value'])

    def test_format_example_values_with_mixed_values(self):
        """Should correctly format a list with mixed values."""
        mixed_values = [1, "string", 3.14, None, "  ", datetime.date(2023, 1, 1)]
        self.assertEqual(format_example_values(mixed_values), ['2023-01-01'])