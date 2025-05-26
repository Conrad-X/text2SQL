import sqlite3
import unittest
from typing import Dict
from unittest.mock import MagicMock, patch

import pytest
import yaml
from utilities.constants.common.indexing_constants import (
    COLUMNS_KEY, TABLE_DESCRIPTION_STR)
from utilities.constants.database_enums import DatasetType
from utilities.constants.prompts_enums import FormatType
from utilities.constants.utilities.format_schema.indexing_constants import (
    COLUMN_DESCRIPTION_KEY, COLUMN_EXAMPLES_KEY, COLUMN_NAME_KEY,
    COLUMN_PRIMARY_KEY, COLUMN_TYPE_KEY, FOREIGN_KEY_FROM_COLUMN_KEY,
    FOREIGN_KEY_TO_COLUMN_KEY, FOREIGN_KEY_TO_TABLE_KEY, TABLE_FOREIGN_KEY)
from utilities.constants.utilities.format_schema.response_messages import \
    ERROR_UNSUPPORTED_FORMAT_TYPE
from utilities.constants.utilities.format_schema.schema_templates import (
    BASIC_SCHEMA_LINE_ENTRY, CODE_REPR_SCHEMA_MISSING_SQL_ENTRY,
    OPENAI_SCHEMA_LINE_ENTRY, SEMANTIC_SCHEMA_DESCRIPTION_KEY,
    SEMANTIC_SCHEMA_TABLE_KEY, TEXT_SCHEMA_LINE_ENTRY)
from utilities.format_schema import (FOREIGN_KEY_FROM_COLUMN_KEY, basic_schema,
                                     code_repr_schema, construct_schema_config,
                                     create_column_schema_object,
                                     filter_table_foreign_keys_by_columns,
                                     format_schema, generate_mschema_columns,
                                     generate_mschema_foreign_keys,
                                     generate_mschema_table_entry,
                                     generate_schema, m_schema,
                                     map_linked_schema_to_original_schema,
                                     openai_schema,
                                     remove_errors_from_linked_schema,
                                     semantic_schema, text_schema)

UNKNOWN_COLUMN_TYPE = "UNKNOWN"


class TestFilterTableForeignKeysByColumns:
    @patch("utilities.format_schema.get_table_foreign_keys")
    def test_no_foreign_keys(self, mock_get_table_foreign_keys):
        mock_get_table_foreign_keys.return_value = []
        connection = MagicMock(())
        table_name = "table1"
        columns = ["col1", "col2"]
        result = filter_table_foreign_keys_by_columns(table_name, columns, connection)
        assert result == []

    @patch("utilities.format_schema.get_table_foreign_keys")
    def test_foreign_keys_present_but_not_in_columns(self, mock_get_table_foreign_keys):
        mock_get_table_foreign_keys.return_value = [
            {
                FOREIGN_KEY_TO_TABLE_KEY: "table2",
                FOREIGN_KEY_TO_COLUMN_KEY: "id",
                FOREIGN_KEY_FROM_COLUMN_KEY: "table2_id",
            }
        ]
        connection = MagicMock()
        table_name = "table1"
        columns = ["col1", "col2"]
        result = filter_table_foreign_keys_by_columns(table_name, columns, connection)
        assert result == []

    @patch("utilities.format_schema.get_table_foreign_keys")
    def test_foreign_keys_present_and_in_columns(self, mock_get_table_foreign_keys):
        mock_get_table_foreign_keys.return_value = [
            {
                FOREIGN_KEY_TO_TABLE_KEY: "table2",
                FOREIGN_KEY_TO_COLUMN_KEY: "id",
                FOREIGN_KEY_FROM_COLUMN_KEY: "col1",
            },
            {
                FOREIGN_KEY_TO_TABLE_KEY: "table3",
                FOREIGN_KEY_TO_COLUMN_KEY: "id",
                FOREIGN_KEY_FROM_COLUMN_KEY: "col2",
            },
            {
                FOREIGN_KEY_TO_TABLE_KEY: "table4",
                FOREIGN_KEY_TO_COLUMN_KEY: "id",
                FOREIGN_KEY_FROM_COLUMN_KEY: "col3",
            },
        ]
        connection = MagicMock()
        table_name = "table1"
        columns = ["col1", "col2"]
        expected = [
            {
                FOREIGN_KEY_TO_TABLE_KEY: "table2",
                FOREIGN_KEY_TO_COLUMN_KEY: "id",
                FOREIGN_KEY_FROM_COLUMN_KEY: "col1",
            },
            {
                FOREIGN_KEY_TO_TABLE_KEY: "table3",
                FOREIGN_KEY_TO_COLUMN_KEY: "id",
                FOREIGN_KEY_FROM_COLUMN_KEY: "col2",
            },
        ]
        result = filter_table_foreign_keys_by_columns(table_name, columns, connection)
        assert result == expected

    @patch("utilities.format_schema.get_table_foreign_keys")
    def test_empty_columns_list(self, mock_get_table_foreign_keys):
        mock_get_table_foreign_keys.return_value = [
            {
                FOREIGN_KEY_TO_TABLE_KEY: "table2",
                FOREIGN_KEY_TO_COLUMN_KEY: "id",
                FOREIGN_KEY_FROM_COLUMN_KEY: "col1",
            }
        ]
        connection = MagicMock()
        table_name = "table1"
        columns = []
        result = filter_table_foreign_keys_by_columns(table_name, columns, connection)
        assert result == []


class TestCreateColumnSchemaObject:
    @patch("utilities.format_schema.get_column_values")
    def test_create_column_schema_object(self, mock_get_column_values):
        mock_get_column_values.return_value = [1, 2, 3]
        connection = MagicMock()
        column_name = "col1"
        table_name = "table1"
        description_dict = {table_name: {COLUMNS_KEY: {column_name: "description"}}}
        table_column_types = {column_name: "INTEGER"}
        primary_key_dict = {table_name: [column_name]}
        expected = {
            COLUMN_NAME_KEY: column_name,
            COLUMN_DESCRIPTION_KEY: "description",
            COLUMN_TYPE_KEY: "INTEGER",
            COLUMN_EXAMPLES_KEY: ["1", "2", "3"],
            COLUMN_PRIMARY_KEY: True,
        }
        result = create_column_schema_object(
            column_name,
            table_name,
            description_dict,
            table_column_types,
            primary_key_dict,
            connection,
        )
        assert result == expected

    @patch("utilities.format_schema.get_column_values")
    def test_create_column_schema_object_no_description(self, mock_get_column_values):
        mock_get_column_values.return_value = [1, 2, 3]
        connection = MagicMock()
        column_name = "col1"
        table_name = "table1"
        description_dict = {table_name: {COLUMNS_KEY: {}}}
        table_column_types = {column_name: "INTEGER"}
        primary_key_dict = {table_name: [column_name]}
        with pytest.raises(KeyError):
            create_column_schema_object(
                column_name,
                table_name,
                description_dict,
                table_column_types,
                primary_key_dict,
                connection,
            )

    @patch("utilities.format_schema.get_column_values")
    def test_create_column_schema_object_no_column_type(self, mock_get_column_values):
        mock_get_column_values.return_value = [1, 2, 3]
        connection = MagicMock()
        column_name = "col1"
        table_name = "table1"
        description_dict = {table_name: {COLUMNS_KEY: {column_name: "description"}}}
        table_column_types = {}
        primary_key_dict = {table_name: [column_name]}
        expected = {
            COLUMN_NAME_KEY: column_name,
            COLUMN_DESCRIPTION_KEY: "description",
            COLUMN_TYPE_KEY: UNKNOWN_COLUMN_TYPE,
            COLUMN_EXAMPLES_KEY: ["1", "2", "3"],
            COLUMN_PRIMARY_KEY: True,
        }
        result = create_column_schema_object(
            column_name,
            table_name,
            description_dict,
            table_column_types,
            primary_key_dict,
            connection,
        )
        assert result == expected

    @patch("utilities.format_schema.get_column_values")
    def test_create_column_schema_object_not_primary_key(self, mock_get_column_values):
        mock_get_column_values.return_value = [1, 2, 3]
        connection = MagicMock()
        column_name = "col1"
        table_name = "table1"
        description_dict = {table_name: {COLUMNS_KEY: {column_name: "description"}}}
        table_column_types = {column_name: "INTEGER"}
        primary_key_dict = {table_name: []}
        expected = {
            COLUMN_NAME_KEY: column_name,
            COLUMN_DESCRIPTION_KEY: "description",
            COLUMN_TYPE_KEY: "INTEGER",
            COLUMN_EXAMPLES_KEY: ["1", "2", "3"],
            COLUMN_PRIMARY_KEY: False,
        }
        result = create_column_schema_object(
            column_name,
            table_name,
            description_dict,
            table_column_types,
            primary_key_dict,
            connection,
        )
        assert result == expected


class TestCreateColumnSchemaObject:
    @patch("utilities.format_schema.get_column_values")
    def test_create_column_schema_object(self, mock_get_column_values):
        mock_get_column_values.return_value = [1, 2, 3]
        connection = MagicMock()
        column_name = "col1"
        table_name = "table1"
        description_dict = {table_name: {COLUMNS_KEY: {column_name: "description"}}}
        table_column_types = {column_name: "INTEGER"}
        primary_key_dict = {table_name: [column_name]}
        expected = {
            COLUMN_NAME_KEY: column_name,
            COLUMN_DESCRIPTION_KEY: "description",
            COLUMN_TYPE_KEY: "INTEGER",
            COLUMN_EXAMPLES_KEY: ["1", "2", "3"],
            COLUMN_PRIMARY_KEY: True,
        }
        result = create_column_schema_object(
            column_name,
            table_name,
            description_dict,
            table_column_types,
            primary_key_dict,
            connection,
        )
        assert result == expected

    @patch("utilities.format_schema.get_column_values")
    def test_create_column_schema_object_no_description(self, mock_get_column_values):
        mock_get_column_values.return_value = [1, 2, 3]
        connection = MagicMock()
        column_name = "col1"
        table_name = "table1"
        description_dict = {table_name: {COLUMNS_KEY: {}}}
        table_column_types = {column_name: "INTEGER"}
        primary_key_dict = {table_name: [column_name]}
        with pytest.raises(KeyError):
            create_column_schema_object(
                column_name,
                table_name,
                description_dict,
                table_column_types,
                primary_key_dict,
                connection,
            )

    @patch("utilities.format_schema.get_column_values")
    def test_create_column_schema_object_no_column_type(self, mock_get_column_values):
        mock_get_column_values.return_value = [1, 2, 3]
        connection = MagicMock()
        column_name = "col1"
        table_name = "table1"
        description_dict = {table_name: {COLUMNS_KEY: {column_name: "description"}}}
        table_column_types = {}
        primary_key_dict = {table_name: [column_name]}
        expected = {
            COLUMN_NAME_KEY: column_name,
            COLUMN_DESCRIPTION_KEY: "description",
            COLUMN_TYPE_KEY: UNKNOWN_COLUMN_TYPE,
            COLUMN_EXAMPLES_KEY: ["1", "2", "3"],
            COLUMN_PRIMARY_KEY: True,
        }
        result = create_column_schema_object(
            column_name,
            table_name,
            description_dict,
            table_column_types,
            primary_key_dict,
            connection,
        )
        assert result == expected

    @patch("utilities.format_schema.get_column_values")
    def test_create_column_schema_object_not_primary_key(self, mock_get_column_values):
        mock_get_column_values.return_value = [1, 2, 3]
        connection = MagicMock()
        column_name = "col1"
        table_name = "table1"
        description_dict = {table_name: {COLUMNS_KEY: {column_name: "description"}}}
        table_column_types = {column_name: "INTEGER"}
        primary_key_dict = {table_name: []}
        expected = {
            COLUMN_NAME_KEY: column_name,
            COLUMN_DESCRIPTION_KEY: "description",
            COLUMN_TYPE_KEY: "INTEGER",
            COLUMN_EXAMPLES_KEY: ["1", "2", "3"],
            COLUMN_PRIMARY_KEY: False,
        }
        result = create_column_schema_object(
            column_name,
            table_name,
            description_dict,
            table_column_types,
            primary_key_dict,
            connection,
        )
        assert result == expected


class TestConstructSchemaConfig:
    @patch("utilities.format_schema.get_primary_keys")
    @patch("utilities.format_schema.get_table_column_types")
    @patch("utilities.format_schema.filter_table_foreign_keys_by_columns")
    @patch("utilities.format_schema.create_column_schema_object")
    def test_construct_schema_config(
        self,
        mock_create_column_schema_object,
        mock_filter_table_foreign_keys_by_columns,
        mock_get_table_column_types,
        mock_get_primary_keys,
    ):
        # Mock the return values of the called functions
        mock_get_primary_keys.return_value = {"table1": ["col1"]}
        mock_get_table_column_types.return_value = {"col1": "INTEGER"}
        mock_filter_table_foreign_keys_by_columns.return_value = []
        mock_create_column_schema_object.return_value = {
            COLUMN_NAME_KEY: "col1",
            COLUMN_DESCRIPTION_KEY: "description",
            COLUMN_TYPE_KEY: "INTEGER",
            COLUMN_EXAMPLES_KEY: [],
            COLUMN_PRIMARY_KEY: True,
        }

        # Define the input parameters for construct_schema_config
        schema_dict = {"table1": ["col1"]}
        description_dict = {
            "table1": {
                TABLE_DESCRIPTION_STR: "table description",
                COLUMNS_KEY: {"col1": "description"},
            }
        }
        connection = MagicMock()

        # Define the expected output
        expected = {
            "table1": {
                TABLE_DESCRIPTION_STR: "table description",
                COLUMNS_KEY: [
                    {
                        COLUMN_NAME_KEY: "col1",
                        COLUMN_DESCRIPTION_KEY: "description",
                        COLUMN_TYPE_KEY: "INTEGER",
                        COLUMN_EXAMPLES_KEY: [],
                        COLUMN_PRIMARY_KEY: True,
                    }
                ],
                TABLE_FOREIGN_KEY: [],
            }
        }

        # Call the function and assert the result
        result = construct_schema_config(schema_dict, description_dict, connection)
        assert result == expected

    @patch("utilities.format_schema.get_primary_keys")
    @patch("utilities.format_schema.get_table_column_types")
    @patch("utilities.format_schema.filter_table_foreign_keys_by_columns")
    @patch("utilities.format_schema.create_column_schema_object")
    def test_construct_schema_config_multiple_columns(
        self,
        mock_create_column_schema_object,
        mock_filter_table_foreign_keys_by_columns,
        mock_get_table_column_types,
        mock_get_primary_keys,
    ):
        # Mock the return values of the called functions
        mock_get_primary_keys.return_value = {"table1": ["col1"]}
        mock_get_table_column_types.return_value = {"col1": "INTEGER", "col2": "TEXT"}
        mock_filter_table_foreign_keys_by_columns.return_value = []
        mock_create_column_schema_object.side_effect = [
            {
                COLUMN_NAME_KEY: "col1",
                COLUMN_DESCRIPTION_KEY: "description1",
                COLUMN_TYPE_KEY: "INTEGER",
                COLUMN_EXAMPLES_KEY: [],
                COLUMN_PRIMARY_KEY: True,
            },
            {
                COLUMN_NAME_KEY: "col2",
                COLUMN_DESCRIPTION_KEY: "description2",
                COLUMN_TYPE_KEY: "TEXT",
                COLUMN_EXAMPLES_KEY: [],
                COLUMN_PRIMARY_KEY: False,
            },
        ]

        # Define the input parameters for construct_schema_config
        schema_dict = {"table1": ["col1", "col2"]}
        description_dict = {
            "table1": {
                TABLE_DESCRIPTION_STR: "table description",
                COLUMNS_KEY: {"col1": "description1", "col2": "description2"},
            }
        }
        connection = MagicMock()

        # Define the expected output
        expected = {
            "table1": {
                TABLE_DESCRIPTION_STR: "table description",
                COLUMNS_KEY: [
                    {
                        COLUMN_NAME_KEY: "col1",
                        COLUMN_DESCRIPTION_KEY: "description1",
                        COLUMN_TYPE_KEY: "INTEGER",
                        COLUMN_EXAMPLES_KEY: [],
                        COLUMN_PRIMARY_KEY: True,
                    },
                    {
                        COLUMN_NAME_KEY: "col2",
                        COLUMN_DESCRIPTION_KEY: "description2",
                        COLUMN_TYPE_KEY: "TEXT",
                        COLUMN_EXAMPLES_KEY: [],
                        COLUMN_PRIMARY_KEY: False,
                    },
                ],
                TABLE_FOREIGN_KEY: [],
            }
        }

        # Call the function and assert the result
        result = construct_schema_config(schema_dict, description_dict, connection)
        assert result == expected

    @patch("utilities.format_schema.get_primary_keys")
    @patch("utilities.format_schema.get_table_column_types")
    @patch("utilities.format_schema.filter_table_foreign_keys_by_columns")
    @patch("utilities.format_schema.create_column_schema_object")
    def test_construct_schema_config_no_primary_key(
        self,
        mock_create_column_schema_object,
        mock_filter_table_foreign_keys_by_columns,
        mock_get_table_column_types,
        mock_get_primary_keys,
    ):
        # Mock the return values of the called functions
        mock_get_primary_keys.return_value = {"table1": []}
        mock_get_table_column_types.return_value = {"col1": "INTEGER"}
        mock_filter_table_foreign_keys_by_columns.return_value = []
        mock_create_column_schema_object.return_value = {
            COLUMN_NAME_KEY: "col1",
            COLUMN_DESCRIPTION_KEY: "description",
            COLUMN_TYPE_KEY: "INTEGER",
            COLUMN_EXAMPLES_KEY: [],
            COLUMN_PRIMARY_KEY: False,
        }

        # Define the input parameters for construct_schema_config
        schema_dict = {"table1": ["col1"]}
        description_dict = {
            "table1": {
                TABLE_DESCRIPTION_STR: "table description",
                COLUMNS_KEY: {"col1": "description"},
            }
        }
        connection = MagicMock()

        # Define the expected output
        expected = {
            "table1": {
                TABLE_DESCRIPTION_STR: "table description",
                COLUMNS_KEY: [
                    {
                        COLUMN_NAME_KEY: "col1",
                        COLUMN_DESCRIPTION_KEY: "description",
                        COLUMN_TYPE_KEY: "INTEGER",
                        COLUMN_EXAMPLES_KEY: [],
                        COLUMN_PRIMARY_KEY: False,
                    }
                ],
                TABLE_FOREIGN_KEY: [],
            }
        }

        # Call the function and assert the result
        result = construct_schema_config(schema_dict, description_dict, connection)
        assert result == expected

    @patch("utilities.format_schema.get_primary_keys")
    @patch("utilities.format_schema.get_table_column_types")
    @patch("utilities.format_schema.filter_table_foreign_keys_by_columns")
    @patch("utilities.format_schema.create_column_schema_object")
    def test_construct_schema_config_with_foreign_keys(
        self,
        mock_create_column_schema_object,
        mock_filter_table_foreign_keys_by_columns,
        mock_get_table_column_types,
        mock_get_primary_keys,
    ):
        # Mock the return values of the called functions
        mock_get_primary_keys.return_value = {"table1": ["col1"]}
        mock_get_table_column_types.return_value = {"col1": "INTEGER"}
        mock_filter_table_foreign_keys_by_columns.return_value = [
            {"to_table": "table2", "to_column": "col2", "from_column": "col1"}
        ]
        mock_create_column_schema_object.return_value = {
            COLUMN_NAME_KEY: "col1",
            COLUMN_DESCRIPTION_KEY: "description",
            COLUMN_TYPE_KEY: "INTEGER",
            COLUMN_EXAMPLES_KEY: [],
            COLUMN_PRIMARY_KEY: True,
        }

        # Define the input parameters for construct_schema_config
        schema_dict = {"table1": ["col1"]}
        description_dict = {
            "table1": {
                TABLE_DESCRIPTION_STR: "table description",
                COLUMNS_KEY: {"col1": "description"},
            }
        }
        connection = MagicMock()

        # Define the expected output
        expected = {
            "table1": {
                TABLE_DESCRIPTION_STR: "table description",
                COLUMNS_KEY: [
                    {
                        COLUMN_NAME_KEY: "col1",
                        COLUMN_DESCRIPTION_KEY: "description",
                        COLUMN_TYPE_KEY: "INTEGER",
                        COLUMN_EXAMPLES_KEY: [],
                        COLUMN_PRIMARY_KEY: True,
                    }
                ],
                TABLE_FOREIGN_KEY: [
                    {"to_table": "table2", "to_column": "col2", "from_column": "col1"}
                ],
            }
        }

        # Call the function and assert the result
        result = construct_schema_config(schema_dict, description_dict, connection)
        assert result == expected


class TestRemoveErrorsFromLinkedSchema:
    def test_remove_errors_from_linked_schema(self):
        linked_schema = {
            "table1": ["col1", "col2", "col3"],
            "table2": ["col4", "col5"],
        }
        schema_dict = {
            "table1": ["col1", "col2"],
            "table3": ["col6", "col7"],
        }
        expected = {
            "table1": ["col1", "col2"],
        }
        result = remove_errors_from_linked_schema(linked_schema, schema_dict)
        assert result == expected

    def test_remove_errors_from_linked_schema_case_insensitive(self):
        linked_schema = {
            "Table1": ["Col1", "Col2", "Col3"],
            "Table2": ["Col4", "Col5"],
        }
        schema_dict = {
            "table1": ["col1", "col2"],
            "table3": ["col6", "col7"],
        }
        expected = {
            "Table1": ["Col1", "Col2"],
        }
        result = remove_errors_from_linked_schema(linked_schema, schema_dict)
        assert result == expected

    def test_remove_errors_from_linked_schema_no_errors(self):
        linked_schema = {
            "table1": ["col1", "col2"],
            "table2": ["col4", "col5"],
        }
        schema_dict = {
            "table1": ["col1", "col2"],
            "table2": ["col4", "col5"],
        }
        expected = {
            "table1": ["col1", "col2"],
            "table2": ["col4", "col5"],
        }
        result = remove_errors_from_linked_schema(linked_schema, schema_dict)
        assert result == expected

    def test_remove_errors_from_linked_schema_empty_linked_schema(self):
        linked_schema = {}
        schema_dict = {
            "table1": ["col1", "col2"],
            "table2": ["col4", "col5"],
        }
        expected = {}
        result = remove_errors_from_linked_schema(linked_schema, schema_dict)
        assert result == expected

    def test_remove_errors_from_linked_schema_empty_schema_dict(self):
        linked_schema = {
            "table1": ["col1", "col2"],
            "table2": ["col4", "col5"],
        }
        schema_dict = {}
        expected = {}
        result = remove_errors_from_linked_schema(linked_schema, schema_dict)
        assert result == expected

    def test_remove_errors_from_linked_schema_empty_values(self):
        linked_schema = {
            "table1": [],
            "table2": [],
        }
        schema_dict = {
            "table1": ["col1", "col2"],
            "table2": ["col4", "col5"],
        }
        expected = {}
        result = remove_errors_from_linked_schema(linked_schema, schema_dict)
        assert result == expected

    
class TestMapLinkedSchemaToOriginalSchema:
    def test_map_linked_schema_to_original_schema(self):
        linked_schema = {
            "table1": ["col1", "col2"],
            "table2": ["col4", "col5"],
        }
        schema_dict = {
            "table1": ["col1", "col2"],
            "table2": ["col4", "col5"],
        }
        expected = {
            "table1": ["col1", "col2"],
            "table2": ["col4", "col5"],
        }
        result = map_linked_schema_to_original_schema(linked_schema, schema_dict)
        assert result == expected

    def test_map_linked_schema_to_original_schema_case_insensitive(self):
        linked_schema = {
            "Table1": ["Col1", "Col2"],
            "Table2": ["Col4", "Col5"],
        }
        schema_dict = {
            "table1": ["col1", "col2"],
            "table2": ["col4", "col5"],
        }
        expected = {
            "table1": ["col1", "col2"],
            "table2": ["col4", "col5"],
        }
        result = map_linked_schema_to_original_schema(linked_schema, schema_dict)
        assert result == expected

    def test_map_linked_schema_to_original_schema_mixed_case(self):
        linked_schema = {
            "Table1": ["Col1", "col2"],
            "table2": ["col4", "Col5"],
        }
        schema_dict = {
            "table1": ["col1", "Col2"],
            "Table2": ["COL4", "col5"],
        }
        expected = {
            "table1": ["col1", "Col2"],
            "Table2": ["COL4", "col5"],
        }
        result = map_linked_schema_to_original_schema(linked_schema, schema_dict)
        assert result == expected

    def test_map_linked_schema_to_original_schema_subset(self):
        linked_schema = {
            "Table1": ["Col1"],
        }
        schema_dict = {
            "table1": ["col1", "col2"],
            "Table2": ["COL4", "col5"],
        }
        expected = {
            "table1": ["col1"],
        }
        result = map_linked_schema_to_original_schema(linked_schema, schema_dict)
        assert result == expected

    def test_map_linked_schema_to_original_schema_empty_linked_schema(self):
        linked_schema = {}
        schema_dict = {
            "table1": ["col1", "col2"],
            "Table2": ["COL4", "col5"],
        }
        expected = {}
        result = map_linked_schema_to_original_schema(linked_schema, schema_dict)
        assert result == expected


    def test_map_linked_schema_to_original_schema_no_match(self):
        linked_schema = {
            "Table3": ["Col6"],
        }
        schema_dict = {
            "table1": ["col1", "col2"],
            "Table2": ["COL4", "col5"],
        }
        with pytest.raises(KeyError):
            map_linked_schema_to_original_schema(linked_schema, schema_dict)


class TestBasicSchema:
    def test_basic_schema_empty(self):
        # Define the input parameters for basic_schema
        schema_config_dict = {}

        # Define the expected output
        expected = ""

        # Call the function and assert the result
        result = basic_schema(schema_config_dict)
        assert result == expected

    def test_basic_schema_single_table(self):
        # Define the input parameters for basic_schema
        schema_config_dict = {
            "table1": {
                TABLE_DESCRIPTION_STR: "table description",
                COLUMNS_KEY: [
                    {
                        COLUMN_NAME_KEY: "col1",
                        COLUMN_DESCRIPTION_KEY: "description1",
                        COLUMN_TYPE_KEY: "INTEGER",
                        COLUMN_EXAMPLES_KEY: [],
                        COLUMN_PRIMARY_KEY: True,
                    },
                    {
                        COLUMN_NAME_KEY: "col2",
                        COLUMN_DESCRIPTION_KEY: "description2",
                        COLUMN_TYPE_KEY: "TEXT",
                        COLUMN_EXAMPLES_KEY: [],
                        COLUMN_PRIMARY_KEY: False,
                    },
                ],
                TABLE_FOREIGN_KEY: [],
            }
        }

        # Define the expected output
        schema = []
        for table in schema_config_dict:
            columns = [
                column[COLUMN_NAME_KEY] for column in schema_config_dict[table][COLUMNS_KEY]
            ]
            schema.append(BASIC_SCHEMA_LINE_ENTRY.format(
                table=table, columns=', '.join(columns)))
        expected = "\n".join(schema)

        # Call the function and assert the result
        result = basic_schema(schema_config_dict)
        assert result == expected

    def test_basic_schema_multiple_tables(self):
        # Define the input parameters for basic_schema
        schema_config_dict = {
            "table1": {
                TABLE_DESCRIPTION_STR: "table1 description",
                COLUMNS_KEY: [
                    {
                        COLUMN_NAME_KEY: "col1",
                        COLUMN_DESCRIPTION_KEY: "description1",
                        COLUMN_TYPE_KEY: "INTEGER",
                        COLUMN_EXAMPLES_KEY: [],
                        COLUMN_PRIMARY_KEY: True,
                    }
                ],
                TABLE_FOREIGN_KEY: [],
            },
            "table2": {
                TABLE_DESCRIPTION_STR: "table2 description",
                COLUMNS_KEY: [
                    {
                        COLUMN_NAME_KEY: "col3",
                        COLUMN_DESCRIPTION_KEY: "description3",
                        COLUMN_TYPE_KEY: "TEXT",
                        COLUMN_EXAMPLES_KEY: [],
                        COLUMN_PRIMARY_KEY: False,
                    },
                    {
                        COLUMN_NAME_KEY: "col4",
                        COLUMN_DESCRIPTION_KEY: "description4",
                        COLUMN_TYPE_KEY: "REAL",
                        COLUMN_EXAMPLES_KEY: [],
                        COLUMN_PRIMARY_KEY: False,
                    },
                ],
                TABLE_FOREIGN_KEY: [],
            },
        }

        # Define the expected output
        schema = []
        for table in schema_config_dict:
            columns = [
                column[COLUMN_NAME_KEY] for column in schema_config_dict[table][COLUMNS_KEY]
            ]
            schema.append(BASIC_SCHEMA_LINE_ENTRY.format(
                table=table, columns=', '.join(columns)))
        expected = "\n".join(schema)

        # Call the function and assert the result
        result = basic_schema(schema_config_dict)
        assert result == expected
    

class TestTextSchema:
    def test_text_schema_empty(self):
        # Define the input parameters for text_schema
        schema_config_dict = {}

        # Define the expected output
        expected = ""

        # Call the function and assert the result
        result = text_schema(schema_config_dict)
        assert result == expected

    def test_text_schema_single_table(self):
        # Define the input parameters for text_schema
        schema_config_dict = {
            "table1": {
                TABLE_DESCRIPTION_STR: "table description",
                COLUMNS_KEY: [
                    {
                        COLUMN_NAME_KEY: "col1",
                        COLUMN_DESCRIPTION_KEY: "description1",
                        COLUMN_TYPE_KEY: "INTEGER",
                        COLUMN_EXAMPLES_KEY: [],
                        COLUMN_PRIMARY_KEY: True,
                    },
                    {
                        COLUMN_NAME_KEY: "col2",
                        COLUMN_DESCRIPTION_KEY: "description2",
                        COLUMN_TYPE_KEY: "TEXT",
                        COLUMN_EXAMPLES_KEY: [],
                        COLUMN_PRIMARY_KEY: False,
                    },
                ],
                TABLE_FOREIGN_KEY: [],
            }
        }

        # Define the expected output
        schema = []
        for table in schema_config_dict:
            columns = [
                column[COLUMN_NAME_KEY] for column in schema_config_dict[table][COLUMNS_KEY]
            ]
            schema.append(TEXT_SCHEMA_LINE_ENTRY.format(
                table=table, columns=', '.join(columns)))
        expected = "\n".join(schema)

        # Call the function and assert the result
        result = text_schema(schema_config_dict)
        assert result == expected

    def test_text_schema_multiple_tables(self):
        # Define the input parameters for text_schema
        schema_config_dict = {
            "table1": {
                TABLE_DESCRIPTION_STR: "table1 description",
                COLUMNS_KEY: [
                    {
                        COLUMN_NAME_KEY: "col1",
                        COLUMN_DESCRIPTION_KEY: "description1",
                        COLUMN_TYPE_KEY: "INTEGER",
                        COLUMN_EXAMPLES_KEY: [],
                        COLUMN_PRIMARY_KEY: True,
                    }
                ],
                TABLE_FOREIGN_KEY: [],
            },
            "table2": {
                TABLE_DESCRIPTION_STR: "table2 description",
                COLUMNS_KEY: [
                    {
                        COLUMN_NAME_KEY: "col3",
                        COLUMN_DESCRIPTION_KEY: "description3",
                        COLUMN_TYPE_KEY: "TEXT",
                        COLUMN_EXAMPLES_KEY: [],
                        COLUMN_PRIMARY_KEY: False,
                    },
                    {
                        COLUMN_NAME_KEY: "col4",
                        COLUMN_DESCRIPTION_KEY: "description4",
                        COLUMN_TYPE_KEY: "REAL",
                        COLUMN_EXAMPLES_KEY: [],
                        COLUMN_PRIMARY_KEY: False,
                    },
                ],
                TABLE_FOREIGN_KEY: [],
            },
        }

        # Define the expected output
        schema = []
        for table in schema_config_dict:
            columns = [
                column[COLUMN_NAME_KEY] for column in schema_config_dict[table][COLUMNS_KEY]
            ]
            schema.append(TEXT_SCHEMA_LINE_ENTRY.format(
                table=table, columns=', '.join(columns)))
        expected = "\n".join(schema)

        # Call the function and assert the result
        result = text_schema(schema_config_dict)
        assert result == expected


class TestCodeReprSchema:
    def test_code_repr_schema_empty(self):
        # Mock the database connection and any other necessary objects
        mock_connection = MagicMock()
        schema_config_dict = {}

        # Call the function and assert the result
        result = code_repr_schema(schema_config_dict, mock_connection)
        assert result == ""

    @patch("utilities.format_schema.get_table_ddl")
    def test_code_repr_schema_single_table(self, get_table_ddl_mock):
        # Mock the database connection and the get_table_ddl function
        mock_connection = MagicMock()
    
        get_table_ddl_mock.return_value = "CREATE TABLE table1 (col1 INTEGER, col2 TEXT);"
        # Define the input parameters for code_repr_schema
        schema_config_dict = {"table1": {}}

        # Define the expected output
        expected = "CREATE TABLE table1 (col1 INTEGER, col2 TEXT);"

        # Call the function and assert the result
        result = code_repr_schema(schema_config_dict, mock_connection)
        assert result == expected

    @patch("utilities.format_schema.get_table_ddl")
    def test_code_repr_schema_multiple_tables(self, mock_get_table_ddl):
        # Mock the database connection
        mock_connection = MagicMock()
        # Set the side effect for get_table_ddl mock to return different DDLs for different tables
        mock_get_table_ddl.side_effect = [
            "CREATE TABLE table1 (col1 INTEGER);",
            "CREATE TABLE table2 (col3 TEXT, col4 REAL);",
        ]

        # Define the input parameters for code_repr_schema
        schema_config_dict = {"table1": {}, "table2": {}}

        # Define the expected output
        expected = "CREATE TABLE table1 (col1 INTEGER);\nCREATE TABLE table2 (col3 TEXT, col4 REAL);"

        # Call the function and assert the result
        result = code_repr_schema(schema_config_dict, mock_connection)
        assert result == expected

    @patch("utilities.format_schema.get_table_ddl")
    def test_code_repr_schema_missing_sql_entry(self, mock_get_table_ddl):
        # Mock the database connection
        mock_connection = MagicMock()
        # Set the return value for get_table_ddl mock to None, simulating a missing SQL entry
        mock_get_table_ddl.return_value = None

        # Define the input parameters for code_repr_schema
        schema_config_dict = {"table1": {}}

        # Define the expected output, which is an error message indicating the missing SQL entry
        expected = CODE_REPR_SCHEMA_MISSING_SQL_ENTRY.format(table="table1")

        # Call the function and assert the result
        result = code_repr_schema(schema_config_dict, mock_connection)
        assert result == expected


class TestOpenaiSchema:
    def test_openai_schema_empty(self):
        schema_config_dict = {}
        result = openai_schema(schema_config_dict)
        assert result == ""

    def test_openai_schema_single_table(self):
        schema_config_dict = {"table1": {"columns": [{"column_name": "col1"}, {"column_name": "col2"}]}}

        schema = []
        for table in schema_config_dict:
            columns = [
                column[COLUMN_NAME_KEY] for column in schema_config_dict[table][COLUMNS_KEY]
            ]
            schema.append(OPENAI_SCHEMA_LINE_ENTRY.format(
                table=table, columns=', '.join(columns)
            ))
        
        expected = "\n".join(schema)

        result = openai_schema(schema_config_dict)
        assert result == expected

    def test_openai_schema_multiple_tables(self):
        schema_config_dict = {
            "table1": {"columns": [{"column_name": "col1"}, {"column_name": "col2"}]},
            "table2": {"columns": [{"column_name": "col3"}, {"column_name": "col4"}]},
        }
        
        schema = []
        for table in schema_config_dict:
            columns = [
                column[COLUMN_NAME_KEY] for column in schema_config_dict[table][COLUMNS_KEY]
            ]
            schema.append(OPENAI_SCHEMA_LINE_ENTRY.format(
                table=table, columns=', '.join(columns)
            ))
        
        expected = "\n".join(schema)
        result = openai_schema(schema_config_dict)
        assert result == expected

    def test_openai_schema_no_columns(self):
        schema_config_dict = {"table1": {"columns": []}}
        schema = []
        for table in schema_config_dict:
            columns = [
                column[COLUMN_NAME_KEY] for column in schema_config_dict[table][COLUMNS_KEY]
            ]
            schema.append(OPENAI_SCHEMA_LINE_ENTRY.format(
                table=table, columns=', '.join(columns)
            ))
        
        expected = "\n".join(schema)
        result = openai_schema(schema_config_dict)
        assert result == expected


class TestSemanticSchema:
    def test_semantic_schema_empty(self):
        schema_config_dict = {}
        result = semantic_schema(schema_config_dict)
        assert result == yaml.dump([], sort_keys=False, default_flow_style=False)

    def test_semantic_schema_single_table(self):
        schema_config_dict = {
            "table1": {
                TABLE_DESCRIPTION_STR: "Description of table1",
                COLUMNS_KEY: [
                    {COLUMN_NAME_KEY: "col1", COLUMN_TYPE_KEY: "INTEGER", COLUMN_DESCRIPTION_KEY: "Description of col1", COLUMN_EXAMPLES_KEY: ["1", "2"]},
                    {COLUMN_NAME_KEY: "col2", COLUMN_TYPE_KEY: "TEXT", COLUMN_DESCRIPTION_KEY: "Description of col2", COLUMN_EXAMPLES_KEY: ["a", "b"]},
                ],
            }
        }
        expected = [
            {
                SEMANTIC_SCHEMA_TABLE_KEY: "table1",
                SEMANTIC_SCHEMA_DESCRIPTION_KEY: "Description of table1",
                "Columns": [
                    'col1: INTEGER, Description: Description of col1, Example: 1',
                    'col2: TEXT, Description: Description of col2, Example: a',
                ],
            }
        ]
        
        expected_yaml = yaml.dump(expected, sort_keys=False, default_flow_style=False)
        result = semantic_schema(schema_config_dict)
        assert result == expected_yaml

    def test_semantic_schema_multiple_tables(self):
        schema_config_dict = {
            "table1": {
                "table_description": "Description of table1",
                "columns": [
                    {"column_name": "col1", "column_type": "INTEGER", "column_description": "Description of col1", "column_examples": ["1", "2"]},
                    {"column_name": "col2", "column_type": "TEXT", "column_description": "Description of col2", "column_examples": ["a", "b"]},
                ],
            },
            "table2": {
                "table_description": "Description of table2",
                "columns": [
                    {"column_name": "col3", "column_type": "REAL", "column_description": "Description of col3", "column_examples": ["1.1", "2.2"]},
                    {"column_name": "col4", "column_type": "BOOLEAN", "column_description": "Description of col4", "column_examples": ["true", "false"]},
                ],
            },
        }
        expected = [
            {
                SEMANTIC_SCHEMA_TABLE_KEY: "table1",
                SEMANTIC_SCHEMA_DESCRIPTION_KEY: "Description of table1",
                "Columns": [
                     'col1: INTEGER, Description: Description of col1, Example: 1',
                     'col2: TEXT, Description: Description of col2, Example: a'
                ],
            },
            {
                SEMANTIC_SCHEMA_TABLE_KEY: "table2",
                SEMANTIC_SCHEMA_DESCRIPTION_KEY: "Description of table2",
                "Columns": [
                        'col3: REAL, Description: Description of col3, Example: 1.1',
                        'col4: BOOLEAN, Description: Description of col4, Example: true',
                    ],
            },
        ]
        expected_yaml = yaml.dump(expected, sort_keys=False, default_flow_style=False)
        result = semantic_schema(schema_config_dict)
        assert result == expected_yaml

    def test_semantic_schema_no_columns(self):
        schema_config_dict = {
            "table1": {
                "table_description": "Description of table1",
                "columns": [],
            }
        }
        expected = [
            {
                SEMANTIC_SCHEMA_TABLE_KEY: "table1",
                SEMANTIC_SCHEMA_DESCRIPTION_KEY: "Description of table1",
                "Columns": [],
            }
        ]
        expected_yaml = yaml.dump(expected, sort_keys=False, default_flow_style=False)
        result = semantic_schema(schema_config_dict)
        assert result == expected_yaml

    def test_semantic_schema_no_examples(self):
        schema_config_dict = {
            "table1": {
                "table_description": "Description of table1",
                "columns": [
                    {"column_name": "col1", "column_type": "INTEGER", "column_description": "Description of col1", "column_examples": []},
                ],
            }
        }
        expected = [
            {
                SEMANTIC_SCHEMA_TABLE_KEY: "table1",
                SEMANTIC_SCHEMA_DESCRIPTION_KEY: "Description of table1",
                "Columns": [
                    "col1: INTEGER, Description: Description of col1, Example: "
                ],
            }
        ]
        expected_yaml = yaml.dump(expected, sort_keys=False, default_flow_style=False)
        result = semantic_schema(schema_config_dict)
        assert result == expected_yaml


class TestGenerateMSchemaColumns:
    def test_generate_mschema_columns_empty(self):
        columns_config = []
        result = generate_mschema_columns(columns_config)
        assert result == []

    def test_generate_mschema_columns_single_column(self):
        columns_config = [
            {
                COLUMN_NAME_KEY: "col1",
                COLUMN_TYPE_KEY: "INTEGER",
                COLUMN_DESCRIPTION_KEY: "Description of col1",
                COLUMN_EXAMPLES_KEY: ["1", "2"],
                COLUMN_PRIMARY_KEY: False,
            }
        ]
        expected = [
            "(col1:INTEGER, Description of col1, Examples: ['1', '2'])"
        ]
        result = generate_mschema_columns(columns_config)
        assert result == expected

    def test_generate_mschema_columns_multiple_columns(self):
        columns_config = [
            {
                COLUMN_NAME_KEY: "col1",
                COLUMN_TYPE_KEY: "INTEGER",
                COLUMN_DESCRIPTION_KEY: "Description of col1",
                COLUMN_EXAMPLES_KEY: ["1", "2"],
                COLUMN_PRIMARY_KEY: False,
            },
            {
                COLUMN_NAME_KEY: "col2",
                COLUMN_TYPE_KEY: "TEXT",
                COLUMN_DESCRIPTION_KEY: "Description of col2",
                COLUMN_EXAMPLES_KEY: ["a", "b"],
                COLUMN_PRIMARY_KEY: True,
            },
        ]
        expected = [
            "(col1:INTEGER, Description of col1, Examples: ['1', '2'])",
            "(col2:TEXT, Description of col2, Primary Key, Examples: ['a', 'b'])",
        ]
        result = generate_mschema_columns(columns_config)
        assert result == expected

    def test_generate_mschema_columns_no_examples(self):
        columns_config = [
            {
                COLUMN_NAME_KEY: "col1",
                COLUMN_TYPE_KEY: "INTEGER",
                COLUMN_DESCRIPTION_KEY: "Description of col1",
                COLUMN_EXAMPLES_KEY: [],
                COLUMN_PRIMARY_KEY: False,
            }
        ]
        expected = [
            "(col1:INTEGER, Description of col1, Examples: [])"
        ]
        result = generate_mschema_columns(columns_config)
        assert result == expected

    def test_generate_mschema_columns_missing_description(self):
        columns_config = [
            {
                COLUMN_NAME_KEY: "col1",
                COLUMN_TYPE_KEY: "INTEGER",
                COLUMN_DESCRIPTION_KEY: None,
                COLUMN_EXAMPLES_KEY: ["1", "2"],
                COLUMN_PRIMARY_KEY: False,
            }
        ]
        expected = [
            "(col1:INTEGER, None, Examples: ['1', '2'])"
        ]
        result = generate_mschema_columns(columns_config)
        assert result == expected


class TestGenerateMSchemaTableEntry:
    def test_generate_mschema_table_entry(self):
        table_name = "table1"
        table_config = {TABLE_DESCRIPTION_STR: "Description of table1"}
        columns = ["(col1:INTEGER, Description of col1, Examples: ['1', '2'])", "(col2:TEXT, Description of col2, Primary Key, Examples: ['a', 'b'])"]
        expected = """# Table: table1, Description of table1

[
(col1:INTEGER, Description of col1, Examples: ['1', '2'])
(col2:TEXT, Description of col2, Primary Key, Examples: ['a', 'b'])
]"""
        result = generate_mschema_table_entry(table_name, table_config, columns)
        assert result == expected

    def test_generate_mschema_table_entry_no_columns(self):
        table_name = "table1"
        table_config = {TABLE_DESCRIPTION_STR: "Description of table1"}
        columns = []
        expected = """# Table: table1, Description of table1

[

]"""
        result = generate_mschema_table_entry(table_name, table_config, columns)
        assert result == expected

    def test_generate_mschema_table_entry_empty_description(self):
        table_name = "table1"
        table_config = {TABLE_DESCRIPTION_STR: ""}
        columns = ["(col1:INTEGER, Description of col1, Examples: ['1', '2'])"]
        expected = """# Table: table1, 

[
(col1:INTEGER, Description of col1, Examples: ['1', '2'])
]"""
        result = generate_mschema_table_entry(table_name, table_config, columns)
        assert result == expected


class TestGenerateMSchemaForeignKeys:
    def test_generate_mschema_foreign_keys(self):
        table_name = "table1"
        foreign_keys_config = [
            {
                FOREIGN_KEY_FROM_COLUMN_KEY: "col1",
                FOREIGN_KEY_TO_TABLE_KEY: "table2",
                FOREIGN_KEY_TO_COLUMN_KEY: "col2",
            },
            {
                FOREIGN_KEY_FROM_COLUMN_KEY: "col3",
                FOREIGN_KEY_TO_TABLE_KEY: "table3",
                FOREIGN_KEY_TO_COLUMN_KEY: "col4",
            },
        ]
        expected = ["table1.col1=table2.col2", "table1.col3=table3.col4"]
        result = generate_mschema_foreign_keys(table_name, foreign_keys_config)
        assert result == expected

    def test_generate_mschema_foreign_keys_empty(self):
        table_name = "table1"
        foreign_keys_config = []
        expected = []
        result = generate_mschema_foreign_keys(table_name, foreign_keys_config)
        assert result == expected

    def test_generate_mschema_foreign_keys_single(self):
        table_name = "table1"
        foreign_keys_config = [
            {
                FOREIGN_KEY_FROM_COLUMN_KEY: "col1",
                FOREIGN_KEY_TO_TABLE_KEY: "table2",
                FOREIGN_KEY_TO_COLUMN_KEY: "col2",
            }
        ]
        expected = ["table1.col1=table2.col2"]
        result = generate_mschema_foreign_keys(table_name, foreign_keys_config)
        assert result == expected


class TestMSchema:
    def test_m_schema_generation(self):
        schema_config_dict = {
            "table1": {
                COLUMNS_KEY: [
                    {
                        "column_name": "col1",
                        "column_type": "INTEGER",
                        "column_description": "Column 1 description",
                        "primary_key": True,
                        "column_examples": ["1", "2"],
                    }
                ],
                TABLE_DESCRIPTION_STR: "Table 1 description",
                TABLE_FOREIGN_KEY: [
                    {
                        FOREIGN_KEY_FROM_COLUMN_KEY: "col1",
                        FOREIGN_KEY_TO_TABLE_KEY: "table2",
                        FOREIGN_KEY_TO_COLUMN_KEY: "col2",
                    }
                ],
            },
            "table2": {
                COLUMNS_KEY: [
                    {
                        "column_name": "col2",
                        "column_type": "TEXT",
                        "column_description": "Column 2 description",
                        "primary_key": False,
                        "column_examples": ["abc", "def"],
                    }
                ],
                TABLE_DESCRIPTION_STR: "Table 2 description",
                TABLE_FOREIGN_KEY: [],
            },
        }
        database_name = "test_db"
        expected = """【DB_ID】 test_db
【Schema】
# Table: table1, Table 1 description

[
(col1:INTEGER, Column 1 description, Primary Key, Examples: ['1', '2'])
]
# Table: table2, Table 2 description

[
(col2:TEXT, Column 2 description, Examples: ['abc', 'def'])
]
【Foreign keys】
table1.col1=table2.col2"""
        result = m_schema(schema_config_dict, database_name)
        assert result == expected

    def test_m_schema_no_foreign_keys(self):
        schema_config_dict = {
            "table1": {
                COLUMNS_KEY: [
                    {
                        "column_name": "col1",
                        "column_type": "INTEGER",
                        "column_description": "Column 1 description",
                        "primary_key": True,
                        "column_examples": ["1", "2"],
                    }
                ],
                TABLE_DESCRIPTION_STR: "Table 1 description",
                TABLE_FOREIGN_KEY: [],
            },
            "table2": {
                COLUMNS_KEY: [
                    {
                        "column_name": "col2",
                        "column_type": "TEXT",
                        "column_description": "Column 2 description",
                        "primary_key": False,
                        "column_examples": ["abc", "def"],
                    }
                ],
                TABLE_DESCRIPTION_STR: "Table 2 description",
                TABLE_FOREIGN_KEY: [],
            },
        }
        database_name = "test_db"
        expected = """【DB_ID】 test_db
【Schema】
# Table: table1, Table 1 description

[
(col1:INTEGER, Column 1 description, Primary Key, Examples: ['1', '2'])
]
# Table: table2, Table 2 description

[
(col2:TEXT, Column 2 description, Examples: ['abc', 'def'])
]"""
        result = m_schema(schema_config_dict, database_name)
        assert result == expected

    def test_m_schema_empty_tables(self):
        schema_config_dict = {}
        database_name = "test_db"
        expected = """【DB_ID】 test_db
【Schema】"""
        result = m_schema(schema_config_dict, database_name)
        assert result == expected


class TestGenerateSchema(unittest.TestCase):
    @patch("utilities.format_schema.basic_schema")
    def test_generate_schema_basic(self, mock_basic_schema):
        schema_config_dict: Dict = {"table_name": "test_table"}
        generate_schema(
            format_type=FormatType.BASIC,
            schema_config_dict=schema_config_dict,
            database_name="test_db",
            connection=sqlite3.connect(":memory:"),
        )
        mock_basic_schema.assert_called_once_with(
            schema_config_dict=schema_config_dict
        )

    @patch("utilities.format_schema.text_schema")
    def test_generate_schema_text(self, mock_text_schema):
        schema_config_dict: Dict = {"table_name": "test_table"}
        generate_schema(
            format_type=FormatType.TEXT,
            schema_config_dict=schema_config_dict,
            database_name="test_db",
            connection=sqlite3.connect(":memory:"),
        )
        mock_text_schema.assert_called_once_with(
            schema_config_dict=schema_config_dict
        )

    @patch("utilities.format_schema.code_repr_schema")
    def test_generate_schema_code(self, mock_code_repr_schema):
        schema_config_dict: Dict = {"table_name": "test_table"}
        connection = sqlite3.connect(":memory:")
        generate_schema(
            format_type=FormatType.CODE,
            schema_config_dict=schema_config_dict,
            database_name="test_db",
            connection=connection,
        )
        mock_code_repr_schema.assert_called_once_with(
            schema_config_dict=schema_config_dict, connection=connection
        )

    @patch("utilities.format_schema.openai_schema")
    def test_generate_schema_openai(self, mock_openai_schema):
        schema_config_dict: Dict = {"table_name": "test_table"}
        generate_schema(
            format_type=FormatType.OPENAI,
            schema_config_dict=schema_config_dict,
            database_name="test_db",
            connection=sqlite3.connect(":memory:"),
        )
        mock_openai_schema.assert_called_once_with(
            schema_config_dict=schema_config_dict
        )

    @patch("utilities.format_schema.semantic_schema")
    def test_generate_schema_semantic(self, mock_semantic_schema):
        schema_config_dict: Dict = {"table_name": "test_table"}
        generate_schema(
            format_type=FormatType.SEMANTIC,
            schema_config_dict=schema_config_dict,
            database_name="test_db",
            connection=sqlite3.connect(":memory:"),
        )
        mock_semantic_schema.assert_called_once_with(
            schema_config_dict=schema_config_dict
        )

    @patch("utilities.format_schema.m_schema")
    def test_generate_schema_m_schema(self, mock_m_schema):
        schema_config_dict: Dict = {"table_name": "test_table"}
        database_name = "test_db"
        generate_schema(
            format_type=FormatType.M_SCHEMA,
            schema_config_dict=schema_config_dict,
            database_name=database_name,
            connection=sqlite3.connect(":memory:"),
        )
        mock_m_schema.assert_called_once_with(
            schema_config_dict=schema_config_dict, database_name=database_name
        )

    def test_generate_schema_unsupported(self):
        schema_config_dict: Dict = {"table_name": "test_table"}
        with self.assertRaises(ValueError) as context:
            generate_schema(
                format_type="UNSUPPORTED",  # type: ignore
                schema_config_dict=schema_config_dict,
                database_name="test_db",
                connection=sqlite3.connect(":memory:"),
            )
        self.assertEqual(
            str(context.exception),
            ERROR_UNSUPPORTED_FORMAT_TYPE.format(format_type="UNSUPPORTED"),
        )


class TestFormatSchema(unittest.TestCase):
    @patch("utilities.format_schema.PATH_CONFIG")
    @patch("utilities.format_schema.get_schema_dict")
    @patch("utilities.format_schema.remove_errors_from_linked_schema")
    @patch("utilities.format_schema.map_linked_schema_to_original_schema")
    @patch("utilities.format_schema.generate_description_dict")
    @patch("utilities.format_schema.construct_schema_config")
    @patch("utilities.format_schema.generate_schema")
    def test_format_schema_default_dataset_type(
        self,
        mock_generate_schema,
        mock_construct_schema_config,
        mock_generate_description_dict,
        mock_map_linked_schema_to_original_schema,
        mock_remove_errors_from_linked_schema,
        mock_get_schema_dict,
        mock_path_config,
    ):
        format_type = FormatType.TEXT
        database_name = "test_db"
        db_path = "test/sqlite/path"
        mock_path_config.sqlite_path.return_value = db_path
        schema_dict = {"table1": ["col1", "col2"]}
        description_dict = {"table1": "description"}
        schema_config_dict = {"table_name": "test_table"}
        formatted_schema = "formatted schema"
        dataset_type = DatasetType.BIRD_DEV

        mock_get_schema_dict.return_value = schema_dict
        mock_generate_description_dict.return_value = description_dict
        mock_construct_schema_config.return_value = schema_config_dict
        mock_generate_schema.return_value = formatted_schema

        with patch("sqlite3.connect", return_value=MagicMock()) as mock_connect:
            result = format_schema(
                format_type=format_type, database_name=database_name, dataset_type=dataset_type
            )

            self.assertEqual(result, formatted_schema)
            mock_get_schema_dict.assert_called_once_with(database_path=db_path)
            mock_remove_errors_from_linked_schema.assert_not_called()
            mock_map_linked_schema_to_original_schema.assert_not_called()
            mock_generate_description_dict.assert_called_once_with(
                database_name=database_name,
                dataset_type=dataset_type,
                schema_dict=schema_dict,
            )
            mock_construct_schema_config.assert_called_once()
            mock_generate_schema.assert_called_once_with(
                format_type=format_type,
                schema_config_dict=schema_config_dict,
                database_name=database_name,
                connection=mock_connect.return_value.__enter__.return_value,
            )
            mock_connect.assert_called_once_with(db_path)

    @patch("utilities.format_schema.PATH_CONFIG")
    @patch("utilities.format_schema.get_schema_dict")
    @patch("utilities.format_schema.remove_errors_from_linked_schema")
    @patch("utilities.format_schema.map_linked_schema_to_original_schema")
    @patch("utilities.format_schema.generate_description_dict")
    @patch("utilities.format_schema.construct_schema_config")
    @patch("utilities.format_schema.generate_schema")
    def test_format_schema_with_linked_schema_and_dataset_type(
        self,
        mock_generate_schema,
        mock_construct_schema_config,
        mock_generate_description_dict,
        mock_map_linked_schema_to_original_schema,
        mock_remove_errors_from_linked_schema,
        mock_get_schema_dict,
        mock_path_config,
    ):
        format_type = FormatType.CODE
        database_name = "test_db"
        linked_schema = {"table1": ["col1", "col2"]}
        dataset_type = DatasetType.BIRD_DEV
        db_path = "test/db/path"
        schema_dict = {"table1": ["col1", "col2"]}
        corrected_linked_schema = {"table1": ["col1", "col2"]}
        description_dict = {"table1": "description"}
        schema_config_dict = {"table_name": "test_table"}
        formatted_schema = "formatted schema"

        mock_get_schema_dict.return_value = schema_dict
        mock_remove_errors_from_linked_schema.return_value = corrected_linked_schema
        mock_generate_schema.return_value = formatted_schema
        mock_path_config.sqlite_path.return_value = db_path
        mock_map_linked_schema_to_original_schema.return_value = schema_dict
        mock_construct_schema_config.return_value = schema_config_dict

        with patch("sqlite3.connect", return_value=MagicMock()) as mock_connect:
            result = format_schema(
                format_type=format_type,
                database_name=database_name,
                linked_schema=linked_schema,
                dataset_type=dataset_type,
            )

            self.assertEqual(result, formatted_schema)
            mock_get_schema_dict.assert_called_once_with(database_path=db_path)
            mock_remove_errors_from_linked_schema.assert_called_once_with(
                linked_schema=linked_schema, schema_dict=schema_dict
            )
            mock_map_linked_schema_to_original_schema.assert_called_once_with(
                linked_schema=corrected_linked_schema, schema_dict=schema_dict
            )
            mock_generate_description_dict.assert_called_once_with(
                database_name=database_name,
                dataset_type=dataset_type,
                schema_dict=schema_dict,
            )
            mock_construct_schema_config.assert_called_once()
            mock_generate_schema.assert_called_once_with(
                format_type=format_type,
                schema_config_dict=schema_config_dict,
                database_name=database_name,
                connection=mock_connect.return_value.__enter__.return_value,
            )
            mock_connect.assert_called_once_with(db_path)