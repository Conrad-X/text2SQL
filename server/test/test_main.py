import unittest
from unittest.mock import MagicMock, patch

import pytest
from app.main import app
from fastapi.testclient import TestClient
from utilities.config import PATH_CONFIG
from utilities.constants.database_enums import DatabaseType
from utilities.constants.LLM_enums import LLMType, ModelType
from utilities.constants.prompts_enums import FormatType, PromptType
from utilities.constants.response_messages import (
    ERROR_NON_NEGATIVE_SHOTS_REQUIRED, ERROR_SHOTS_REQUIRED,
    ERROR_ZERO_SHOTS_REQUIRED)

client = TestClient(app)


class TestGenerateAndExecuteSQLQuery:

    @patch("app.main.execute_sql_query")
    @patch("app.main.ClientFactory.get_client")
    @patch("app.main.PromptFactory.get_prompt_class")
    def test_successful_query_generation_and_execution(
        self, mock_get_prompt_class, mock_get_client, mock_execute_sql_query
    ):
        mock_prompt = "Generated Prompt"
        mock_get_prompt_class.return_value = mock_prompt

        mock_client = MagicMock()
        mock_client.execute_prompt.return_value = "SELECT * FROM test_table"
        mock_get_client.return_value = mock_client

        mock_execute_sql_query.return_value = [{"id": 1, "name": "Test"}]

        request_data = {
            "question": "Test question",
            "prompt_type": "openai_demonstration",
            "shots": 0,
            "llm_type": "openai",
            "model": "gpt-4o-mini-2024-07-18",
            "temperature": 0.7,
            "max_tokens": 1000,
        }

        response = client.post("/queries/generate-and-execute/", json=request_data)

        assert response.status_code == 200
        response_json = response.json()
        assert "result" in response_json
        assert response_json["result"] == [{"id": 1, "name": "Test"}]
        assert "query" in response_json
        assert response_json["query"] == "SELECT * FROM test_table"
        assert "prompt_used" in response_json
        assert response_json["prompt_used"] == "Generated Prompt"

        mock_get_prompt_class.assert_called_once_with(
            prompt_type=PromptType.OPENAI_DEMO,
            target_question=request_data["question"],
            shots=request_data["shots"],
        )

        mock_get_client.assert_called_once_with(
            type=LLMType.OPENAI,
            model=ModelType.OPENAI_GPT4_O_MINI,
            temperature=request_data["temperature"],
            max_tokens=request_data["max_tokens"],
        )

        mock_execute_sql_query.assert_called_once_with(
            unittest.mock.ANY, sql_query="SELECT * FROM test_table"
        )

        mock_client.execute_prompt.assert_called_once_with(prompt=mock_prompt)

    def test_missing_question_parameter(self):
        response = client.post(
            "/queries/generate-and-execute/",
            json={
                "prompt_type": "full_information",
                "shots": 2,
                "llm_type": "openai",
                "model": "gpt-4o-mini-2024-07-18",
                "temperature": 0.7,
                "max_tokens": 1000,
            },
        )
        assert response.status_code == 422

    def test_missing_shots_parameter(self):
        response = client.post(
            "/queries/generate-and-execute/",
            json={
                "question": "TARGET QUESTION",
                "prompt_type": "full_information",
                "llm_type": "openai",
                "model": "gpt-4o-mini-2024-07-18",
                "temperature": 0.7,
                "max_tokens": 1000,
            },
        )
        assert response.status_code == 422

    def test_missing_prompt_type_parameter(self):
        response = client.post(
            "/queries/generate-and-execute/",
            json={
                "question": "TARGET QUESTION",
                "shots": 2,
                "llm_type": "openai",
                "model": "gpt-4o-mini-2024-07-18",
                "temperature": 0.7,
                "max_tokens": 1000,
            },
        )
        assert response.status_code == 422

    @pytest.mark.parametrize(
        "prompt_type", ["full_information", "sql_only", "dail_sql"]
    )
    def test_shots_required_for_few_shot_prompts(self, prompt_type):
        response = client.post(
            "/queries/generate-and-execute/",
            json={
                "question": "TARGET QUESTION",
                "prompt_type": prompt_type,
                "shots": None,
                "llm_type": "openai",
                "model": "gpt-4o-mini-2024-07-18",
                "temperature": 0.7,
                "max_tokens": 1000,
            },
        )
        assert response.status_code == 400
        assert response.json()["detail"] == ERROR_SHOTS_REQUIRED

    @pytest.mark.parametrize(
        "prompt_type",
        [
            "openai_demonstration",
            "code_representation",
            "alpaca_sft",
            "text_representation",
            "basic",
        ],
    )
    def test_no_shots_required_for_zero_shot_prompts(self, prompt_type):
        response = client.post(
            "/queries/generate-and-execute/",
            json={
                "question": "TARGET QUESTION",
                "prompt_type": prompt_type,
                "shots": 2,
                "llm_type": "openai",
                "model": "gpt-4o-mini-2024-07-18",
                "temperature": 0.7,
                "max_tokens": 1000,
            },
        )
        assert response.status_code == 400
        assert response.json()["detail"] == ERROR_ZERO_SHOTS_REQUIRED

    @patch("app.main.execute_sql_query")
    @patch("app.main.ClientFactory.get_client")
    @patch("app.main.PromptFactory.get_prompt_class")
    def test_failure_handling_for_sql_execution(
        self, mock_get_prompt_class, mock_get_client, mock_execute_sql_query
    ):
        mock_prompt = MagicMock()
        mock_prompt.return_value = "Generated Prompt"
        mock_get_prompt_class.return_value = mock_prompt

        mock_client = MagicMock()
        mock_client.execute_prompt.return_value = "SELECT * FROM test_table"
        mock_get_client.return_value = mock_client

        mock_execute_sql_query.side_effect = Exception("SQL execution failed")

        request_data = {
            "question": "Test question",
            "prompt_type": "full_information",
            "shots": 2,
            "llm_type": "openai",
            "model": "gpt-4o-mini-2024-07-18",
            "temperature": 0.7,
            "max_tokens": 1000,
        }
        response = client.post("/queries/generate-and-execute/", json=request_data)

        assert response.status_code == 400
        error_detail = response.json()["detail"]
        assert error_detail["error"] == "SQL execution failed"
        assert error_detail["query"] == "SELECT * FROM test_table"
        assert error_detail["result"] == ""


class TestMaskSingleQuestionAndQuery:

    @patch("app.main.mask_question")
    @patch("app.main.mask_sql_query")
    @patch("app.main.get_array_of_table_and_column_name")
    def test_successful_masking(
        self,
        mock_get_array_of_table_and_column_name,
        mock_mask_sql_query,
        mock_mask_question,
    ):
        mock_get_array_of_table_and_column_name.return_value = [
            {"table": "test_table", "columns": ["id", "name"]}
        ]
        mock_mask_question.return_value = "Masked question"
        mock_mask_sql_query.return_value = "Masked SQL query"

        request_data = {
            "question": "Test question",
            "sql_query": "SELECT * FROM test_table WHERE id = 1",
        }

        response = client.post("/masking/question-and-query/", json=request_data)

        assert response.status_code == 200
        response_json = response.json()
        assert "masked_question" in response_json
        assert response_json["masked_question"] == "Masked question"
        assert "masked_sql_query" in response_json
        assert response_json["masked_sql_query"] == "Masked SQL query"

        mock_get_array_of_table_and_column_name.assert_called_once_with(
            PATH_CONFIG.sqlite_path()
        )
        mock_mask_question.assert_called_once_with(
            request_data["question"],
            table_and_column_names=mock_get_array_of_table_and_column_name.return_value,
        )
        mock_mask_sql_query.assert_called_once_with(request_data["sql_query"])

    @patch("app.main.get_array_of_table_and_column_name")
    def test_masking_failure_due_to_exception(
        self, mock_get_array_of_table_and_column_name
    ):
        mock_get_array_of_table_and_column_name.side_effect = Exception(
            "Failed to retrieve table and column names"
        )

        request_data = {
            "question": "Test question",
            "sql_query": "SELECT * FROM test_table WHERE id = 1",
        }
        response = client.post("/masking/question-and-query/", json=request_data)

        assert response.status_code == 500
        assert response.json()["detail"] == "Failed to retrieve table and column names"


class TestMaskQuestionAndAnswerFile:

    @patch("app.main.get_array_of_table_and_column_name")
    @patch("app.main.mask_question_and_answer_files")
    def test_successful_file_masking(
        self,
        mock_mask_question_and_answer_files,
        mock_get_array_of_table_and_column_names,
    ):
        mock_get_array_of_table_and_column_names.return_value = [
            {"table": "test_table", "columns": ["id", "name"]}
        ]
        mock_mask_question_and_answer_files.return_value = "masked_file_name.txt"

        request_data = {"database_name": "test"}
        response = client.post("/masking/file/", json=request_data)

        assert response.status_code == 200
        response_json = response.json()
        assert "masked_file_name" in response_json
        assert response_json["masked_file_name"] == "masked_file_name.txt"

        mock_get_array_of_table_and_column_names.assert_called_once_with(
            PATH_CONFIG.sqlite_path()
        )
        mock_mask_question_and_answer_files.assert_called_once_with(
            database_name=request_data["database_name"],
            table_and_column_names=mock_get_array_of_table_and_column_names.return_value,
        )

    @patch("app.main.get_array_of_table_and_column_name")
    def test_file_masking_failure_due_to_exception(
        self, mock_get_array_of_table_and_column_names
    ):
        mock_get_array_of_table_and_column_names.side_effect = Exception(
            "Failed to retrieve table and column names"
        )

        request_data = {"database_name": "test"}

        response = client.post("/masking/file/", json=request_data)

        assert response.status_code == 500
        assert response.json()["detail"] == "Failed to retrieve table and column names"

        mock_get_array_of_table_and_column_names.assert_called_once_with(
            PATH_CONFIG.sqlite_path()
        )


class TestGeneratePrompt:

    @patch("app.main.PromptFactory.get_prompt_class")
    def test_successful_prompt_generation(self, mock_get_prompt_class):
        mock_prompt = "Generated prompt"
        mock_get_prompt_class.return_value = mock_prompt

        request_data = {
            "prompt_type": "openai_demonstration",
            "shots": 1,
            "question": "What is the capital of France?",
        }

        response = client.post("/prompts/generate/", json=request_data)

        assert response.status_code == 200
        response_json = response.json()
        assert "generated_prompt" in response_json
        assert response_json["generated_prompt"] == "Generated prompt"

        mock_get_prompt_class.assert_called_once_with(
            prompt_type=PromptType.OPENAI_DEMO,
            target_question=request_data["question"],
            shots=request_data["shots"],
        )

    def test_negative_shots(self):
        request_data = {
            "prompt_type": "openai_demonstration",
            "shots": -1,
            "question": "What is the capital of France?",
        }
        response = client.post("/prompts/generate/", json=request_data)

        assert response.status_code == 400
        assert response.json()["detail"] == ERROR_NON_NEGATIVE_SHOTS_REQUIRED

    @patch("app.main.PromptFactory.get_prompt_class")
    def test_prompt_generation_failure(self, mock_get_prompt_class):
        mock_get_prompt_class.side_effect = Exception("Prompt generation failed")

        request_data = {
            "prompt_type": "openai_demonstration",
            "shots": 1,
            "question": "What is the capital of France?",
        }

        response = client.post("/prompts/generate/", json=request_data)

        assert response.status_code == 500
        assert response.json()["detail"] == "Prompt generation failed"

        mock_get_prompt_class.assert_called_once_with(
            prompt_type=PromptType.OPENAI_DEMO,
            target_question=request_data["question"],
            shots=request_data["shots"],
        )


class TestChangeDatabase:

    @patch("app.main.db.set_database")
    @patch("app.main.format_schema")
    @patch("app.main.PATH_CONFIG.database_name", new_callable=MagicMock)
    def test_successful_database_change(
        self, mock_active_database, mock_format_schema, mock_set_database
    ):
        mock_format_schema.return_value = "Database Schema"
        mock_active_database.value = "hotel"

        request_data = {"database_type": "hotel"}

        response = client.post("/database/change/", json=request_data)

        assert response.status_code == 200
        response_json = response.json()

        assert "database_type" in response_json
        assert response_json["database_type"] == "hotel"
        assert "schema" in response_json
        assert response_json["schema"] == "Database Schema"

        mock_set_database.assert_called_once_with(DatabaseType.HOTEL)
        mock_format_schema.assert_called_once_with(
            FormatType.CODE, PATH_CONFIG.sqlite_path()
        )

    def test_change_database_failure(self):
        request_data = {"database_type": "invalid_database"}
        response = client.post("/database/change/", json=request_data)

        assert response.status_code == 422


class TestGetDatabaseSchema:

    @patch("app.main.format_schema")
    @patch("app.main.PATH_CONFIG.database_name", new_callable=MagicMock)
    def test_successful_get_database_schema(
        self, mock_active_database, mock_format_schema
    ):
        mock_format_schema.return_value = "Database Schema"
        mock_active_database.value = "hotel"

        response = client.get("/database/schema/")

        assert response.status_code == 200
        response_json = response.json()
        assert "database_type" in response_json
        assert response_json["database_type"] == "hotel"
        assert "schema" in response_json
        assert response_json["schema"] == "Database Schema"

        mock_format_schema.assert_called_once_with(
            FormatType.CODE, PATH_CONFIG.sqlite_path()
        )

    @patch("app.main.format_schema")
    @patch("app.main.PATH_CONFIG.database_name", new_callable=MagicMock)
    def test_get_database_schema_failure(
        self, mock_active_database, mock_format_schema
    ):
        mock_format_schema.side_effect = ValueError("Error retrieving schema")

        response = client.get("/database/schema/")

        assert response.status_code == 400
        assert response.json()["detail"] == "Error retrieving schema"
