import os
import json
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient
from app.main import app
from utilities.config import MASKED_FOLDER_PATH

client = TestClient(app)

def set_database_schema_to_hotel():
    response = client.post("/database/change/", json={"database_type": "hotel"})
    assert response.status_code == 200, f"Failed to change database: {response.json()}"

@patch('app.main.ClientFactory.get_client')
def test_generate_and_execute_sql_query_success(mock_get_client):
    set_database_schema_to_hotel() 

    mock_client_instance = MagicMock()
    mock_client_instance.execute_prompt.return_value = "SELECT * FROM hotel;"
    mock_get_client.return_value = mock_client_instance

    expected_result = [
        {"hotelno": "fb01", "hotelname": "Grosvenor", "city": "London"},
        {"hotelno": "fb02", "hotelname": "Watergate", "city": "Paris"},
        {"hotelno": "ch01", "hotelname": "Omni Shoreham", "city": "London"},
        {"hotelno": "ch02", "hotelname": "Phoenix Park", "city": "London"},
        {"hotelno": "dc01", "hotelname": "Latham", "city": "Berlin"}
    ]
    expected_prompt = (
        "/* Given the following database schema : */\n"
        "Table guest, columns = [ guestno, guestname, guestaddress ]\n"
        "Table hotel, columns = [ hotelno, hotelname, city ]\n"
        "Table room, columns = [ roomno, hotelno, type, price ]\n"
        "Table booking, columns = [ hotelno, guestno, datefrom, dateto, roomno ]\n"
        "\n"
        "/* Answer the following : List all hotels which are in London. Order the result in descending order by hotel name. */\n"
        "\n"
        "select * from hotel where city ='London' order by hotelname desc;\n"
        "\n"
        "/* Given the following database schema : */\n"
        "Table guest, columns = [ guestno, guestname, guestaddress ]\n"
        "Table hotel, columns = [ hotelno, hotelname, city ]\n"
        "Table room, columns = [ roomno, hotelno, type, price ]\n"
        "Table booking, columns = [ hotelno, guestno, datefrom, dateto, roomno ]\n"
        "\n"
        "/* Answer the following : List all hotels whose name’s third alphabet has a ‘t’. */\n"
        "\n"
        "select * from hotel where hotelname like '__t%';\n"
        "\n"
        "/*Complete sqlite SQL query only and with no explanation\n"
        "Given the following database schema : */\n"
        "Table guest, columns = [ guestno, guestname, guestaddress ]\n"
        "Table hotel, columns = [ hotelno, hotelname, city ]\n"
        "Table room, columns = [ roomno, hotelno, type, price ]\n"
        "Table booking, columns = [ hotelno, guestno, datefrom, dateto, roomno ]\n"
        "\n"
        "/* Answer the following : List all Hotels */\n"
        "\nSELECT"
    )
    expected_query = "SELECT * FROM hotel;"

    response = client.post("/queries/generate-and-execute/", json={
        "question": "List all Hotels",
        "prompt_type": "full_information",
        "shots": 2,
        "llm_type": "openai",
        "model": "gpt-4o-mini",
        "temperature": 0.7,
        "max_tokens": 1000
    })

    assert response.status_code == 200
    assert "result" in response.json()
    assert "query" in response.json()
    assert "prompt_used" in response.json()

    assert response.json()["result"] == expected_result
    assert response.json()["query"] == expected_query
    assert response.json()["prompt_used"] == expected_prompt

@patch('app.main.ClientFactory.get_client')
def test_generate_and_execute_sql_query_query_execution_error(mock_get_client):
    set_database_schema_to_hotel()

    mock_client_instance = MagicMock()
    mock_client_instance.execute_prompt.return_value = "SELECT * FROM" # Invalid SQL
    mock_get_client.return_value = mock_client_instance
    
    response = client.post("/queries/generate-and-execute/", json={
        "question": "List all Hotels",
        "prompt_type": "dail_sql",
        "shots": 2,
        "llm_type": "openai",
        "model": "gpt-4o-mini",
        "temperature": 0.7,
        "max_tokens": 1000
    })

    assert response.status_code == 400
    assert "detail" in response.json()
    assert "error" in response.json()["detail"]
    assert "query" in response.json()["detail"]

    assert response.json()["detail"]["error"] == "Database query error: incomplete input"
    assert response.json()["detail"]["query"] == "SELECT * FROM"

def test_mask_single_question_and_query_success():
    set_database_schema_to_hotel() 
    
    expected_masked_question ="List the <mask> <mask> and <mask> <mask> of those <mask> who are from <unk> and their <unk> <mask> is <unk> or <unk> <mask> is <unk> ."
    expected_masked_query = "select <mask>, <mask> from <mask> where <mask> like <unk> and (<mask> like <unk> or <mask> like <unk>);"

    response = client.post("/masking/question-and-query/", json={
        "question": "List the guest name and guest address of those guests who are from Glasgow and their first name is Tony or last name is Farrel.",
        "sql_query": f"select guestname, guestaddress from guest where guestaddress like '%Glasgow%' and (guestname like 'Tony%' or guestname like '% Farrel');"
    })
    
    assert response.status_code == 200
    assert "masked_question" in response.json()
    assert "masked_sql_query" in response.json()

    assert response.json()["masked_question"] == expected_masked_question
    assert response.json()["masked_sql_query"] == expected_masked_query

def test_mask_question_and_answer_file_success():
    expected_content = [
        {
            "id": 1,
            "masked_question": "<unk> all <mask> which are in <unk> . Order the <unk> in descending <unk> by <mask> <mask> .",
            "masked_answer": "select * from <mask> where <mask> =<unk> order <mask> <mask> <mask>;"
        },
        {
            "id": 2,
            "masked_question": "<unk> all <mask> whose <mask> <unk> <mask> <unk> <unk> has a <unk> <mask> <unk> .",
            "masked_answer": "select * from <mask> where <mask> like <unk>;"
        }
    ]

    response = client.post("/masking/file/", json={"file_name": "hotel_schema.json"})

    print(response.json())
    
    assert response.status_code == 200
    assert "masked_file_name" in response.json()
    assert response.json()["masked_file_name"].startswith("masked_")

    masked_file_path = os.path.join(
        os.path.abspath(os.path.dirname(__file__)),
        f'../{MASKED_FOLDER_PATH}/{response.json()["masked_file_name"]}'
    )

    with open(masked_file_path, 'r') as f:
        content = json.load(f)

    assert content == expected_content
    os.remove(masked_file_path)

def test_generate_prompt_success():
    set_database_schema_to_hotel()

    expected_prompt = (
        "Table guest, columns = [ guestno, guestname, guestaddress ]\n"
        "Table hotel, columns = [ hotelno, hotelname, city ]\n"
        "Table room, columns = [ roomno, hotelno, type, price ]\n"
        "Table booking, columns = [ hotelno, guestno, datefrom, dateto, roomno ]\n"
        "Q: List all hotels\n"
        "A: SELECT"
    )

    response = client.post("/prompts/generate/", json={
        "prompt_type": "basic",
        "shots": 0,
        "question": "List all hotels"
    })

    assert response.status_code == 200
    assert "generated_prompt" in response.json()

    assert response.json()["generated_prompt"] == expected_prompt

def test_generate_prompt_negative_shots():
    response = client.post("/prompts/generate/", json={
        "prompt_type": "basic",
        "shots": -1,
        "question": "List all hotels"
    })

    assert response.status_code == 400
    assert "detail" in response.json()

    assert response.json()["detail"] == "Shots must be a non-negative integer."

def test_generate_prompt_invalid_type():
    response = client.post("/prompts/generate/", json={
        "prompt_type": "invalid_type",
        "shots": 1,
        "question": "List all Hotels"
    })

    assert response.status_code == 422
    assert "detail" in response.json()
    assert len(response.json()["detail"]) > 0

    error_detail = response.json()["detail"][0]
    assert error_detail["type"] == "enum"
    assert error_detail["loc"] == ["body", "prompt_type"]
    assert error_detail["msg"] == (
        "Input should be 'basic', 'text_representation', 'openai_demonstration', "
        "'code_representation', 'alpaca_sft', 'full_information', 'sql_only' or 'dail_sql'"
    )
    assert error_detail["input"] == "invalid_type"

def test_change_database_success():
    expected_schema = (
        "CREATE TABLE guest (\n\tguestno NUMERIC(5) NOT NULL, \n\tguestname VARCHAR(20), "
        "\n\tguestaddress VARCHAR(50), \n\tPRIMARY KEY (guestno)\n)\n"
        "CREATE TABLE hotel (\n\thotelno VARCHAR(10) NOT NULL, \n\thotelname VARCHAR(20), "
        "\n\tcity VARCHAR(20), \n\tPRIMARY KEY (hotelno)\n)\n"
        "CREATE TABLE room (\n\troomno NUMERIC(5) NOT NULL, \n\thotelno VARCHAR(10) NOT NULL, "
        "\n\ttype VARCHAR(10), \n\tprice DECIMAL(5, 2), \n\tPRIMARY KEY (roomno, hotelno), "
        "\n\tFOREIGN KEY(hotelno) REFERENCES hotel (hotelno)\n)\n"
        "CREATE TABLE booking (\n\thotelno VARCHAR(10) NOT NULL, \n\tguestno NUMERIC(5) NOT NULL, "
        "\n\tdatefrom DATETIME NOT NULL, \n\tdateto DATETIME, \n\troomno NUMERIC(5) NOT NULL, "
        "\n\tPRIMARY KEY (hotelno, guestno, datefrom, roomno), "
        "\n\tFOREIGN KEY(guestno) REFERENCES guest (guestno), \n\tFOREIGN KEY(hotelno) "
        "REFERENCES hotel (hotelno), \n\tFOREIGN KEY(roomno) REFERENCES room (roomno)\n)"
    )

    response = client.post("/database/change/", json={"database_type": "hotel"})

    assert response.status_code == 200
    assert "database_type" in response.json()
    assert "schema" in response.json()

    assert response.json()["database_type"] == "hotel"
    assert response.json()["schema"] == expected_schema

def test_change_database_invalid_type():
    response = client.post("/database/change/", json={"database_type": "invalid_db"})

    assert response.status_code == 422
    assert "detail" in response.json()
    assert len(response.json()["detail"]) > 0
    
    error_detail = response.json()["detail"][0]
    assert error_detail["type"] == "enum"
    assert error_detail["loc"] == ["body", "database_type"]
    assert error_detail["msg"] == ("Input should be 'hotel', 'store', 'healthcare' or 'music_festival'")
    assert error_detail["input"] == "invalid_db"

def test_get_database_schema_success():
    set_database_schema_to_hotel()
    expected_schema = (
        "CREATE TABLE guest (\n\tguestno NUMERIC(5) NOT NULL, \n\tguestname VARCHAR(20), "
        "\n\tguestaddress VARCHAR(50), \n\tPRIMARY KEY (guestno)\n)\n"
        "CREATE TABLE hotel (\n\thotelno VARCHAR(10) NOT NULL, \n\thotelname VARCHAR(20), "
        "\n\tcity VARCHAR(20), \n\tPRIMARY KEY (hotelno)\n)\n"
        "CREATE TABLE room (\n\troomno NUMERIC(5) NOT NULL, \n\thotelno VARCHAR(10) NOT NULL, "
        "\n\ttype VARCHAR(10), \n\tprice DECIMAL(5, 2), \n\tPRIMARY KEY (roomno, hotelno), "
        "\n\tFOREIGN KEY(hotelno) REFERENCES hotel (hotelno)\n)\n"
        "CREATE TABLE booking (\n\thotelno VARCHAR(10) NOT NULL, \n\tguestno NUMERIC(5) NOT NULL, "
        "\n\tdatefrom DATETIME NOT NULL, \n\tdateto DATETIME, \n\troomno NUMERIC(5) NOT NULL, "
        "\n\tPRIMARY KEY (hotelno, guestno, datefrom, roomno), "
        "\n\tFOREIGN KEY(guestno) REFERENCES guest (guestno), \n\tFOREIGN KEY(hotelno) "
        "REFERENCES hotel (hotelno), \n\tFOREIGN KEY(roomno) REFERENCES room (roomno)\n)"
    )

    response = client.get("/database/schema/")

    assert response.status_code == 200
    assert "database_type" in response.json()
    assert "schema" in response.json()

    assert response.json()["database_type"] == "hotel"
    assert response.json()["schema"] == expected_schema
