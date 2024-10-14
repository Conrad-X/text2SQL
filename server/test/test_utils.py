import pytest
import sqlite3
from unittest.mock import patch, MagicMock
from utilities.utility_functions import *
from utilities.constants.response_messages import (
    ERROR_DATABASE_QUERY_FAILURE, 
    ERROR_SQL_QUERY_REQUIRED, 
    ERROR_INVALID_MODEL_FOR_TYPE,
    ERROR_UNSUPPORTED_CLIENT_TYPE,
    ERROR_SQL_MASKING_FAILED,
    ERROR_FILE_MASKING_FAILED,
    ERROR_UNSUPPORTED_FORMAT_TYPE,
    ERROR_FAILED_FETCH_COLUMN_NAMES,
    ERROR_FAILED_FETCH_TABLE_NAMES
)

def create_in_memory_db():
    """
    Creates an in-memory SQLite database, sets up tables, and inserts sample data.
    Returns the SQLite connection.
    """
    connection = sqlite3.connect(":memory:")

    connection.execute("""
    CREATE TABLE hotel (
        hotelno TEXT PRIMARY KEY,
        hotelname TEXT,
        city TEXT
    );
    """)

    connection.execute("""
    CREATE TABLE guest (
        guestno TEXT PRIMARY KEY,
        guestname TEXT,
        guestaddress TEXT
    );
    """)

    connection.executemany("""
    INSERT INTO hotel (hotelno, hotelname, city)
    VALUES (?, ?, ?);
    """, [
        ("fb01", "Grosvenor", "London"),
        ("fb02", "Watergate", "Paris"),
        ("ch01", "Omni Shoreham", "London"),
        ("ch02", "Phoenix Park", "London"),
        ("dc01", "Latham", "Berlin")
    ])
    
    return connection

def test_execute_sql_query_success():
    connection = create_in_memory_db()

    expected_result = [
        {"hotelno": "fb01", "hotelname": "Grosvenor", "city": "London"},
        {"hotelno": "ch01", "hotelname": "Omni Shoreham", "city": "London"},
        {"hotelno": "ch02", "hotelname": "Phoenix Park", "city": "London"}
    ]
    
    sql_query = "SELECT * FROM hotel WHERE city = 'London';"
    result = execute_sql_query(connection, sql_query)

    assert result == expected_result

def test_execute_sql_query_no_query():
    connection = sqlite3.connect(":memory:")
    
    with pytest.raises(ValueError) as excinfo:
        execute_sql_query(connection, "")
    
    assert str(excinfo.value) == ERROR_SQL_QUERY_REQUIRED

def test_execute_sql_query_invalid_query():
    connection = sqlite3.connect(":memory:")
    
    sql_query = "SELECT * FROM non_existing_table;"
    
    with pytest.raises(RuntimeError) as excinfo:
        execute_sql_query(connection, sql_query)
    
    assert str(excinfo.value) == ERROR_DATABASE_QUERY_FAILURE.format(error="no such table: non_existing_table")

def test_validate_llm_and_model_valid():
    validate_llm_and_model(LLMType.OPENAI, ModelType.OPENAI_GPT4_O)
    validate_llm_and_model(LLMType.OPENAI, ModelType.OPENAI_GPT4_O_MINI)
    validate_llm_and_model(LLMType.ANTHROPIC, ModelType.ANTHROPIC_CLAUDE_3_5_SONNET)
    validate_llm_and_model(LLMType.ANTHROPIC, ModelType.ANTHROPIC_CLAUDE_3_SONNET)
    validate_llm_and_model(LLMType.ANTHROPIC, ModelType.ANTHROPIC_CLAUDE_3_HAIKU)

def test_validate_llm_and_model_invalid_llm_type():
    with pytest.raises(ValueError) as excinfo:
        validate_llm_and_model("invalid_llm_type", ModelType.OPENAI_GPT4_O)
    
    assert str(excinfo.value) == ERROR_UNSUPPORTED_CLIENT_TYPE

def test_validate_llm_and_model_invalid_model_for_llm_type():
    expected_error = ERROR_INVALID_MODEL_FOR_TYPE.format(
        model=ModelType.ANTHROPIC_CLAUDE_3_SONNET.value, 
        llm_type=LLMType.OPENAI.value
    )

    with pytest.raises(ValueError) as excinfo:
        validate_llm_and_model(LLMType.OPENAI, ModelType.ANTHROPIC_CLAUDE_3_SONNET)

    assert str(excinfo.value) == expected_error
    
def test_get_table_names_success():
    connection = create_in_memory_db()

    result = get_table_names(connection)
    
    assert sorted(result) == sorted(["hotel", "guest"])

@patch("utilities.utility_functions.execute_sql_query", side_effect=Exception("Simulated database error"))
def test_get_table_names_runtime_error(mock_execute_sql_query):
    connection = sqlite3.connect(":memory:")
    
    expected_error_message = ERROR_FAILED_FETCH_TABLE_NAMES.format(error="Simulated database error")
    
    with pytest.raises(RuntimeError) as excinfo:
        get_table_names(connection)
    
    assert str(excinfo.value) == expected_error_message

def test_get_table_columns_success():
    connection = create_in_memory_db()
    
    result = get_table_columns(connection, "hotel")
    
    assert sorted(result) == sorted(["hotelno", "hotelname", "city"])

@patch("utilities.utility_functions.execute_sql_query", side_effect=Exception("Simulated database error"))
def test_get_table_columns_runtime_error(mock_execute_sql_query):
    connection = create_in_memory_db()
    
    expected_error_message = ERROR_FAILED_FETCH_COLUMN_NAMES.format(error="Simulated database error")
    
    with pytest.raises(RuntimeError) as excinfo:
        get_table_columns(connection, "hotel")
    
    assert str(excinfo.value) == expected_error_message

def test_get_array_of_table_and_column_name():
    expected_result = ['alembic_version', 'guest', 'hotel', 'room', 'booking', 
                       'version_num', 
                       'guestno', 'guestname', 'guestaddress', 
                       'hotelno', 'hotelname', 'city', 
                       'roomno', 'hotelno', 'type', 'price', 
                       'hotelno', 'guestno', 'datefrom', 'dateto', 'roomno']

    result = get_array_of_table_and_column_name("./databases/hotel.db")
    assert result == expected_result

def test_format_schema_basic():
    expected_output = """Table guest, columns = [ guestno, guestname, guestaddress ]
Table hotel, columns = [ hotelno, hotelname, city ]
Table room, columns = [ roomno, hotelno, type, price ]
Table booking, columns = [ hotelno, guestno, datefrom, dateto, roomno ]"""
    
    result = format_schema(FormatType.BASIC, "./databases/hotel.db")
    assert result == expected_output

def test_format_schema_text():
    # Expected output for text format
    expected_output = """guest: guestno, guestname, guestaddress
hotel: hotelno, hotelname, city
room: roomno, hotelno, type, price
booking: hotelno, guestno, datefrom, dateto, roomno"""
    
    result = format_schema(FormatType.TEXT, "./databases/hotel.db")
    assert result == expected_output

def normalize_whitespace(sql_string):
    return re.sub(r'\s+', ' ', sql_string.strip())

def test_format_schema_code():
    expected_output = """CREATE TABLE guest (
        guestno NUMERIC(5) NOT NULL,
        guestname VARCHAR(20),
        guestaddress VARCHAR(50),
        PRIMARY KEY (guestno)
    )
    CREATE TABLE hotel (
        hotelno VARCHAR(10) NOT NULL,
        hotelname VARCHAR(20),
        city VARCHAR(20),
        PRIMARY KEY (hotelno)
    )
    CREATE TABLE room (
        roomno NUMERIC(5) NOT NULL,
        hotelno VARCHAR(10) NOT NULL,
        type VARCHAR(10),
        price DECIMAL(5, 2),
        PRIMARY KEY (roomno, hotelno),
        FOREIGN KEY(hotelno) REFERENCES hotel (hotelno)
    )
    CREATE TABLE booking (
        hotelno VARCHAR(10) NOT NULL,
        guestno NUMERIC(5) NOT NULL,
        datefrom DATETIME NOT NULL,
        dateto DATETIME,
        roomno NUMERIC(5) NOT NULL,
        PRIMARY KEY (hotelno, guestno, datefrom, roomno),
        FOREIGN KEY(guestno) REFERENCES guest (guestno),
        FOREIGN KEY(hotelno) REFERENCES hotel (hotelno),
        FOREIGN KEY(roomno) REFERENCES room (roomno)
    )"""

    result = format_schema(FormatType.CODE, "./databases/hotel.db")
    
    normalized_result = normalize_whitespace(result)
    normalized_expected = normalize_whitespace(expected_output)
    
    assert normalized_result == normalized_expected, f"Expected:\n{normalized_expected}\n\nGot:\n{normalized_result}"

def test_format_schema_openai():
    expected_output = """# guest ( guestno, guestname, guestaddress )
# hotel ( hotelno, hotelname, city )
# room ( roomno, hotelno, type, price )
# booking ( hotelno, guestno, datefrom, dateto, roomno )"""
    
    result = format_schema(FormatType.OPENAI, "./databases/hotel.db")
    assert result == expected_output

def test_convert_word_to_singular_form():
    assert convert_word_to_singular_form("dogs") == "dog"
    assert convert_word_to_singular_form("cats") == "cat"
    assert convert_word_to_singular_form("children") == "child"
    assert convert_word_to_singular_form("geese") == "goose"
    assert convert_word_to_singular_form("mouse") == "mouse"

def test_mask_question():
    question = "Get the details of patients with appointments scheduled in the next 7 days."
    table_and_column_names = ["patients", "appointments"]
    
    expected_output = "Get the <unk> of <mask> with <mask> scheduled in the <unk> <unk> <unk> ."
    
    masked_output = mask_question(question, table_and_column_names)
    assert masked_output == expected_output, f"Expected '{expected_output}', but got '{masked_output}'"

def test_mask_sql_query():
    input_query = "SELECT d.department_name, COUNT(p.id) AS patient_count FROM department d LEFT JOIN patient p ON d.id = p.department_id GROUP BY d.department_name;"
    expected_output = "SELECT <mask>.<mask>, COUNT(<mask>.<mask>) AS <mask> FROM <mask> <mask> LEFT JOIN <mask> <mask> ON <mask>.<mask> = <mask>.<mask> GROUP <mask> <mask>.<mask>;"

    output = mask_sql_query(input_query)
    assert output == expected_output, f"Test failed: expected '{expected_output}', got '{output}'"

def test_mask_question_and_answer_file_success():
    file_name = "hotel_schema.json"
    table_and_column_names = ['alembic_version', 'guest', 'hotel', 'room', 'booking', 
                       'version_num', 
                       'guestno', 'guestname', 'guestaddress', 
                       'hotelno', 'hotelname', 'city', 
                       'roomno', 'hotelno', 'type', 'price', 
                       'hotelno', 'guestno', 'datefrom', 'dateto', 'roomno']

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

    result = mask_question_and_answer_files(file_name, table_and_column_names, "<mask>", "<unk>")
    assert result.startswith("masked_")

    masked_file_path = os.path.join(
        os.path.abspath(os.path.dirname(__file__)),
        f'../{MASKED_FOLDER_PATH}/{result}'
    )

    with open(masked_file_path, 'r') as f:
        content = json.load(f)

    assert content == expected_content
    os.remove(masked_file_path)