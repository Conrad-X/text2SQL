# FastAPI Text2SQL

A FastAPI application that converts natural language questions into SQL queries using large language models (LLMs).

## Table of Contents
1. [Project Overview](#project-overview)
2. [Project Structure](#project-structure)
3. [Setup Instructions](#setup-instructions)
4. [Database Setup](#database-setup)
5. [API Endpoints](#api-endpoints)
6. [How to Use the FastAPI Interactive Documentation](#how-to-use-the-fastapi-interactive-documentation)


## Project Overview
This FastAPI application leverages large language models (LLMs) to transform natural language questions into SQL queries. It uses SQLite as the database and integrates with OpenAI and Anthropic for query generation.


## Project Structure

```bash
server
│
├── app/
│   ├── main.py                     # The main FastAPI application file that defines the API endpoints and runs the server.
│   ├── db.py                       # Contains database connection setup and session management.
│   └── models.py                   # Defines SQLAlchemy models for the database schema.
│
├── migrations/                     # Directory for Alembic migrations.
│   ├── versions/                   # Folder for migration versions (automatically generated migration scripts).
│   └── env.py                      # Alembic environment configuration for managing database migrations.
│
├── utilities/
│   ├── constants/                  # Folder for constants used across the project.
│   ├── prompt_builder/             # Folder for building prompts.
│   │   ├── database_schema_representation.py  # Database schema representation for prompts.
│   │   └── prompt_builder.py       # Logic for building prompts.
│   └── utility_functions.py        # General utility functions for various operations.
│
├── services/
│   ├── base_client.py              # Base Implementation for all LLM clients.
│   ├── openai_client.py            # Implementation for OpenAI API client.
│   └── anthropic_client.py         # Implementation for Anthropic API client.
│
├── test/                           # Folder for unit tests and test utilities.
│
├── .env                            # Environment variables (e.g., API keys).
├── alembic.ini                     # Configuration file for Alembic migrations.
├── seed_db.py                      # Script for seeding initial data into the database.
├── test.db                         # Pre-populated SQLite database with sample data.
└── requirements.txt                # List of Python dependencies required for the project.
```

## Setup Instructions

To get a local copy of the project up and running, follow these simple steps.

#### 1. Clone the Repository

```sh
https://github.com/Conrad-X/text2SQL.git
```

#### 2. Create a Virtual Environment
```sh
python -m venv venv
source venv/bin/activate
```

#### 3. Install Dependencies
```sh
pip install -r requirements.txt
```

#### 4. Configure Environment Variables
Create a `.env` file in the root directory and add the following variables:

```plaintext
OPENAI_API_KEY=your_openai_api_key
ANTHROPIC_API_KEY=your_anthropic_api_key
```
#### 4. Run the Application

```bash
uvicorn app.main:app --reload
```
The application will be accessible at http://127.0.0.1:8000.

## Database Setup
A pre-populated SQLite database (test.db) of a Hotel Schema is included in the repository. To view this database, install the SQLite Viewer extension for Visual Studio Code.

If you prefer to set up the database from scratch:

#### 1. Database Configuration:
The database connection is configured in app/db.py. By default, the SQLite database URL is set to `sqlite:///./test.db`. Adjust this URL if you need to use a different database file or path.

#### 2. Initialize Alembic:
```sh
alembic init migrations
```

#### 3. Configure Alembic: 
Edit `alembic.ini` to set the database URL for your SQLite database.

#### 4. Create Initial Migration:

```sh
alembic revision --autogenerate -m "Initial Migration"
```
#### 5. Apply Migrations:

```sh
alembic upgrade head
```

#### 6. Populate the database:

```sh
python seed_db.py
```

## API Endpoints

#### 1. Execute SQL Query

- **Endpoint**: `/execute_sql_query/`
- **Method**: `POST`
- **Request Body**:

  ```json
  {
    "query": "SELECT * FROM hotel"
  }
  ```
 - **Description**: Executes the provided SQL query on the database and returns the result.

 - **Response**: The result of the SQL query.

#### 2. Generate and Execute SQL Query (OpenAI)
 - **Endpoint**: /generate_and_execute_sql_query_openai/
 - **Method**: POST
 - **Request Body**:
    ```json
    {
        "question": "List all hotels which are in London. Order the result in descending order by hotel name."
    }
    ```
 - **Description**: Generates an SQL query from a natural language question using OpenAI, executes the query, and returns the result.
 - **Response**: Includes the SQL query, the result, and the prompt used.

#### 3. Generate and Execute SQL Query (Anthropic)
 - **Endpoint**: /generate_and_execute_sql_query_anthropic/
 - **Method**: POST
 - **Request Body**:
    ```json
    {
        "question": "List all hotels which are in London. Order the result in descending order by hotel name."
    }
    ```
 - **Description**:Generates an SQL query from a natural language question using Anthropic, executes the query, and returns the result.
 - **Response**: Includes the SQL query, the result, and the prompt used.

## How to Use the FastAPI Interactive Documentation
FastAPI provides interactive API documentation at /docs and /redoc endpoints. Here’s how to use it:

#### 1. Access the Interactive Documentation:

- **Swagger UI**: Open your web browser and go to http://127.0.0.1:8000/docs. This will display the interactive API documentation provided by Swagger UI.
- **Redoc**: Alternatively, go to http://127.0.0.1:8000/redoc for the Redoc documentation.

#### 2. Try Out Endpoints:
To test an API endpoint:

1. **Navigate to the Desired Endpoint**: In the interactive documentation, find and click on the endpoint you want to test

2. **Click Try it out**: Click the Try it out button to enable input fields for the request body.

3. **Enter the Required Data**: Fill in the input fields with the necessary data. For example:
    - For `/execute_sql_query/`: Enter your SQL query in the query field.
    - For `/generate_and_execute_sql_query_openai/`: Enter a natural language question in the question field.
    - For `/generate_and_execute_sql_query_anthropic/`: Enter a natural language question in the question field.

4. **Click Execute**: Press the Execute button to send the request. The response will be displayed directly in the browser, showing the results of the query or any error messages.
