from typing import Optional
from typing import Dict, List, Any, Union

from datatypes import Column, Table
from snowflake.connector import connect
from snowflake.connector.connection import SnowflakeConnection
import pandas as pd
from loguru import logger
from openai import OpenAI
from dotenv import load_dotenv
import os
import string
from config import (
    LLM,
    OPENAI_MODEL,
    ADD_SYNONYMS,
    LLM_ENUM,
    SYNONYM_NUMBER
)

load_dotenv()

_TABLE_COMMENT_COL = "TABLE_COMMENT"
_COLUMN_NAME_COL = "COLUMN_NAME"
_COLUMN_COMMENT_ALIAS = "COLUMN_COMMENT"
AUTOGEN_TOKEN = "__"
_autogen_model = "llama3-8b"
SYNONYM_PROMPT="""Here is column from table {table_name}{table_comment_part}:
    name: {column_name};
    type: {datatype};
    values: {values};
    Please provide a list of {synonym_number} synonyms in natural language for the column seperated by a ";". Only return the list of synonyms nothing else. Do not add any punctuation except for ;."""

COLUMN_DESCRIPTION_PROMPT="""Here is column from table {table_name}.{table_comment_part}:
            name: {column_name};
            type: {datatype};
            values: {values};
            Please provide a business description for the column. Only return the description without any other text."""

TABLE_DESCRIPTION_PROMPT="Here is a table with below DDL: {ddl} \nPlease provide a business description for the table. Only return the description without any other text."


def process_synonyms(synonyms):
    # Create a translation table to remove punctuation
    try:
        translator = str.maketrans('', '', string.punctuation + "'")
        split=synonyms.split(';')
        split=[i[1:] if i[0]==' ' else i for i in split]
        # Split the input string and remove punctuation from each synonym
        return [synonym.translate(translator) for synonym in split]
    except Exception as e:
        logger.error(f"Exception in processing synonyms: {e}")
        return [" "]

class SnowflakeConnector:

    def __init__(self):
        self.conn=None

    def _create_connection_parameters(self,
        user: str,
        account: str,
        password: Optional[str] = None,
        host: Optional[str] = None,
        role: Optional[str] = None,
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        authenticator: Optional[str] = None,
        passcode: Optional[str] = None,
        passcode_in_password: Optional[bool] = None,
    ) -> Dict[str, Union[str, bool]]:
        connection_parameters: Dict[str, Union[str, bool]] = dict(
            user=user, account=account
        )
        if password:
            connection_parameters["password"] = password
        if role:
            connection_parameters["role"] = role
        if warehouse:
            connection_parameters["warehouse"] = warehouse
        if database:
            connection_parameters["database"] = database
        if schema:
            connection_parameters["schema"] = schema
        if authenticator:
            connection_parameters["authenticator"] = authenticator
        if host:
            connection_parameters["host"] = host
        if passcode:
            connection_parameters["passcode"] = passcode
        if passcode_in_password:
            connection_parameters["passcode_in_password"] = passcode_in_password
        return connection_parameters


    def _connection(self,
        connection_parameters: Dict[str, Union[str, bool]]
    ):
        # https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect
        return connect(**connection_parameters)


    def snowflake_connection(self,
        user: str,
        account: str,
        role: str,
        warehouse: str,
        password: Optional[str] = None,
        host: Optional[str] = None,
        authenticator: Optional[str] = None,
        passcode: Optional[str] = None,
        passcode_in_password: Optional[bool] = None,
        database: Optional[str] = None,
    ):
        """
        Returns a Snowflake Connection to the specified account.
        """

        self.conn= self._connection(
            self._create_connection_parameters(
                user=user,
                password=password,
                host=host,
                account=account,
                role=role,
                warehouse=warehouse,
                authenticator=authenticator,
                passcode=passcode,
                passcode_in_password=passcode_in_password,
                database=database
            )
        )

        return self.conn

    def execute(self, query: str) -> Dict[str, List[Any]]:
        """
        Executes a query on the Snowflake connection.
        """
        cursor = self.conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()

def openai_response(prompt):
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    completion = client.chat.completions.create(
    model=OPENAI_MODEL,
    messages=[
        {"role": "user", "content": prompt}
    ]
    )
    return completion.choices[0].message.content

def cortex_response(conn, prompt):
    complete_sql = f"select SNOWFLAKE.CORTEX.COMPLETE('{_autogen_model}', '{prompt}')"
    cmt = conn.cursor().execute(complete_sql).fetchall()[0][0]
    return cmt

def get_llm_response(prompt, conn):
    if LLM==LLM_ENUM.Cortex.value:
        return cortex_response(conn, prompt)
    elif LLM==LLM_ENUM.OpenAI.value:
        return openai_response(prompt)

def get_table_comment(
    conn: SnowflakeConnection,
    schema_name: str,
    table_name: str,
    columns_df: pd.DataFrame,
) -> str:
    if len(columns_df[_TABLE_COMMENT_COL])>0 and columns_df[_TABLE_COMMENT_COL].iloc[0]:
        return columns_df[_TABLE_COMMENT_COL].iloc[0]  # type: ignore[no-any-return]
    else:
        # auto-generate table comment if it is not provided.
        try:
            tbl_ddl = (
                conn.cursor()  # type: ignore[union-attr]
                .execute(f"select get_ddl('table', '{schema_name}.{table_name.lower()}');")
                .fetchall()[0][0]
                .replace("'", "\\'")
            )
            comment_prompt = TABLE_DESCRIPTION_PROMPT.format(ddl=tbl_ddl)
            cmt = get_llm_response(comment_prompt,conn)
            return str(cmt)
        except Exception as e:
            logger.warning(f"Unable to auto generate table comment: {e}")
            return ""
    
def get_col_comment_from_bird(column_row):
    table_name=column_row['TABLE_NAME'].lower()
    bird_path=f'../server/data/bird/train/train_databases/retails/database_description/{table_name}.csv'
    table_df=pd.read_csv(bird_path)
    column_description=table_df[table_df['original_column_name']==column_row['COLUMN_NAME'].lower()].iloc[0,1]
    return column_description



def get_column_comment(
    conn: SnowflakeConnection, column_row: pd.Series, column_values: Optional[List[str]], table_comment=None
) -> str:   
    if column_row[_COLUMN_COMMENT_ALIAS]:
        return column_row[_COLUMN_COMMENT_ALIAS]  # type: ignore[no-any-return]
    else:
        # auto-generate column comment if it is not provided.
        try:
            table_comment_part = f" The table description is {table_comment}" if table_comment else ""

            comment_prompt=COLUMN_DESCRIPTION_PROMPT.format(
                table_name=column_row['TABLE_NAME'],
                table_comment_part=table_comment_part,
                datatype=column_row['DATA_TYPE'],
                column_name=column_row['COLUMN_NAME'],
                values=';'.join(column_values) if column_values else ""
            )            
            comment_prompt = comment_prompt.replace("'", "\\'")
            cmt = get_llm_response(comment_prompt, conn)
          
            # if you want to return column comments from Bird descriptions uncomment this:
            # cmt = get_col_comment_from_bird(column_row)
            return str(cmt)
        except Exception as e:
            logger.warning(f"Unable to auto generate column comment: {e}")
            return ""
    
def get_column_synonyms(conn, column_row, column_values, table_comment=None):
    table_comment_part = f" The table description is {table_comment}." if table_comment else ""

    prompt=SYNONYM_PROMPT.format(
        table_name=column_row['TABLE_NAME'],
        table_comment_part=table_comment_part,
        datatype=column_row['DATA_TYPE'],
        column_name=column_row['COLUMN_NAME'],
        values=';'.join(column_values) if column_values else "",
        synonym_number=SYNONYM_NUMBER
    )

    if not ADD_SYNONYMS:
        return [' ']
    prompt = prompt.replace("'", "\\'")
    resp=get_llm_response(prompt, conn)
    synonyms=process_synonyms(resp)
    return synonyms