from typing import Optional
from typing import Dict, List, Any, Union

from datatypes import Column, Table
from snowflake.connector import connect
from snowflake.connector.connection import SnowflakeConnection
import pandas as pd
from loguru import logger

_TABLE_COMMENT_COL = "TABLE_COMMENT"
AUTOGEN_TOKEN = "__"
_autogen_model = "llama3-8b"

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
            comment_prompt = f"Here is a table with below DDL: {tbl_ddl} \nPlease provide a business description for the table. Only return the description without any other text."
            complete_sql = f"select SNOWFLAKE.CORTEX.COMPLETE('{_autogen_model}', '{comment_prompt}')"
            cmt = conn.cursor().execute(complete_sql).fetchall()[0][0]  # type: ignore[union-attr]
            return str(cmt + AUTOGEN_TOKEN)
        except Exception as e:
            logger.warning(f"Unable to auto generate table comment: {e}")
            return ""