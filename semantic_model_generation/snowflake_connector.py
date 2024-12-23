from typing import Optional
from typing import Dict, List, Any, Union

from datatypes import Column, Table
from snowflake.connector import connect
from snowflake.connector.connection import SnowflakeConnection

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
