#requirments:
#sqlglot[rs]


import os
from typing import Dict, List, Optional

from sqlglot import exp, parse_one
from sqlglot.optimizer.qualify import qualify
from utilities.logging_utils import setup_logger

logger = setup_logger(__name__)

def execute_sql(db_path: str, sql: str) -> List:
    """
    Executes an SQL query on a database and returns the results.
    
    Args:
        db_path (str): The path to the database file.
        sql (str): The SQL query string.
        
    Returns:
        List: The results of the SQL query.
    """
    try:
        from sqlite3 import connect
        with connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            return cursor.fetchall()
    except Exception as e:
        logger.error(f"Error in execute_sql: {e}\nSQL: {sql}")
        raise e

def _get_main_parent(expression: exp.Expression) -> Optional[exp.Expression]:
    """
    Retrieves the main parent expression for a given SQL expression.
    
    Args:
        expression (exp.Expression): The SQL expression.
        
    Returns:
        Optional[exp.Expression]: The main parent expression or None if not found.
    """
    parent = expression.parent
    while parent and not isinstance(parent, exp.Subquery):
        parent = parent.parent
    return parent

def _get_table_with_alias(parsed_sql: exp.Expression, alias: str) -> Optional[exp.Table]:
    """
    Retrieves the table associated with a given alias.
    
    Args:
        parsed_sql (exp.Expression): The parsed SQL expression.
        alias (str): The table alias.
        
    Returns:
        Optional[exp.Table]: The table associated with the alias or None if not found.
    """
    return next((table for table in parsed_sql.find_all(exp.Table) if table.alias == alias), None)
    
def get_db_all_tables(db_path: str) -> List[str]:
    """
    Retrieves all table names from the database.
    
    Args:
        db_path (str): The path to the database file.
        
    Returns:
        List[str]: A list of table names.
    """
    try:
        raw_table_names = execute_sql(db_path, "SELECT name FROM sqlite_master WHERE type='table';")
        return [table[0].replace('\"', '').replace('`', '') for table in raw_table_names if table[0] != "sqlite_sequence"]
    except Exception as e:
        logger.error(f"Error in get_db_all_tables: {e}")
        raise e

def get_sql_tables(db_path: str, sql: str) -> List[str]:
    """
    Retrieves table names involved in an SQL query.
    
    Args:
        db_path (str): Path to the database file.
        sql (str): The SQL query string.
        
    Returns:
        List[str]: List of table names involved in the SQL query.
    """
    db_tables = get_db_all_tables(db_path)
    try:
        parsed_tables = list(parse_one(sql, read='sqlite').find_all(exp.Table))
        correct_tables = [
            str(table.name).strip().replace('\"', '').replace('`', '') 
            for table in parsed_tables
            if str(table.name).strip().lower() in [db_table.lower() for db_table in db_tables]
        ]
        return correct_tables
    except Exception as e:
        logger.critical(f"Error in get_sql_tables: {e}\nSQL: {sql}")
        raise e

def get_table_all_columns(db_path: str, table_name: str) -> List[str]:
    """
    Retrieves all column names for a given table.
    
    Args:
        db_path (str): The path to the database file.
        table_name (str): The name of the table.
        
    Returns:
        List[str]: A list of column names.
    """
    try:
        table_info_rows = execute_sql(db_path, f"PRAGMA table_info(`{table_name}`);")
        return [row[1].replace('\"', '').replace('`', '') for row in table_info_rows]
    except Exception as e:
        logger.error(f"Error in get_table_all_columns: {e}\nTable: {table_name}")
        raise e

def get_sql_columns_dict(db_path: str, sql: str) -> Dict[str, List[str]]:
    """
    Retrieves a dictionary of tables and their respective columns involved in an SQL query.
    
    Args:
        db_path (str): Path to the database file.
        sql (str): The SQL query string.
        
    Returns:
        Dict[str, List[str]]: Dictionary of tables and their columns.
    """
    sql = qualify(parse_one(sql, read='sqlite'), qualify_columns=True, validate_qualify_columns=False) if isinstance(sql, str) else sql
    columns_dict = {}

    sub_queries = [subq for subq in sql.find_all(exp.Subquery) if subq != sql]
    for sub_query in sub_queries:
        subq_columns_dict = get_sql_columns_dict(db_path, sub_query)
        for table, columns in subq_columns_dict.items():
            if table not in columns_dict:
                columns_dict[table] = columns
            else:
                columns_dict[table].extend([col for col in columns if col.lower() not in [c.lower() for c in columns_dict[table]]])

    for column in sql.find_all(exp.Column):
        column_name = column.name
        table_alias = column.table
        table = _get_table_with_alias(sql, table_alias) if table_alias else None
        table_name = table.name if table else None

        if not table_name:
            candidate_tables = [t for t in sql.find_all(exp.Table) if _get_main_parent(t) == _get_main_parent(column)]
            for candidate_table in candidate_tables:
                table_columns = get_table_all_columns(db_path, candidate_table.name)
                if column_name.lower() in [col.lower() for col in table_columns]:
                    table_name = candidate_table.name
                    break

        if table_name:
            if table_name not in columns_dict:
                columns_dict[table_name] = []
            if column_name.lower() not in [c.lower() for c in columns_dict[table_name]]:
                columns_dict[table_name].append(column_name)

    return columns_dict