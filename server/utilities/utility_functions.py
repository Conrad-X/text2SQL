from sqlalchemy import text
from sqlalchemy.orm import Session

def execute_sql_query(db: Session, sql_query: str):
    """
    Executes a SQL query and returns the results as a list of dictionaries.

    Args:
        db (Session): SQLAlchemy session object.
        sql_query (str): SQL query string.

    Returns:
        List[dict]: Query results as a list of dictionaries where each dictionary represents a row.

    Raises:
        ValueError: If sql_query is empty.
        RuntimeError: If there is a database query error.
    """
    if not sql_query:
        raise ValueError("Query parameter is required")

    try:
        result = db.execute(text(sql_query))
        columns = result.keys()
        rows = [dict(zip(columns, row)) for row in result.fetchall()]
        return rows
    except Exception as e:
        raise RuntimeError(f"Database query error: {str(e)}")
