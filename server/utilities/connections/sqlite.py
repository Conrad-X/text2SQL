"""SQLite database connection utilities.

This module provides utilities for creating and managing SQLite database connections,
particularly for in-memory database operations to improve query performance.
"""

import sqlite3


def make_sqlite_connection(path):
    """Create an in-memory SQLite connection from a database file.
    
    Loads a SQLite database from the provided path into memory for faster
    query execution, and configures the connection to return rows as dictionaries.
    
    Args:
        path: Path to the source SQLite database file.
        
    Returns:
        sqlite3.Connection: An in-memory SQLite connection with the database contents.
    """
    source: sqlite3.Connection
    with sqlite3.connect(str(path)) as source:
        dest = sqlite3.connect(":memory:")
        dest.row_factory = sqlite3.Row
        source.backup(dest)
    return dest
