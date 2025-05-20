"""
Common utilities for database connection management.

This module provides functions for managing database connections
such as closing connections safely.
"""

def close_connection(connection):
    """Close a database connection if it exists.

    Safely closes the provided database connection if it is not None.

    Args:
        connection : object
            The database connection object to be closed.
    """
    if connection:
        connection.close()
