import logging

import duckdb

from coffeebeans_dataeng_exercise.constants.constants import DB_FILE


class DatabaseConnection:
    """
    A class to manage a connection to a DuckDB database and execute queries.
    """

    def __init__(self, db_file=DB_FILE):
        """
        Initialize the DatabaseConnection object with a connection to the database.

        Args:
            db_file (str): Path to the database file. Defaults to DB_FILE from constants.
        """
        self.db_file = db_file  # Store the path to the database file
        # Establish a connection to the DuckDB database
        self.con = duckdb.connect(self.db_file)
        # Log the successful connection to the database
        logging.info(f"Connected to database: {self.db_file}")

    def execute(self, query, params=None):
        """
        Execute a SQL query on the database.

        Args:
            query (str): The SQL query to execute.
            params (tuple, optional): Parameters to pass to the query. Defaults to None.
        """
        if params:
            # Execute the query with parameters
            self.con.execute(query, params)
        else:
            # Execute the query without parameters
            self.con.execute(query)
        # Log the executed query
        logging.info(f"Executed query: {query}")

    def close(self):
        """
        Close the database connection.
        """
        # Close the connection to the database
        self.con.close()
        # Log the closure of the database connection
        logging.info(f"Closed database connection: {self.db_file}")
