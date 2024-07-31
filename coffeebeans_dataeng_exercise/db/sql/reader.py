import logging


class Reader:
    """
    A class to handle reading SQL queries from a file.
    """

    @staticmethod
    def read(sql_path):
        """
        Reads an SQL query from a file and logs the content.

        Args:
            sql_path (str): The path to the file containing the SQL query.

        Returns:
            str: The SQL query read from the file.
        """
        # Open the file in read mode
        with open(sql_path, 'r') as file:
            # Read the entire content of the file into a string
            create_schema_query = file.read()

        # Log the SQL query read from the file
        logging.info(f"Read query : \n {create_schema_query} \n From file: {sql_path}")

        # Return the SQL query
        return create_schema_query
