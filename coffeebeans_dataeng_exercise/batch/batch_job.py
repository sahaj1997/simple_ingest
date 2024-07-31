import logging
from abc import ABC, abstractmethod

from coffeebeans_dataeng_exercise.db.db import DatabaseConnection
from coffeebeans_dataeng_exercise.db.schema_factory import SchemaFactory
from coffeebeans_dataeng_exercise.db.schema_manager import SchemaManager


# Template Method Pattern: Define the skeleton of the batch process
class BatchJob(ABC):

    def __init__(self, db_file, schemas: list[str]):
        """
        Initializes the BatchJob with a database connection and schema information.

        Args:
            db_file (str): Path to the database file.
            schemas (list[str]): List of schema names to be managed.
        """
        # Create a database connection
        self.db_connection = DatabaseConnection(db_file)
        # Dictionary to hold schema managers for each schema
        self.schema_managers: dict[str, SchemaManager] = {}
        # List of schemas to be managed
        self.schemas = schemas
        logging.info("Batch Job initialized")

    def set_schema_manager(self, schemas):
        """
        Sets up schema managers for the given schemas using SchemaFactory.

        Args:
            schemas (list[str]): List of schema names to set up.

        Returns:
            dict[str, SchemaManager]: Dictionary of schema managers keyed by schema name.
        """
        for schema in schemas:
            if schema not in self.schema_managers:
                # Create and store a schema manager for each schema
                self.schema_managers[schema] = SchemaFactory.schema(
                    schema, self.db_connection)
        return self.schema_managers

    def run_schema_managers(self):
        """
        Executes the schema creation process for all schema managers.
        """
        for schema_obj in self.schema_managers.values():
            # Create the schema and associated tables
            schema_obj.create_schema_and_table()

    def run(self, file_path):
        """
        Runs the batch job process: sets schema managers, runs schema managers,
        transforms data from the given file, and closes the database connection.

        Args:
            file_path (str): Path to the file to be processed.
        """
        self.set_schema_manager(self.schemas)  # Set up schema managers
        self.run_schema_managers()  # Create schemas and tables
        self.transform(file_path)  # Process and transform the data
        self.close_connection()  # Close the database connection

    @abstractmethod
    def transform(self, file_path):
        """
        Abstract method to be implemented by subclasses to define the data transformation logic.

        Args:
            file_path (str): Path to the file to be transformed.
        """
        pass

    def close_connection(self):
        """
        Closes the database connection and logs the closure.
        """
        self.db_connection.close()
        logging.info("Batch Job connection closed")
