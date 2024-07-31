from abc import ABC, abstractmethod


class SchemaManager(ABC):
    """
    Abstract base class for managing schema creation and table setup in the database.
    Inherits from ABC to define abstract methods that must be implemented by subclasses.
    """

    def __init__(self, db_connection, schema, table):
        """
        Initialize the SchemaManager with database connection, schema, and table information.

        Args:
            db_connection (DatabaseConnection): The connection to the database.
            schema (str): The name of the schema.
            table (str): The name of the table.
        """
        self.db_connection = db_connection  # Store the database connection
        self.schema = schema  # Store the schema name
        self.table = table  # Store the table name

    @abstractmethod
    def create_schema_and_table(self):
        """
        Abstract method that must be implemented by subclasses to define how the schema
        and table should be created.
        """
        pass
