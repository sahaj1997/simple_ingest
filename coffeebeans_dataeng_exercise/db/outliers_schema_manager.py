import logging

from coffeebeans_dataeng_exercise.constants.constants import (
    CREATE_SCHEMA_PATH,  # Path to the SQL file for creating the schema
)
from coffeebeans_dataeng_exercise.constants.constants import (
    OUTLIERS_TABLE,  # Name of the table for outliers
)
from coffeebeans_dataeng_exercise.constants.constants import (
    SCHEMA,  # Default schema name
)
from coffeebeans_dataeng_exercise.db.schema_manager import SchemaManager
from coffeebeans_dataeng_exercise.db.sql.reader import Reader


class OutliersSchemaManager(SchemaManager):
    """
    Manages the schema and table creation for outliers in the database.
    Inherits from SchemaManager.
    """

    def __init__(self, db_connection, schema=SCHEMA, table=OUTLIERS_TABLE):
        """
        Initialize the OutliersSchemaManager with database connection, schema, and table.

        Args:
            db_connection (DatabaseConnection): The connection to the database.
            schema (str): The schema name. Defaults to SCHEMA from constants.
            table (str): The table name. Defaults to OUTLIERS_TABLE from constants.
        """
        super().__init__(db_connection, schema, table)  # Initialize the parent SchemaManager
        # Log the creation of the SchemaManager for the specified schema and table
        logging.info(f"SchemaManager created for schema: {schema}.{table}")

    def create_schema_and_table(self):
        """
        Create the schema and table in the database if they do not already exist.
        """
        # Read the SQL query for creating the schema from the file and format it with the schema name
        create_schema_query = Reader.read(CREATE_SCHEMA_PATH).format(schema=self.schema)
        # Execute the schema creation query on the database
        self.db_connection.execute(create_schema_query)
        # Log the successful creation of the schema
        logging.info(f"Schema created if not existed: {self.schema}")
