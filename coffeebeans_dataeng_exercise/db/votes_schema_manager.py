import logging

from coffeebeans_dataeng_exercise.constants.constants import (
    CREATE_SCHEMA_PATH,  # Path to the SQL file for creating the schema
)
from coffeebeans_dataeng_exercise.constants.constants import (
    CREATE_VOTES_TABLE_PATH,  # Path to the SQL file for creating the votes table
)
from coffeebeans_dataeng_exercise.constants.constants import (
    SCHEMA,  # Default schema name
)
from coffeebeans_dataeng_exercise.constants.constants import (
    VOTES_TABLE,  # Default table name for votes
)
from coffeebeans_dataeng_exercise.db.schema_manager import SchemaManager
from coffeebeans_dataeng_exercise.db.sql.reader import Reader


class VotesSchemaManager(SchemaManager):
    """
    Manages the creation of the schema and votes table in the database.
    Inherits from SchemaManager and implements schema and table creation.
    """

    def __init__(self, db_connection, schema=SCHEMA, table=VOTES_TABLE):
        """
        Initialize the VotesSchemaManager with database connection, schema, and table.

        Args:
            db_connection (DatabaseConnection): The connection to the database.
            schema (str): The schema name. Defaults to SCHEMA from constants.
            table (str): The table name for votes. Defaults to VOTES_TABLE from constants.
        """
        super().__init__(db_connection, schema, table)  # Initialize the parent SchemaManager
        # Log the creation of the SchemaManager for the specified schema and table
        logging.info(f"SchemaManager created for schema: {schema}.{table}")

    def create_schema_and_table(self):
        """
        Create the schema and the votes table in the database if they do not already exist.
        """
        # Read the SQL query for creating the schema and format it with the schema name
        create_schema_query = Reader.read(CREATE_SCHEMA_PATH).format(schema=self.schema)
        # Execute the schema creation query on the database
        self.db_connection.execute(create_schema_query)

        # Read the SQL query for creating the votes table and format it with schema and table names
        create_votes_table_query = Reader.read(CREATE_VOTES_TABLE_PATH).format(
            schema=self.schema, table=self.table)
        # Execute the table creation query on the database
        self.db_connection.execute(create_votes_table_query)

        # Log the successful creation of the schema and table
        logging.info(f"Table created if not existed: {self.schema}.{self.table}")
