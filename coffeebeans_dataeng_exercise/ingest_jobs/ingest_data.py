import logging
from datetime import datetime

from coffeebeans_dataeng_exercise.batch.batch_job import BatchJob
from coffeebeans_dataeng_exercise.constants.constants import (
    DB_FILE,  # Default path to the database file
)
from coffeebeans_dataeng_exercise.constants.constants import (
    INSERT_INTO_VOTES_TABLE_PATH,  # Path to the SQL file for inserting data into the votes table
)
from coffeebeans_dataeng_exercise.constants.constants import (
    SchemaType,  # Enumeration of schema types
)
from coffeebeans_dataeng_exercise.db.sql.reader import Reader


# Factory Method Pattern: Concrete implementation of the ingestion process
class IngestVotes(BatchJob):
    """
    Concrete implementation of a batch job that ingests vote data into the database.
    Inherits from BatchJob and implements the transformation process.
    """

    def __init__(self, db_file=DB_FILE, schemas=[SchemaType.VOTES]):
        """
        Initialize the IngestVotes job with database file and schema type.

        Args:
            db_file (str): Path to the database file. Defaults to DB_FILE from constants.
            schemas (list): List of schema types. Defaults to [SchemaType.VOTES].
        """
        super().__init__(db_file, schemas)  # Initialize the parent BatchJob with the database file and schemas

    def transform(self, file_path):
        """
        Transform the data by inserting it into the votes table.

        Args:
            file_path (str): Path to the file containing the data to be ingested.
        """
        start_time = datetime.now()  # Record the start time of the data ingestion process
        logging.info(f"Started data ingestion for file: {file_path}")

        # Read the SQL query for inserting data into the votes table and format it with schema, table, and file path
        insert_into_votes_table_query = Reader.read(INSERT_INTO_VOTES_TABLE_PATH).format(
            # Schema name for the votes table
            schema=self.schema_managers[SchemaType.VOTES].schema,
            # Table name for votes
            table=self.schema_managers[SchemaType.VOTES].table,
            file_path=file_path  # Path to the data file
        )

        # Execute the SQL query to insert data into the votes table
        self.db_connection.execute(insert_into_votes_table_query)

        end_time = datetime.now()  # Record the end time of the data ingestion process
        # Calculate the total time taken
        total_time = (end_time - start_time).total_seconds()
        logging.info(
            f"Completed data ingestion for file: {file_path} in {total_time:.2f} seconds")  # Log the completion time
