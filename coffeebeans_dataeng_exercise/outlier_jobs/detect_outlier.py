import logging
from datetime import datetime

from coffeebeans_dataeng_exercise.batch.batch_job import BatchJob
from coffeebeans_dataeng_exercise.constants.constants import (
    CREATE_OUTLIER_VIEW_PATH,  # Path to the SQL file for creating the outlier view
)
from coffeebeans_dataeng_exercise.constants.constants import (
    DB_FILE,  # Default path to the database file
)
from coffeebeans_dataeng_exercise.constants.constants import (
    SchemaType,  # Enumeration of schema types
)
from coffeebeans_dataeng_exercise.db.sql.reader import Reader


# Factory Method Pattern: Concrete implementation of the outlier detection process
class CalculateOutlier(BatchJob):
    """
    Concrete implementation of a batch job that detects outliers by creating a view in the database.
    Inherits from BatchJob and implements the transformation process for outlier detection.
    """

    def __init__(self, db_file=DB_FILE, schemas=[SchemaType.VOTES, SchemaType.OUTLIER]):
        """
        Initialize the CalculateOutlier job with database file and schema types.

        Args:
            db_file (str): Path to the database file. Defaults to DB_FILE from constants.
            schemas (list): List of schema types. Defaults to [SchemaType.VOTES, SchemaType.OUTLIER].
        """
        super().__init__(db_file, schemas)  # Initialize the parent BatchJob with the database file and schemas

    def transform(self, file_path):
        """
        Transform the data by creating an outlier view in the database.

        Args:
            file_path (str): Path to the file containing the data to be processed (not used in this method).
        """
        start_time = datetime.now()  # Record the start time of the outlier detection process
        logging.info(f"Started outlier detection for file: {file_path}")

        # Read the SQL query for creating the outlier view and format it with source and sink schema/table names
        create_outlier_view_query = Reader.read(CREATE_OUTLIER_VIEW_PATH).format(
            # Schema name for the source table
            source_schema=self.schema_managers[SchemaType.VOTES].schema,
            # Table name for the source table
            source_table=self.schema_managers[SchemaType.VOTES].table,
            # Schema name for the sink table (outliers view)
            sink_schema=self.schema_managers[SchemaType.OUTLIER].schema,
            # Table name for the sink table (outliers view)
            sink_table=self.schema_managers[SchemaType.OUTLIER].table
        )

        # Execute the SQL query to create the outlier view in the database
        self.db_connection.execute(create_outlier_view_query)

        end_time = datetime.now()  # Record the end time of the outlier detection process
        # Calculate the total time taken
        total_time = (end_time - start_time).total_seconds()
        logging.info(
            f"Completed outlier detection for file: {file_path} in {total_time:.2f} seconds")  # Log the completion time
