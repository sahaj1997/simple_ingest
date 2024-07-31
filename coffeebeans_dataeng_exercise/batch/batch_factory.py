import logging

from coffeebeans_dataeng_exercise.constants.constants import DB_FILE, OperationType
from coffeebeans_dataeng_exercise.ingest_jobs.ingest_data import IngestVotes
from coffeebeans_dataeng_exercise.outlier_jobs.detect_outlier import CalculateOutlier


# Factory Method Pattern: Factory class for creating different types of batch jobs
class BatchFactory:

    @staticmethod
    def operation(type, schemas, db_file=DB_FILE):
        """
        Static method to create and return an instance of a batch job based on the operation type.

        Args:
            type (OperationType): The type of operation to be performed (INGEST or OUTLIER).
            schemas (list): A list of schemas needed for the operation.
            db_file (str): Path to the database file. Defaults to DB_FILE from constants.

        Returns:
            Instance of IngestVotes or CalculateOutlier based on the operation type.

        Raises:
            ValueError: If an unknown operation type is provided.
        """
        # Check the type of operation and create the corresponding batch job
        if type == OperationType.INGEST:
            return IngestVotes(db_file, schemas)
        if type == OperationType.OUTLIER:
            return CalculateOutlier(db_file, schemas)
        else:
            # Log an error if an unknown operation type is provided
            logging.error(f"Unknown ingest type: {type}")
            # Raise a ValueError to indicate the unknown operation type
            raise ValueError(f"Unknown ingest type: {type}")
