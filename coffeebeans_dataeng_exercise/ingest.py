import logging
import os

from coffeebeans_dataeng_exercise.batch.batch_factory import BatchFactory
from coffeebeans_dataeng_exercise.constants.constants import (
    FILE_PATH,  # Path to the data file to be processed
)
from coffeebeans_dataeng_exercise.constants.constants import (
    OperationType,  # Enumeration of operation types (e.g., INGEST)
)
from coffeebeans_dataeng_exercise.constants.constants import (
    SchemaType,  # Enumeration of schema types (e.g., VOTES)
)

# Configure logging to display INFO level messages and above, with a specific format
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

if __name__ == "__main__":
    """
    Main entry point of the script. Creates and runs a batch job based on the operation type.
    """
    # Create an instance of the batch job based on the operation type (INGEST) and schema type (VOTES)
    data_ingestion = BatchFactory.operation(OperationType.INGEST, [SchemaType.VOTES])

    # Check if the data file exists at the specified path
    if os.path.exists(FILE_PATH):
        # Run the batch job with the data file path
        data_ingestion.run(FILE_PATH)
    else:
        # Log an error if the data file is not found
        logging.error(f"Data file {FILE_PATH} not found.")
