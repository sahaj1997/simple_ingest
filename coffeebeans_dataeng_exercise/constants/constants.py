class OperationType:
    """
    Defines the types of operations that can be performed by batch jobs.
    """
    INGEST = "Ingest"  # Operation type for data ingestion
    OUTLIER = "Outlier"  # Operation type for outlier detection


class SchemaType:
    """
    Defines the types of schemas used in the database.
    """
    VOTES = "votes"  # Schema for voting data
    OUTLIER = "outlier_weeks"  # Schema for outlier detection data


# Constants related to database configuration and file paths
DB_FILE = "warehouse.db"  # Default path to the database file
SCHEMA = "blog_analysis"  # Default schema name for the database
VOTES_TABLE = "votes"  # Table name for storing vote data
OUTLIERS_TABLE = "outlier_weeks"  # Table name for storing outlier data

FILE_PATH = "uncommitted/votes.jsonl"  # Path to the input file for votes data

# Paths to SQL scripts for database operations
# SQL script to create the outlier view
CREATE_OUTLIER_VIEW_PATH = "coffeebeans_dataeng_exercise/db/sql/create_outlier_view.sql"
# SQL script to create the schema
CREATE_SCHEMA_PATH = "coffeebeans_dataeng_exercise/db/sql/create_schema.sql"
# SQL script to create the votes table
CREATE_VOTES_TABLE_PATH = "coffeebeans_dataeng_exercise/db/sql/create_votes_table.sql"
# SQL script to drop tables
DROP_TABLE_PATH = "coffeebeans_dataeng_exercise/db/sql/drop_table.sql"
# SQL script to insert data into the votes table
INSERT_INTO_VOTES_TABLE_PATH = "coffeebeans_dataeng_exercise/db/sql/insert_into_votes_table.sql"
