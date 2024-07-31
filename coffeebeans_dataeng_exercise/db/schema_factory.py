from coffeebeans_dataeng_exercise.constants.constants import SchemaType
from coffeebeans_dataeng_exercise.db.outliers_schema_manager import (
    OutliersSchemaManager,
)
from coffeebeans_dataeng_exercise.db.votes_schema_manager import VotesSchemaManager


# Factory Method Pattern: Factory class for creating different types of schema managers
class SchemaFactory:
    @staticmethod
    def schema(type, db_connection):
        """
        Factory method to create and return an instance of a schema manager based on the type.

        Args:
            type (SchemaType): The type of schema manager to create (VOTES or OUTLIER).
            db_connection (DatabaseConnection): The connection to the database.

        Returns:
            SchemaManager: An instance of the appropriate schema manager.

        Raises:
            ValueError: If an unknown schema type is provided.
        """
        # Check the type of schema and create the corresponding schema manager
        if type == SchemaType.VOTES:
            # Return a VotesSchemaManager for VOTES type
            return VotesSchemaManager(db_connection)
        if type == SchemaType.OUTLIER:
            # Return an OutliersSchemaManager for OUTLIER type
            return OutliersSchemaManager(db_connection)
        else:
            # Raise an error if the provided schema type is unknown
            raise ValueError(f"Unknown schema type: {type}")
