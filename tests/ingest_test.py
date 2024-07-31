import unittest
import logging
import duckdb
import os
from datetime import datetime
from coffeebeans_dataeng_exercise.batch.batch_factory import BatchFactory
from coffeebeans_dataeng_exercise.constants.constants import SchemaType, OperationType, FILE_PATH

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class IngestTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.file_path = 'tests/resources/votes.jsonl'
        cls.db_file = 'tests/resources/test_warehouse.db'
        cls.schema = 'blog_analysis'
        cls.table = 'votes'

        con = duckdb.connect(cls.db_file)
        con.execute(f"DELETE FROM {cls.schema}.{cls.table};")

        outlier_detection = BatchFactory.operation(OperationType.INGEST, [SchemaType.VOTES], cls.db_file)

        if os.path.exists(cls.file_path):
            outlier_detection.run(cls.file_path)
        else:
            logging.error(f"Data file {cls.file_path} not found.")

        # cls.data_ingestion = Ingest(db_file=cls.db_file, schema=cls.schema, table=cls.table)
        # cls.data_ingestion.create_schema_and_table()
        # cls.data_ingestion.ingest_data(cls.file_path)

    def test_table_exists(self):
        con = duckdb.connect(self.db_file)
        result = con.execute(f"SELECT * FROM information_schema.tables WHERE table_schema = '{self.schema}' AND table_name = '{self.table}';").fetchall()
        self.assertTrue(len(result) > 0)
        con.close()

    def test_ingest(self):
        con = duckdb.connect(self.db_file)
        result = con.execute(f"SELECT COUNT(*) FROM {self.schema}.{self.table};").fetchone()[0]
        self.assertEqual(result, 16)
        con.close()

    def test_specific_data_ingestion(self):
        con = duckdb.connect(self.db_file)
        result = con.execute(f"SELECT * FROM {self.schema}.{self.table} WHERE Id = 1;").fetchall()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0], "1")
        self.assertEqual(result[0][2], "1")
        self.assertEqual(result[0][3], "2")
        self.assertEqual(result[0][5], datetime(2022, 1, 2, 0, 0))
        con.close()

    def test_data_with_null_user_id(self):
        con = duckdb.connect(self.db_file)
        result = con.execute(f"SELECT * FROM {self.schema}.{self.table} WHERE UserId IS NULL;").fetchall()
        self.assertGreater(len(result), 0)
        con.close()

    def test_unique_ids(self):
        con = duckdb.connect(self.db_file)
        result = con.execute(f"SELECT COUNT(DISTINCT Id) FROM {self.schema}.{self.table};").fetchone()[0]
        self.assertEqual(result, 16)
        con.close()

    def test_correct_schema(self):
        con = duckdb.connect(self.db_file)
        result = con.execute(f"SELECT table_schema FROM information_schema.tables WHERE table_name = '{self.table}';").fetchall()
        self.assertEqual(result[0][0], self.schema)
        con.close()

    def test_correct_column_names(self):
        con = duckdb.connect(self.db_file)
        result = con.execute(f"DESCRIBE {self.schema}.{self.table};").fetchall()
        expected_columns = ['Id', 'UserId', 'PostId', 'VoteTypeId', 'BountyAmount','CreationDate']
        self.assertEqual([row[0] for row in result], expected_columns)
        con.close()

if __name__ == "__main__":
    unittest.main()
