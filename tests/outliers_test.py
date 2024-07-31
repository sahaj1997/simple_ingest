import unittest
import os
import duckdb
import logging
from coffeebeans_dataeng_exercise.batch.batch_factory import BatchFactory
from coffeebeans_dataeng_exercise.constants.constants import SchemaType, OperationType, FILE_PATH

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class TestOutliers(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db_file = 'tests/resources/test_warehouse.db'
        cls.schema = 'blog_analysis'
        cls.input_table = 'votes'
        cls.table = 'outlier_weeks'

        cls.votes_file_path = 'tests/resources/votes.jsonl'

        outlier_detection = BatchFactory.operation(OperationType.OUTLIER, [SchemaType.VOTES, SchemaType.OUTLIER], cls.db_file)

        if os.path.exists(cls.votes_file_path):
            outlier_detection.run(cls.votes_file_path)
        else:
            logging.error(f"Data file {cls.votes_file_path} not found.")

    def test_view_exists(self):
        con = duckdb.connect(self.db_file)
        result = con.execute(f"SELECT * FROM information_schema.tables WHERE table_schema = '{self.schema}' AND table_name = '{self.table}';").fetchall()
        self.assertTrue(len(result) > 0)
        con.close()

    def test_outlier_detection(self):
        con = duckdb.connect(self.db_file)
        con.execute(f"DELETE FROM {self.schema}.{self.input_table};")
        data_ingestion = BatchFactory.operation(OperationType.INGEST, [SchemaType.VOTES], self.db_file)
        if os.path.exists(self.votes_file_path):
            data_ingestion.run(self.votes_file_path)
        else:
            logging.error(f"Data file {self.votes_file_path} not found.")
        
        outlier_detection = BatchFactory.operation(OperationType.OUTLIER, [SchemaType.VOTES, SchemaType.OUTLIER], self.db_file)

        if os.path.exists(self.votes_file_path):
            outlier_detection.run(self.votes_file_path)
        else:
            logging.error(f"Data file {self.votes_file_path} not found.")
        result = con.execute(f"SELECT * FROM {self.schema}.{self.table};").fetchall()
        expected_outliers = [(2022, '00', 1)
                             , (2022, '01', 3)
                             , (2022, '02', 3)
                             , (2022, '05', 1)
                             , (2022, '06', 1)
                             , (2022, '08', 1)]
        print(result)
        self.assertEqual(len(result), 6)
        self.assertEqual(result, expected_outliers)
        for i, row in enumerate(result):
            self.assertEqual((row[0], row[1], row[2]), expected_outliers[i])
        con.close()

    def test_no_outliers_when_data_is_uniform(self):
        con = duckdb.connect(self.db_file)
        con.execute(f"DELETE FROM {self.schema}.{self.input_table};")
        uniform_file_path = 'tests/resources/uniform_votes.jsonl'
        data_ingestion = BatchFactory.operation(OperationType.INGEST, [SchemaType.VOTES], self.db_file)
        if os.path.exists(uniform_file_path):
            data_ingestion.run(uniform_file_path)
        else:
            logging.error(f"Data file {uniform_file_path} not found.")
        
        outlier_detection = BatchFactory.operation(OperationType.OUTLIER, [SchemaType.VOTES, SchemaType.OUTLIER], self.db_file)

        if os.path.exists(uniform_file_path):
            outlier_detection.run(uniform_file_path)
        else:
            logging.error(f"Data file {uniform_file_path} not found.")
        result = con.execute(f"SELECT * FROM {self.schema}.{self.table};").fetchall()
        self.assertEqual(len(result), 0)
        con.close()

    def test_correct_schema(self):
        con = duckdb.connect(self.db_file)
        result = con.execute(f"SELECT table_schema FROM information_schema.tables WHERE table_name = '{self.table}';").fetchall()
        self.assertEqual(result[0][0], self.schema)
        con.close()

    def test_correct_column_names(self):
        con = duckdb.connect(self.db_file)
        result = con.execute(f"DESCRIBE {self.schema}.{self.table};").fetchall()
        expected_columns = ['year', 'week_number', 'vote_count']
        self.assertEqual([row[0] for row in result], expected_columns)
        con.close()

    def test_empty_input_table(self):
        con = duckdb.connect(self.db_file)
        con.execute(f"DELETE FROM {self.schema}.{self.input_table};")
        result = con.execute(f"SELECT * FROM {self.schema}.{self.table};").fetchall()
        self.assertEqual(len(result), 0)
        con.close()

    def test_view_recreation(self):
        con = duckdb.connect(self.db_file)
        result = con.execute(f"SELECT * FROM information_schema.tables WHERE table_schema = '{self.schema}' AND table_name = '{self.table}';").fetchall()
        self.assertTrue(len(result) > 0)
        con.close()

if __name__ == "__main__":
    unittest.main()
