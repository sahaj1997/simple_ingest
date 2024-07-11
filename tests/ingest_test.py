import unittest
import os
import duckdb
from datetime import datetime
from coffeebeans_dataeng_exercise.ingest import Ingest

class IngestTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.file_path = 'tests/resources/votes.jsonl'
        cls.db_file = 'tests/resources/test_warehouse.db'
        cls.schema = 'blog_analysis'
        cls.table = 'votes'

        cls.data_ingestion = Ingest(db_file=cls.db_file, schema=cls.schema, table=cls.table)
        cls.data_ingestion.create_schema_and_table()
        cls.data_ingestion.ingest_data(cls.file_path)

    @classmethod
    def tearDownClass(cls):
        cls.data_ingestion.close_connection()

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
        self.assertEqual(result[0][0], 1)
        self.assertEqual(result[0][2], 1)
        self.assertEqual(result[0][3], 2)
        self.assertEqual(result[0][4], datetime(2022, 1, 2, 0, 0))
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
        expected_columns = ['Id', 'UserId', 'PostId', 'VoteTypeId', 'CreationDate']
        self.assertEqual([row[0] for row in result], expected_columns)
        con.close()

if __name__ == "__main__":
    unittest.main()
