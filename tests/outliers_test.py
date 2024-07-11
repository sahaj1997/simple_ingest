import unittest
import os
import duckdb
from datetime import datetime
from coffeebeans_dataeng_exercise.outliers import Outliers  

class TestOutliers(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db_file = 'tests/resources/test_warehouse.db'
        cls.schema = 'blog_analysis'
        cls.input_table = 'votes'
        cls.table = 'outlier_weeks'

        cls.votes_file_path = 'tests/resources/votes.jsonl'

        cls.con = duckdb.connect(cls.db_file)
        cls.con.execute(f"CREATE SCHEMA IF NOT EXISTS {cls.schema};")
        cls.con.execute(f"DROP TABLE IF EXISTS {cls.schema}.{cls.input_table};")
        cls.con.execute(f"""
            CREATE TABLE IF NOT EXISTS {cls.schema}.{cls.input_table} (
                Id INTEGER PRIMARY KEY,
                UserId INTEGER NULL,
                PostId INTEGER NOT NULL,
                VoteTypeId INTEGER NOT NULL,
                CreationDate TIMESTAMP NOT NULL
            );
        """)
        cls.con.execute(f"""
            INSERT INTO {cls.schema}.{cls.input_table}
            (SELECT Id, UserId, PostId, VoteTypeId, CreationDate FROM read_json_auto('{cls.votes_file_path}'));
        """)

        cls.db_manager = Outliers(cls.db_file)
        cls.db_manager.create_schema_and_table(cls.schema, cls.table, cls.input_table)

    @classmethod
    def tearDownClass(cls):
        cls.con.close()

    def test_view_exists(self):
        con = duckdb.connect(self.db_file)
        result = con.execute(f"SELECT * FROM information_schema.tables WHERE table_schema = '{self.schema}' AND table_name = '{self.table}';").fetchall()
        self.assertTrue(len(result) > 0)
        con.close()

    def test_outlier_detection(self):
        con = duckdb.connect(self.db_file)
        self.con.execute(f"DROP TABLE IF EXISTS {self.schema}.{self.input_table};")
        self.con.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.{self.input_table} (
                Id INTEGER PRIMARY KEY,
                UserId INTEGER NULL,
                PostId INTEGER NOT NULL,
                VoteTypeId INTEGER NOT NULL,
                CreationDate TIMESTAMP NOT NULL
            );
        """)
        self.con.execute(f"""
            INSERT INTO {self.schema}.{self.input_table}
            (SELECT Id, UserId, PostId, VoteTypeId, CreationDate FROM read_json_auto('{self.votes_file_path}'));
        """)
        self.db_manager.create_schema_and_table(self.schema, self.table, self.input_table)
        result = con.execute(f"SELECT * FROM {self.schema}.{self.table};").fetchall()
        expected_outliers = [(2022, 0, 1)
                             , (2022, 1, 3)
                             , (2022, 2, 3)
                             , (2022, 5, 1)
                             , (2022, 6, 1)
                             , (2022, 8, 1)]
        self.assertEqual(len(result), 6)
        self.assertEqual(result, expected_outliers)
        for i, row in enumerate(result):
            self.assertEqual((row[0], row[1], row[2]), expected_outliers[i])
        con.close()

    def test_no_outliers_when_data_is_uniform(self):
        con = duckdb.connect(self.db_file)
        con.execute(f"DELETE FROM {self.schema}.{self.input_table};")
        uniform_file_path = 'tests/resources/uniform_votes.jsonl'
        con.execute(f"""
            INSERT INTO {self.schema}.{self.input_table}
            (SELECT Id, UserId, PostId, VoteTypeId, CreationDate FROM read_json_auto('{uniform_file_path}'));
        """)
        self.db_manager.create_schema_and_table(self.schema, self.table, self.input_table)
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
        self.db_manager.create_schema_and_table(self.schema, self.table, self.input_table)
        result = con.execute(f"SELECT * FROM {self.schema}.{self.table};").fetchall()
        self.assertEqual(len(result), 0)
        con.close()

    def test_view_recreation(self):
        con = duckdb.connect(self.db_file)
        self.db_manager.create_schema_and_table(self.schema, self.table, self.input_table)
        result = con.execute(f"SELECT * FROM information_schema.tables WHERE table_schema = '{self.schema}' AND table_name = '{self.table}';").fetchall()
        self.assertTrue(len(result) > 0)
        con.close()

if __name__ == "__main__":
    unittest.main()
