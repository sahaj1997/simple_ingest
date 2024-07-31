import unittest
from unittest.mock import patch, MagicMock
from coffeebeans_dataeng_exercise.db.db import DatabaseConnection
from coffeebeans_dataeng_exercise.constants.constants import DB_FILE

class TestDatabaseConnection(unittest.TestCase):

    def setUp(self):
        # Common setup code for tests
        self.db_file = 'tests/resources/test_warehouse.db'
        
    @patch('coffeebeans_dataeng_exercise.db.db.duckdb.connect')
    def test_initialization(self, mock_connect):
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection

        
        db_conn = DatabaseConnection()
        
        # Check if duckdb.connect is called with the default DB_FILE
        mock_connect.assert_called_with(DB_FILE)
        self.assertEqual(db_conn.db_file, DB_FILE)
        self.assertEqual(db_conn.con, mock_connection)

    @patch('coffeebeans_dataeng_exercise.db.db.duckdb.connect')
    def test_execute_query_without_params(self, mock_connect):
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        db_conn = DatabaseConnection()
        
        query = "SELECT * FROM test_table"
        db_conn.execute(query)
        
        # Check if execute is called on the connection object with the correct query
        mock_connection.execute.assert_called_with(query)

    @patch('coffeebeans_dataeng_exercise.db.db.duckdb.connect')
    def test_execute_query_with_params(self, mock_connect):
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        db_conn = DatabaseConnection()
        
        query = "SELECT * FROM test_table WHERE id = ?"
        params = (1,)
        db_conn.execute(query, params)
        
        # Check if execute is called on the connection object with the correct query and params
        mock_connection.execute.assert_called_with(query, params)

    @patch('coffeebeans_dataeng_exercise.db.db.duckdb.connect')
    def test_close_connection(self, mock_connect):
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        db_conn = DatabaseConnection()
        
        db_conn.close()
        
        # Check if close is called on the connection object
        mock_connection.close.assert_called_once()

if __name__ == '__main__':
    unittest.main()