import json
import os
from datetime import datetime
import duckdb

class Ingest:
    def __init__(self, db_file="warehouse.db", schema="blog_analysis", table="votes"):
        self.db_file = db_file
        self.schema = schema
        self.table = table
        self.con = duckdb.connect(self.db_file)

    def create_schema_and_table(self):
        self.con.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema};")
        self.con.execute(f"DROP TABLE IF EXISTS {self.schema}.{self.table};")
        self.con.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.{self.table} (
                Id INTEGER PRIMARY KEY,              
                UserId INTEGER NULL,                  
                PostId INTEGER NOT NULL,           
                VoteTypeId INTEGER NOT NULL,           
                CreationDate TIMESTAMP NOT NULL
            );
        """)

    def ingest_data(self, file_path):
        total_lines = sum(1 for _ in open(file_path, 'r', encoding='utf-8'))
        start_time = datetime.now()

        self.con.execute(f"""
            INSERT INTO {self.schema}.{self.table} 
            (SELECT Id, UserId, PostId, VoteTypeId, CreationDate FROM read_json('{file_path}'));
        """)

        end_time = datetime.now()
        total_time = (end_time - start_time).total_seconds()
        print(f"Total time taken : {total_time:.2f} seconds")
        print(f"Progress: 100.00% - Ingestion complete.")

    def close_connection(self):
        self.con.close()

if __name__ == "__main__":
    data_ingestion = Ingest()
    data_ingestion.create_schema_and_table()

    file_path = "uncommitted/votes.jsonl"
    if os.path.exists(file_path):
        data_ingestion.ingest_data(file_path)
    else:
        print(f"Data file {file_path} not found.")

    data_ingestion.close_connection()
