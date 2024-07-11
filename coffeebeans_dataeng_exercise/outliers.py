import json
import os
from datetime import datetime

import duckdb


class Outliers:
    def __init__(self, db_file):
        self.db_file = db_file

    def create_schema_and_table(self, schema, table, input_table):
        # Connect to DuckDB
        con = duckdb.connect(self.db_file)

        # Create schema if not exists
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

        # Create the votes table within the specified schema if not exists
        con.execute(f"""
            CREATE OR REPLACE VIEW {schema}.{table} AS
        WITH weekly_votes AS (
            SELECT
                EXTRACT(year FROM CreationDate) AS year,
                CASE 
                    WHEN EXTRACT(WEEK FROM CreationDate) = 52 AND EXTRACT(DAY FROM CreationDate) < 7 
                    THEN 0 
                    ELSE EXTRACT(WEEK FROM CreationDate) 
                END AS week_number,
                COUNT(*) AS vote_count
            FROM {schema}.{input_table}
            GROUP BY 1, 2
        ),
        overall_avg AS (
            SELECT year, AVG(vote_count) AS avg_vote_count
            FROM weekly_votes
            GROUP BY 1
        ),
        outliers AS (
            SELECT
                w.year,
                w.week_number,
                w.vote_count,
                o.avg_vote_count
            FROM weekly_votes w, overall_avg o
            WHERE ABS(1.0 - (w.vote_count / o.avg_vote_count)) > 0.2
        )
        SELECT
            year,
            week_number,
            vote_count
        FROM outliers
        ORDER BY year, week_number;
        """)

        con.close()


if __name__ == "__main__":
    db_manager = Outliers("warehouse.db")

    SCHEMA = "blog_analysis"
    TABLE = "outlier_weeks"
    INPUT_TABLE = "votes"

    # Create schema and table if they do not exist
    db_manager.create_schema_and_table(SCHEMA, TABLE, INPUT_TABLE)
