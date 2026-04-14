import os

import psycopg2


def get_connection():
    return psycopg2.connect(
        dbname=os.getenv("PGDATABASE", "sales_db"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", "postgres"),
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("PGPORT", "5432"),
    )


def run_query_with_columns(query, params=None):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            if cur.description is None:
                return [], []
            columns = [desc[0] for desc in cur.description]
            return columns, cur.fetchall()


def run_query(query, params=None):
    _, results = run_query_with_columns(query, params)
    return results
