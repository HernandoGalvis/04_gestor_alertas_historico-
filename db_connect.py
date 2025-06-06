import psycopg2
import psycopg2.extras
from config import DB_CONFIG

def get_connection():
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        database=DB_CONFIG["database"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"]
    )

def fetch_dataframe(query, params=None):
    import pandas as pd
    conn = get_connection()
    df = pd.read_sql(query, conn, params=params)
    conn.close()
    return df

def fetchall_dict(query, params=None):
    conn = get_connection()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query, params)
        results = cur.fetchall()
    conn.close()
    return results

def execute_many(query, data):
    conn = get_connection()
    with conn.cursor() as cur:
        cur.executemany(query, data)
        conn.commit()
    conn.close()