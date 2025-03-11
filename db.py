# db.py
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    conn = psycopg2.connect(
        host=os.getenv('PGHOST'),
        database=os.getenv('PGDATABASE'),
        user=os.getenv('PGUSER'),
        password=os.getenv('PGPASSWORD'),
        port=os.getenv('PGPORT')
    )
    return conn

def create_table():
    conn = get_connection()
    cur = conn.cursor()
    # Drop the table if it already exists
    cur.execute("DROP TABLE IF EXISTS products;")
    create_table_query = """
    CREATE TABLE products (
        id SERIAL PRIMARY KEY,
        product_id TEXT,
        upc_ean TEXT,
        brand TEXT,
        product_name TEXT,
        category TEXT,
        subcategory TEXT,
        size TEXT
    );
    """
    cur.execute(create_table_query)
    conn.commit()
    cur.close()
    conn.close()

def insert_data(df):
    """Insert rows from the DataFrame into the products table."""
    conn = get_connection()
    cur = conn.cursor()
    for index, row in df.iterrows():
        cur.execute(
            """
            INSERT INTO products (product_id, upc_ean, brand, product_name, category, subcategory, size)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (row['product_id'], row['upc_ean'], row['brand'], row['product_name'],
             row['category'], row['subcategory'], row['size'])
        )
    conn.commit()
    cur.close()
    conn.close()
