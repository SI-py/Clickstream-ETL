import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()


class PostgresConfig:
    def __init__(self, host, port, db, user, password):
        self.host = host
        self.port = port
        self.db = db
        self.user = user
        self.password = password


def get_postgres_config():
    return PostgresConfig(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        db=os.getenv("POSTGRES_DB", "clickstream"),
        user=os.getenv("POSTGRES_USER", "clickstream_user"),
        password=os.getenv("POSTGRES_PASSWORD", "clickstream_pass"),
    )


def get_connection():
    cfg = get_postgres_config()
    return psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.db,
        user=cfg.user,
        password=cfg.password,
    )


def create_events_table():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    event_time TIMESTAMP,
                    event_type TEXT,
                    product_id BIGINT,
                    category_id BIGINT,
                    category_code TEXT,
                    brand TEXT,
                    price FLOAT,
                    user_id BIGINT,
                    user_session TEXT
                );
            """)
            conn.commit()
    finally:
        conn.close()


def create_events_indexes():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_events_event_time
                ON events (event_time);
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_events_product_id
                ON events (product_id);
            """)

        conn.commit()
    finally:
        conn.close()
