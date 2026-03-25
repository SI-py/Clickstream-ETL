import os

from clickhouse_connect import get_client
from dotenv import load_dotenv

load_dotenv()


def _client_kwargs():
    return {
        "host": os.getenv("CLICKHOUSE_HOST", "localhost"),
        "port": int(os.getenv("CLICKHOUSE_PORT", "8123")),
        "username": os.getenv("CLICKHOUSE_USER", "clickstream_user"),
        "password": os.getenv("CLICKHOUSE_PASSWORD", "clickstream_pass"),
    }


def get_clickhouse_client():
    db = os.getenv("CLICKHOUSE_DB", "clickstream")
    return get_client(database=db, **_client_kwargs())


def create_database():
    db = os.getenv("CLICKHOUSE_DB", "clickstream")
    client = get_client(**_client_kwargs())
    client.command(f"CREATE DATABASE IF NOT EXISTS `{db}`")


def create_silver_events_table():
    client = get_clickhouse_client()
    client.command("""
        CREATE TABLE IF NOT EXISTS clickstream.silver_events (
            event_time DateTime,
            event_type String,
            product_id UInt64,
            category_id UInt64,
            category_code String,
            top_category String,
            sub_category String,
            brand String,
            price Float64,
            user_id UInt64,
            user_session String,
            event_date Date,
            event_hour UInt8,
            day_of_week UInt8
        )
        ENGINE = MergeTree
        ORDER BY (event_time, product_id)
    """)
