from clickhouse_connect import get_client
from dotenv import load_dotenv
import os

load_dotenv()


def main():
    client = get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "clickstream_user"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "clickstream_pass"),
        database=os.getenv("CLICKHOUSE_DB", "clickstream"),
    )

    result = client.query("SELECT 1")
    print("Connected to ClickHouse")
    print(result.result_rows)


if __name__ == "__main__":
    main()