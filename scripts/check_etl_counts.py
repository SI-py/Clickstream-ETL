from app.config.settings import get_config
from app.db.clickhouse import get_clickhouse_client
from app.db.postgres import get_connection


def main():
    cfg = get_config()
    pg = cfg["postgres"]
    source = pg["source_table"]
    if "." in source:
        events_table = source
    else:
        events_table = f"public.{source}"

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {events_table};")
            n_pg = cur.fetchone()[0]
    finally:
        conn.close()

    ch = cfg["clickhouse"]
    target = ch.get("target_table", "silver_events")
    if "." in target:
        ch_db, ch_tbl = target.split(".", 1)
    else:
        ch_db = ch.get("database", "clickstream")
        ch_tbl = target
    client = get_clickhouse_client()
    try:
        r = client.query(f"SELECT count() FROM `{ch_db}`.`{ch_tbl}`")
        n_ch = r.result_rows[0][0]
    except Exception:
        n_ch = "n/a (table missing?)"

    print(f"Postgres {events_table}: {n_pg} rows")
    print(f"ClickHouse {ch_db}.{ch_tbl}: {n_ch} rows")


if __name__ == "__main__":
    main()
