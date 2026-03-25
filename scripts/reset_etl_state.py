from pathlib import Path

from app.config.settings import get_config
from app.db.clickhouse import get_clickhouse_client
from app.db.postgres import get_connection


def main():
    cfg = get_config()
    pg = cfg["postgres"]
    ch = cfg["clickhouse"]
    source = pg["source_table"]
    if "." in source:
        events_table = source
    else:
        events_table = f"public.{source}"

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {events_table};")
        conn.commit()
    finally:
        conn.close()

    client = get_clickhouse_client()
    target = ch.get("target_table", "silver_events")
    if "." in target:
        ch_db, ch_tbl = target.split(".", 1)
    else:
        ch_db = ch.get("database", "clickstream")
        ch_tbl = target
    client.command(f"TRUNCATE TABLE IF EXISTS `{ch_db}`.`{ch_tbl}`")

    hwm_dir = Path(cfg["hwm"]["store_path"]).resolve()
    if hwm_dir.exists():
        for f in hwm_dir.glob("*.yml"):
            f.unlink()

    print("Reset OK: Postgres events truncated, ClickHouse silver truncated, HWM YAML removed.")


if __name__ == "__main__":
    main()
