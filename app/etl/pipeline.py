import logging
import os
import time
from pathlib import Path

from onetl.connection import Clickhouse, Postgres
from onetl.db import DBReader, DBWriter
from onetl.hwm.store import YAMLHWMStore
from onetl.strategy import IncrementalStrategy
from pyspark.sql import SparkSession

from app.config.settings import get_config
from app.db.clickhouse import create_database, create_silver_events_table
from app.etl.transforms import FINAL_COLUMN_ORDER, apply_all

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

log = logging.getLogger(__name__)

EVENT_COLUMNS = [
    "event_time",
    "event_type",
    "product_id",
    "category_id",
    "category_code",
    "brand",
    "price",
    "user_id",
    "user_session",
]


def _spark(cfg):
    packages = Postgres.get_packages() + Clickhouse.get_packages()
    return (
        SparkSession.builder.appName(cfg["spark"]["app_name"])
        .master(cfg["spark"]["master"])
        .config("spark.jars.packages", ",".join(packages))
        .getOrCreate()
    )


def _pg_credentials(cfg):
    pg = cfg["postgres"]
    user = pg.get("user") or os.environ.get("POSTGRES_USER")
    password = pg.get("password") or os.environ.get("POSTGRES_PASSWORD")
    if not user or not password:
        raise ValueError("Set POSTGRES_USER and POSTGRES_PASSWORD in the environment.")
    return user, password


def _ch_credentials(cfg):
    ch = cfg["clickhouse"]
    user = ch.get("user") or os.environ.get("CLICKHOUSE_USER")
    password = ch.get("password") or os.environ.get("CLICKHOUSE_PASSWORD")
    if not user or not password:
        raise ValueError("Set CLICKHOUSE_USER and CLICKHOUSE_PASSWORD in the environment.")
    return user, password


def _clear_hwm(cfg):
    hwm_cfg = cfg["hwm"]
    store = YAMLHWMStore(path=Path(hwm_cfg["store_path"]).resolve())
    path = store.get_file_path(hwm_cfg["name"])
    if path.exists():
        path.unlink()
        log.info("Removed HWM file %s for full reload", path)


def _run_etl(cfg, *, write_mode):
    t0 = time.perf_counter()
    log.info("ETL started (mode=%s)", write_mode)
    spark = _spark(cfg)
    spark.sparkContext.setLogLevel("WARN")

    pg_user, pg_password = _pg_credentials(cfg)
    ch_user, ch_password = _ch_credentials(cfg)

    pg = cfg["postgres"]
    ch = cfg["clickhouse"]
    hwm = cfg["hwm"]
    max_sess = int(cfg.get("etl", {}).get("max_events_per_session", 5000))
    log.info(
        "Connections config loaded (postgres=%s:%s, clickhouse=%s:%s, source=%s, target=%s)",
        pg["host"],
        pg["port"],
        ch["host"],
        ch["port"],
        pg["source_table"],
        ch["target_table"],
    )

    postgres = Postgres(
        host=pg["host"],
        port=int(pg["port"]),
        user=pg_user,
        password=pg_password,
        database=pg["database"],
        spark=spark,
    ).check()

    clickhouse = Clickhouse(
        host=ch["host"],
        port=int(ch["port"]),
        user=ch_user,
        password=ch_password,
        database=ch.get("database"),
        spark=spark,
    ).check()

    reader = DBReader(
        connection=postgres,
        source=pg["source_table"],
        columns=EVENT_COLUMNS,
        hwm=DBReader.AutoDetectHWM(
            name=hwm["name"],
            expression=hwm["expression"],
        ),
    )

    writer = DBWriter(
        connection=clickhouse,
        target=ch["target_table"],
        options=Clickhouse.WriteOptions(
            if_exists=write_mode,
            createTableOptions="ENGINE = MergeTree ORDER BY (event_time, product_id)",
        ),
    )

    rows_raw = 0
    rows_final = 0

    try:
        with YAMLHWMStore(path=Path(hwm["store_path"]).resolve()):
            with IncrementalStrategy():
                log.info("Reading source data via DBReader")
                df = reader.run()
                if not df.take(1):
                    log.warning("No rows in this batch (nothing to load)")
                    elapsed = time.perf_counter() - t0
                    return {
                        "rows_read": 0,
                        "rows_written": 0,
                        "seconds": round(elapsed, 3),
                        "write_mode": write_mode,
                    }

                rows_raw = df.count()
                log.info("Source rows read: %s", rows_raw)
                transformed = apply_all(df, max_events_per_session=max_sess)
                final_df = transformed.select(*FINAL_COLUMN_ORDER)
                rows_final = final_df.count()
                log.info(
                    "Transform complete: rows_before=%s rows_after=%s (write_mode=%s)",
                    rows_raw,
                    rows_final,
                    write_mode,
                )
                log.info("Writing transformed batch to ClickHouse")
                writer.run(final_df)
                log.info("Write complete: rows_written=%s", rows_final)
                log.info("HWM will be updated by IncrementalStrategy on successful context exit")
    finally:
        spark.stop()

    elapsed = time.perf_counter() - t0
    log.info("ETL finished in %.3f seconds", elapsed)
    return {
        "rows_read": rows_raw,
        "rows_written": rows_final,
        "seconds": round(elapsed, 3),
        "write_mode": write_mode,
    }


def run_full_etl():
    cfg = get_config()
    create_database()
    create_silver_events_table()
    _clear_hwm(cfg)
    log.info("Starting full snapshot (replace target table)")
    return _run_etl(cfg, write_mode="replace_entire_table")


def run_incremental_etl():
    cfg = get_config()
    create_database()
    create_silver_events_table()
    log.info("Starting incremental load (append to target table)")
    return _run_etl(cfg, write_mode="append")
