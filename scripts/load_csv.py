import argparse

import pandas as pd
from psycopg2.extras import execute_batch

from app.db.postgres import get_connection, create_events_table, create_events_indexes


INSERT_SQL = """
    INSERT INTO events (
        event_time,
        event_type,
        product_id,
        category_id,
        category_code,
        brand,
        price,
        user_id,
        user_session
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


def _parse_row(row):
    et = row.get("event_time")
    if isinstance(et, str):
        et = pd.to_datetime(et, utc=True, errors="coerce")
    if pd.isna(et):
        et = None
    elif hasattr(et, "to_pydatetime"):
        et = et.to_pydatetime()

    def num(v, as_int):
        if pd.isna(v):
            return None
        return int(v) if as_int else float(v)

    return (
        et,
        None if pd.isna(row.get("event_type")) else str(row.get("event_type")),
        num(row.get("product_id"), True),
        num(row.get("category_id"), True),
        None if pd.isna(row.get("category_code")) else str(row.get("category_code")),
        None if pd.isna(row.get("brand")) else str(row.get("brand")),
        num(row.get("price"), False),
        num(row.get("user_id"), True),
        None if pd.isna(row.get("user_session")) else str(row.get("user_session")),
    )


def load_csv(file_path, limit, offset, chunk_size):
    print(f"Reading CSV from {file_path} (offset={offset}, limit={limit})...")

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            if offset > 0 and limit is not None:
                skip = range(1, offset + 1)
                reader = pd.read_csv(
                    file_path,
                    skiprows=skip,
                    nrows=limit,
                    chunksize=chunk_size,
                    parse_dates=["event_time"],
                    low_memory=False,
                )
            elif offset > 0:
                reader = pd.read_csv(
                    file_path,
                    skiprows=range(1, offset + 1),
                    chunksize=chunk_size,
                    parse_dates=["event_time"],
                    low_memory=False,
                )
            elif limit is not None:
                reader = pd.read_csv(
                    file_path,
                    nrows=limit,
                    chunksize=chunk_size,
                    parse_dates=["event_time"],
                    low_memory=False,
                )
            else:
                reader = pd.read_csv(
                    file_path,
                    chunksize=chunk_size,
                    parse_dates=["event_time"],
                    low_memory=False,
                )

            total = 0
            for chunk in reader:
                chunk["event_time"] = pd.to_datetime(chunk["event_time"], utc=True, errors="coerce")
                batch = [_parse_row(row) for _, row in chunk.iterrows()]
                execute_batch(cur, INSERT_SQL, batch, page_size=5000)
                total += len(batch)
                print(f"Inserted {total} rows...")

        conn.commit()
        print(f"Done. Total rows inserted: {total}")
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--chunk-size", type=int, default=50_000)

    args = parser.parse_args()

    create_events_table()
    create_events_indexes()

    load_csv(args.file, args.limit, args.offset, args.chunk_size)


if __name__ == "__main__":
    main()
