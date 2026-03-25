from pyspark.sql.functions import coalesce, col, dayofweek, hour, lit, lower, split, to_date


def basic_clean(df):
    return (
        df.filter(col("product_id").isNotNull())
        .filter(lower(col("event_type")).isin("view", "cart", "remove_from_cart", "purchase"))
    )


def filter_heavy_sessions(df, max_events_per_session):
    from pyspark.sql.functions import count as spark_count

    df2 = df.withColumn("_sk", coalesce(col("user_session"), lit("")))
    counts = df2.groupBy("_sk").agg(spark_count("*").alias("_n"))
    ok = counts.filter(col("_n") <= max_events_per_session).select("_sk")
    return df2.join(ok, "_sk", "inner").drop("_sk")


def parse_category(df):
    return (
        df.withColumn("top_category", split(col("category_code"), "\\.").getItem(0))
        .withColumn("sub_category", split(col("category_code"), "\\.").getItem(1))
    )


def add_time_features(df):
    return (
        df.withColumn("event_date", to_date(col("event_time")))
        .withColumn("event_hour", hour(col("event_time")))
        .withColumn("day_of_week", dayofweek(col("event_time")))
    )


def fill_clickhouse_defaults(df):
    """ClickHouse table uses non-nullable types; JDBC driver rejects Spark nulls."""
    out = df
    for c in ("category_code", "top_category", "sub_category", "brand", "user_session"):
        out = out.withColumn(c, coalesce(col(c), lit("")))
    return (
        out.withColumn("category_id", coalesce(col("category_id"), lit(0)))
        .withColumn("user_id", coalesce(col("user_id"), lit(0)))
        .withColumn("price", coalesce(col("price"), lit(0.0)))
    )


def apply_all(df, max_events_per_session=5000):
    df = basic_clean(df)
    df = filter_heavy_sessions(df, max_events_per_session)
    df = parse_category(df)
    df = add_time_features(df)
    df = fill_clickhouse_defaults(df)
    return df


FINAL_COLUMN_ORDER = [
    "event_time",
    "event_type",
    "product_id",
    "category_id",
    "category_code",
    "top_category",
    "sub_category",
    "brand",
    "price",
    "user_id",
    "user_session",
    "event_date",
    "event_hour",
    "day_of_week",
]
