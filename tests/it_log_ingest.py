"""Integration tests for the Watchtower Ingestion Pipeline."""

import dlt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F  # noqa: N812

spark: SparkSession = SparkSession.builder.getOrCreate()


@dlt.table(
    name="integration.test_bronze_logs",
    comment="TEST: bronze table have file metadata",
    temporary=True,
    table_properties={
        "SubType": "true",
    },
)
@dlt.expect_or_fail(
    "file_path_non_empty",
    "file_path IS NOT NULL",
)
@dlt.expect_or_fail(
    "file_name_non_empty",
    "file_name IS NOT NULL",
)
@dlt.expect_or_fail(
    "file_size_non_empty",
    "file_size IS NOT NULL",
)
@dlt.expect_or_fail(
    "file_modification_time_non_empty",
    "file_modification_time IS NOT NULL",
)
def test_bronze_logs():  # type: ignore[no-untyped-def]
    """TEST: bronze table have file metadata."""
    return spark.read.table("bronze.logs")


@dlt.table(
    name="integration.test_silver_logs",
    comment="TEST: silver table parses log lines",
    temporary=True,
    table_properties={
        "SubType": "true",
    },
)
@dlt.expect_or_fail(
    "keep_all_rows",
    "num_rows = 25",
)
@dlt.expect_or_fail(
    "invalid_file_names_should_be_dropped",
    "invalid_file_name = 0",
)
@dlt.expect_or_fail(
    "keep_unparseable_timestamps",
    "null_log_timestamps = 19",
)
def test_silver_logs():  # type: ignore[no-untyped-def]
    """TEST: silver table parses log lines."""
    logs = spark.read.table("silver.logs")
    return logs.agg(
        F.count("*").alias("num_rows"),
        F.sum(
            F.when(~F.col("file_name").isin("stdout", "stderr", "log4j"), 1).otherwise(
                0
            )
        ).alias("invalid_file_name"),
        F.sum(F.when(F.col("log_ts").isNull(), 1).otherwise(0)).alias(
            "null_log_timestamps"
        ),
    )
