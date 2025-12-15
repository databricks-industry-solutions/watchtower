"""Log Ingestion Pipeline for Databricks Watchtower.

This module implements a Delta Live Tables (DLT) pipeline for ingesting and processing
Spark driver logs from job compute clusters. The pipeline follows a medallion architecture
with bronze, silver, and reference tables.

Pipeline Components:
    - Bronze Layer: Raw log ingestion from cloud files (text format)
    - Silver Layer: Parsed and cleaned logs with structured data extraction
    - Reference Table: Compute ID to job run mapping for analysis

Tables Created:
    - bronze.logs: Raw Spark driver logs with file metadata
    - silver.logs: Parsed logs with extracted timestamps, log levels, and messages
    - silver.job_run_compute: Reference table mapping compute IDs to job runs

Data Sources:
    - Raw log files from cluster driver logs (stdout, stderr, log4j)
    - System tables (system.lakeflow.job_task_run_timeline)

Quality Expectations:
    - File size validation for bronze logs
    - Non-null message validation
    - Cluster ID and log source validation for silver logs
    - Log level parsing validation for log4j sources
    - Timestamp extraction validation

Usage:
    This pipeline is designed to run as a DLT pipeline in Databricks, automatically
    processing streaming log data and maintaining data quality through expectations.
"""

import dlt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F  # noqa: N812

spark: SparkSession = SparkSession.builder.getOrCreate()

REGEX_CLUSTER_ID = r"/([0-9]{4}-[0-9]{6}-[0-9a-z]+)/"
REGEX_LOG_SOURCE = r"^(stdout|stderr|log4j).*$"
REGEX_LOG_LEVEL = r"\b(TRACE|DEBUG|INFO|WARN|ERROR|FATAL|CRITICAL)\b"
REGEX_SUBJECTIVE_LOG_TS = r"(\d{2}[/\-]\d{2}[/\-]\d{2}\s\d{2}:\d{2}:\d{2})"


@dlt.table(
    name="bronze.logs",
    comment="Raw spark driver logs from job compute clusters.",
    table_properties={
        "Quality": "bronze",
    },
    cluster_by=["file_modification_time"],
)
@dlt.expect("valid_file_size", "file_size > 0")
@dlt.expect("non_empty_message", "msg IS NOT NULL")
def bronze_logs():  # type: ignore[no-untyped-def]
    """Raw spark driver logs from job compute clusters."""
    raw_log_location: str = (spark.conf.get("raw_log_location") or "").rstrip("/")
    if not raw_log_location:
        raise ValueError("raw_log_location must be set")

    # Autoloader retention configuration
    clean_source_action: str = (
        spark.conf.get("watchtower.autoloader.cleanSource") or "DELETE"
    )
    retention_duration: str = (
        spark.conf.get("watchtower.autoloader.retentionDuration") or "30 days"
    )
    move_destination: str = (
        spark.conf.get("watchtower.autoloader.moveDestination") or ""
    )

    options = {
        "cloudFiles.format": "text",
        "cloudFiles.cleanSource": clean_source_action,
        "cloudFiles.cleanSource.retentionDuration": retention_duration,
    }
    if clean_source_action == "MOVE":
        options["cloudFiles.cleanSource.moveDestination"] = move_destination

    return (
        spark.readStream.format("cloudFiles")
        .options(**options)
        .load(f"{raw_log_location}/*/driver/")
        .selectExpr(
            "value as msg",
            "_metadata.file_path",
            "_metadata.file_name",
            "_metadata.file_size",
            "_metadata.file_modification_time",
        )
    )


# ruff: noqa: E501
@dlt.table(
    name="silver.logs",
    table_properties={
        "Quality": "silver",
    },
    cluster_by=["cluster_id", "log_ts"],
)
@dlt.expect("non_empty_cluster_id", "cluster_id IS NOT NULL AND cluster_id != ''")
@dlt.expect("non_empty_log_source", "log_source IS NOT NULL")
@dlt.expect("parsed_log4j_log_level", "log_source != 'log4j' OR log_level != ''")
@dlt.expect("non_empty_log4j_ts", "log_source != 'log4j' OR log_ts IS NOT NULL")
def silver_logs():  # type: ignore[no-untyped-def]
    """Clean and parse logs from job compute clusters."""
    # Parsing cluster_id and log_source which are part of the structured file path.
    cluster_id = F.regexp_extract(
        F.col("file_path"),
        REGEX_CLUSTER_ID,
        1,
    ).alias("cluster_id")
    log_source = F.regexp_extract(
        F.col("file_name"),
        REGEX_LOG_SOURCE,
        1,
    ).alias("log_source")

    # When msg was parsed as structured JSON, it becomes easy and efficient to parse using VARIANT,
    # otherwise, regex can be used, which is inefficient and more prone to mistakes.
    parse_level_as_variant = F.try_variant_get(F.col("parsed_msg"), "$.level", "string")
    parse_level_as_regex = F.regexp_extract(
        F.col("msg"),
        REGEX_LOG_LEVEL,
        1,
    )

    log_level = (
        F.when(
            F.col("parsed_msg").isNotNull(),
            parse_level_as_variant,
        )
        .otherwise(
            parse_level_as_regex,
        )
        .alias("log_level")
    )

    parse_log_ts_from_json = F.ifnull(
        F.try_variant_get(F.col("parsed_msg"), "$.timestamp", "timestamp"),
        F.try_variant_get(F.col("parsed_msg"), "$.@timestamp", "timestamp"),
    )
    parse_log_ts_from_regex = F.try_to_timestamp(
        F.replace(
            F.regexp_extract(
                F.col("msg"),
                REGEX_SUBJECTIVE_LOG_TS,
                1,
            ),
            F.lit("/"),
            F.lit("-"),
        ),
        F.lit("yy-MM-dd HH:mm:ss"),
    )

    log_ts = (
        F.when(
            F.col("parsed_msg").isNotNull(),
            parse_log_ts_from_json,
        )
        .otherwise(
            parse_log_ts_from_regex,
        )
        .alias("log_ts")
    )

    log_msg = (
        F.when(
            F.col("parsed_msg").isNotNull(),
            F.try_variant_get(F.col("parsed_msg"), "$.message", "string"),
        )
        .otherwise(
            F.col("msg"),
        )
        .alias("log_msg")
    )

    return (
        dlt.read_stream("bronze.logs")
        .withColumn("parsed_msg", F.try_parse_json("msg"))
        .select(
            cluster_id,
            log_source,
            log_level,
            log_ts,
            log_msg,
            F.col("file_path"),
            F.col("file_name"),
            F.col("file_size"),
            F.col("file_modification_time"),
        )
    )


@dlt.table(
    name="silver.job_run_compute",
    table_properties={
        "Quality": "silver",
    },
    cluster_by=["compute_id", "job_id"],
    comment="""Reference table of compute IDs used in each job run.
    Compute ID may be a cluster ID or warehouse ID.""",
)
@dlt.expect("non_empty_compute_id", "compute_id IS NOT NULL AND compute_id != ''")
@dlt.expect("non_empty_job_id", "job_id IS NOT NULL")
@dlt.expect("non_empty_workspace_id", "workspace_id IS NOT NULL")
@dlt.expect("non_empty_job_run_id", "job_run_id IS NOT NULL")
def job_run_compute():  # type: ignore[no-untyped-def]
    """Create reference table of compute IDs used in each job run.

    Compute ID may be a cluster ID or warehouse ID.
    """
    lookback_interval: str = (
        spark.conf.get(
            "watchtower.job_run_compute.lookback_interval",
        )
        or "INTERVAL 7 DAYS"
    )
    lookback_expr = F.now() - F.expr(lookback_interval)
    return (
        spark.read.table("system.lakeflow.job_task_run_timeline")
        .where(F.col("period_start_time") >= lookback_expr)
        .select(
            "workspace_id",
            "job_id",
            "job_run_id",
            F.explode("compute_ids").alias("compute_id"),
        )
        .distinct()
    )
