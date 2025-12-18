# Databricks notebook source

"""Demo notebook showcasing some of watchtower features.

This demo notebook is executed using Databricks Workflows
as defined in resources/watchtower.demo.job.yml.
"""

# COMMAND ----------

import json
import logging

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQueryListener  # type: ignore[attr-defined]
from pyspark.sql.streaming.listener import (
    QueryProgressEvent,
    QueryStartedEvent,
    QueryTerminatedEvent,
)

# Set up structured logging


class JSONFormatter(logging.Formatter):
    """Structured JSON formatter for logging."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log records as JSON."""
        log_record = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
            "line": record.lineno,
        }
        return json.dumps(log_record)


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create a stream (console) handler
# and set the formatter for the handler
handler = logging.StreamHandler()
formatter = JSONFormatter(datefmt="%Y-%m-%dT%H:%M:%S")  # ISO-8601 format
handler.setFormatter(formatter)
logger.addHandler(handler)

# COMMAND ----------

# Use the logger object to log messages instead of print()
logger.debug("This is a debug message")
logger.info("This is an info message")
logger.warning("This is a warning message")

# Handle exceptions with logging.
try:
    raise RuntimeError("This is a runtime error")
except RuntimeError:
    logger.error("This is an error message", exc_info=True)

# COMMAND ----------

# Intentionally setting a small executor memory
# to demonstrate spill to disk.
# NOTE: This is for demo purposes only and should NOT be used in production.
spark = SparkSession.builder.config("spark.executor.memory", "512m").getOrCreate()

# NOTE: It is a best practice to remove .show() and display()
#        from code before it is deployed to production.
spark.range(10).show()

# COMMAND ----------

# NOTE: The following is only to demo a bad practice that should not be done in
#       production; we should not log PII or sensitive data.
#       We include this here to demonstrate what NOT to do, and how watchtower
#       can help us detect this once the logs are ingested.
logger.info("user email: john.doe@example.com")
logger.info("phone: 5015551234")

# COMMAND ----------

w = WorkspaceClient()

notebook_path = (
    w.dbutils.notebook.entry_point.getDbutils()  # type: ignore[union-attr]
    .notebook()
    .getContext()
    .notebookPath()
    .get()
)

logger.info("Notebook path: %s", notebook_path)

# COMMAND ----------


# ruff: noqa: N802
class SparkStreamingLogger(StreamingQueryListener):
    """Logging utility for spark structured streaming queries."""

    def onQueryStarted(self, event: QueryStartedEvent) -> None:
        """Log the start of a streaming query."""
        logger.info("Query started: %s (%s)", event.name, event.id)

    def onQueryProgress(self, event: QueryProgressEvent) -> None:
        """Log progression events of a streaming query."""
        logger.info("Query progress: %s", event.progress.json)

    def onQueryTerminated(self, event: QueryTerminatedEvent) -> None:
        """Log the termination of a streaming query."""
        if event.exception:
            logger.error("Query terminated: %s (%s)", event.id, event.exception)
        else:
            logger.info("Query terminated: %s", event.id)


# COMMAND ----------

# Register the logging listener
spark.streams.addListener(SparkStreamingLogger())
