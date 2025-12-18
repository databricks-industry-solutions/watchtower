# Databricks notebook source

"""Setup test data for watchtower integration tests."""

import os
import shutil

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
w = WorkspaceClient()
dbutils = w.dbutils

# COMMAND ----------

dbutils.widgets.text("source_test_dir", "")
dbutils.widgets.text("target_dir", "")

source_test_dir = dbutils.widgets.get("source_test_dir")
target_dir = dbutils.widgets.get("target_dir")

CATALOG = "watchtower_test"
SCHEMA = "integration"
VOLUME_NAME = "test_data"

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS `{CATALOG}`")
spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.`{VOLUME_NAME}`")

# COMMAND ----------

if not os.path.exists(target_dir):
    os.makedirs(target_dir)

for root, _, files in os.walk(source_test_dir):
    rel_path = os.path.relpath(root, source_test_dir)
    dest_dir = os.path.join(target_dir, rel_path) if rel_path != "." else target_dir
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)
    for file in files:
        src_file = os.path.join(root, file)
        dest_file = os.path.join(dest_dir, file)
        shutil.copy2(src_file, dest_file)
