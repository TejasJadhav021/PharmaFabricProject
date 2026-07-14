# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from delta.tables import DeltaTable

import json
import datetime
import os


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================================================
# NOTEBOOK PARAMETERS
# These values can be injected by the Fabric Pipeline
# (Base Parameters on the "Run Notebook" activity).
# If not supplied, the defaults below are used, which
# preserves today's behaviour for local / interactive runs.
# ==========================================================

try:
    ENVIRONMENT
except NameError:
    ENVIRONMENT = "DEV"

try:
    PROJECT_NAME
except NameError:
    PROJECT_NAME = "PharmaFlow360"

try:
    LAKEHOUSE_NAME
except NameError:
    LAKEHOUSE_NAME = "LH_PHARMA"

try:
    METADATA_TABLE
except NameError:
    METADATA_TABLE = "CTL.CTL_INTEGRATION_METADATA"

try:
    AUDIT_TABLE
except NameError:
    AUDIT_TABLE = "CTL.CTL_PIPELINE_AUDIT"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================================================
# PATH / NAMING CONSTANTS
# Centralised here so NB_BRONZE_TO_SILVER and NB_SILVER_TO_GLD
# never hardcode these literals directly.
# ==========================================================

try:
    FILES_ROOT_PATH
except NameError:
    FILES_ROOT_PATH = "Files"

try:
    BRONZE_PATH
except NameError:
    BRONZE_PATH = "Files/Bronze"

try:
    SILVER_PATH
except NameError:
    SILVER_PATH = "Tables"

try:
    GOLD_PATH
except NameError:
    GOLD_PATH = "Tables"

try:
    BRONZE_FOLDER_NAME
except NameError:
    BRONZE_FOLDER_NAME = "Bronze"

try:
    SILVER_FOLDER_NAME
except NameError:
    SILVER_FOLDER_NAME = "Silver"

try:
    GOLD_SCHEMA_NAME
except NameError:
    GOLD_SCHEMA_NAME = "dbo"

try:
    INTEGRATION_ID_COLUMN
except NameError:
    INTEGRATION_ID_COLUMN = "INTEGRATION_ID"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================================================
# SCD TYPE 2 SETTINGS
# GOLD tables listed here are loaded with full change-history
# tracking (see merge_scd2 in NB_COMMON_UTILS) instead of the
# standard upsert-only merge.
# ==========================================================

try:
    SCD2_DIM_TABLES
except NameError:
    # Gold (dimension) tables that require SCD Type 2 history.
    # Add more GOLD_TABLE names here as needed - no notebook
    # code change required.
    SCD2_DIM_TABLES = ["DIM_PRODUCT"]

try:
    SCD2_EFFECTIVE_START_COLUMN
except NameError:
    SCD2_EFFECTIVE_START_COLUMN = "EFF_START_DATE"

try:
    SCD2_EFFECTIVE_END_COLUMN
except NameError:
    SCD2_EFFECTIVE_END_COLUMN = "EFF_END_DATE"

try:
    SCD2_CURRENT_FLAG_COLUMN
except NameError:
    SCD2_CURRENT_FLAG_COLUMN = "IS_CURRENT"

try:
    SCD2_EXCLUDED_COLUMNS
except NameError:
    # Technical / audit columns that should NOT trigger a new
    # SCD2 version just because they change on every run.
    SCD2_EXCLUDED_COLUMNS = [
        "LOAD_TIMESTAMP",
        "CREATED_DT",
        "UPDATED_DT",
        "PIPELINE_RUN_ID",
        "RECORD_STATUS"
    ]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================================================
# DELTA MAINTENANCE / PARTITIONING SETTINGS
# ==========================================================

try:
    OPTIMIZE_WRITE_ENABLED
except NameError:
    # Fabric auto-compacts small files into right-sized ones as
    # part of the write itself (reduces need for manual OPTIMIZE).
    OPTIMIZE_WRITE_ENABLED = "true"

try:
    AUTO_COMPACT_ENABLED
except NameError:
    # Runs a lightweight compaction pass immediately after a write
    # if many small files were produced.
    AUTO_COMPACT_ENABLED = "true"

try:
    VACUUM_RETENTION_HOURS
except NameError:
    # 168h = 7 days = the Delta safe minimum. Do not go lower
    # without understanding time-travel / concurrent-reader risk.
    VACUUM_RETENTION_HOURS = 168

try:
    GOLD_PARTITION_COLUMNS
except NameError:
    # GOLD_TABLE name -> partition column list.
    # Only large, date-driven fact tables should be partitioned;
    # dimension tables (incl. SCD2 dims) generally should NOT be
    # partitioned - low row counts mean partitioning just creates
    # excess small files.
    GOLD_PARTITION_COLUMNS = {
        "FACT_SALES": ["SALE_YEAR", "SALE_MONTH"],
        "FACT_PRESCRIPTIONS": ["PRESCRIPTION_YEAR", "PRESCRIPTION_MONTH"]
    }


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================================================
# SPARK CONFIGURATION
# ==========================================================

try:
    SHUFFLE_PARTITIONS
except NameError:
    SHUFFLE_PARTITIONS = "8"

try:
    AUTO_MERGE_SCHEMA
except NameError:
    AUTO_MERGE_SCHEMA = "true"

try:
    REJECT_THRESHOLD_PCT
except NameError:
    REJECT_THRESHOLD_PCT = 5.0

spark.conf.set(
    "spark.databricks.delta.schema.autoMerge.enabled",
    AUTO_MERGE_SCHEMA
)

spark.conf.set(
    "spark.sql.shuffle.partitions",
    SHUFFLE_PARTITIONS
)

spark.conf.set(
    "spark.databricks.delta.optimizeWrite.enabled",
    OPTIMIZE_WRITE_ENABLED
)

spark.conf.set(
    "spark.databricks.delta.autoCompact.enabled",
    AUTO_COMPACT_ENABLED
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("===================================")
print(f" {PROJECT_NAME} Configuration ")
print("===================================")

print(f"PROJECT_NAME      : {PROJECT_NAME}")
print(f"ENVIRONMENT       : {ENVIRONMENT}")
print(f"LAKEHOUSE_NAME    : {LAKEHOUSE_NAME}")
print(f"METADATA_TABLE    : {METADATA_TABLE}")
print(f"AUDIT_TABLE       : {AUDIT_TABLE}")
print(f"FILES_ROOT_PATH   : {FILES_ROOT_PATH}")
print(f"GOLD_SCHEMA_NAME  : {GOLD_SCHEMA_NAME}")
print(f"SHUFFLE_PARTITIONS: {SHUFFLE_PARTITIONS}")
print(f"OPTIMIZE_WRITE    : {OPTIMIZE_WRITE_ENABLED}")
print(f"AUTO_COMPACT      : {AUTO_COMPACT_ENABLED}")
print(f"VACUUM_RETENTION  : {VACUUM_RETENTION_HOURS}h")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
