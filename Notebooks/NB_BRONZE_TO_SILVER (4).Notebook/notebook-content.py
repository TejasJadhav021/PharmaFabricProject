# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

%run NB_COMMON_CONFIG


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run NB_COMMON_UTILS


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#===========================================================
# NB_BRONZE_TO_SILVER
# Generic Bronze → Silver Processing Notebook
#===========================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from delta.tables import DeltaTable

from datetime import datetime

import traceback

spark = SparkSession.builder.getOrCreate()

start_time = datetime.now()

print("="*70)
print("NB_BRONZE_TO_SILVER")
print("="*70)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# RECEIVE PARAMETERS
# ============================================================

try:
    # Values passed from Fabric Pipeline
    integration_id = int(INTEGRATION_ID)
    pipeline_run_id = PIPELINE_RUN_ID

except NameError:
    # Local notebook execution
    integration_id = 1
    pipeline_run_id = "LOCAL_TEST"

print(f"Integration ID : {integration_id}")
print(f"Pipeline Run ID : {pipeline_run_id}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

metadata_df = (
        spark.table(METADATA_TABLE)
            .filter(col("integration_id") == int(integration_id))
    )

if metadata_df.count() == 0:
        raise Exception(f"Metadata not found for INTEGRATION_ID = {integration_id}")

metadata = metadata_df.first()

print("Metadata Loaded Successfully")
metadata_df.show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# EXTRACT METADATA
# ============================================================

SOURCE_SYSTEM = metadata["SOURCE_SYSTEM"]
SOURCE_OBJECT = metadata["SOURCE_OBJECT"]
SOURCE_QUERY = metadata["SOURCE_QUERY"]
SOURCE_PATH = metadata["SOURCE_PATH"]

TARGET_LAYER = metadata["TARGET_LAYER"]
TARGET_FOLDER = metadata["TARGET_FOLDER"]
TARGET_FILE_NAME = metadata["TARGET_FILE_NAME"]
TARGET_TABLE = metadata["TARGET_TABLE"]

FILE_FORMAT = metadata["FILE_FORMAT"]

LOAD_TYPE = metadata["LOAD_TYPE"]
CDC_FLAG = metadata["CDC_FLAG"]

PRIMARY_KEY = metadata["PRIMARY_KEY_COLUMN"]
WATERMARK_COLUMN = metadata["WATERMARK_COLUMN"]

LAST_EXECUTION_DATE = metadata["LAST_EXECUTION_DATE"]
SESSION_CDC_EXTRACT_DT = metadata["SESSION_CDC_EXTRACT_DT"]

TARGET_PATH = metadata["TARGET_PATH"]

print("="*60)
print("Metadata Variables")
print("="*60)

print(f"SOURCE_SYSTEM           : {SOURCE_SYSTEM}")
print(f"SOURCE_OBJECT           : {SOURCE_OBJECT}")
print(f"TARGET_TABLE            : {TARGET_TABLE}")
print(f"LOAD_TYPE               : {LOAD_TYPE}")
print(f"CDC_FLAG                : {CDC_FLAG}")
print(f"PRIMARY_KEY             : {PRIMARY_KEY}")
print(f"WATERMARK_COLUMN        : {WATERMARK_COLUMN}")
print(f"SESSION_CDC_EXTRACT_DT  : {SESSION_CDC_EXTRACT_DT}")
print(f"TARGET_PATH             : {TARGET_PATH}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# MAIN PROCESSING
# ============================================================

from pyspark.sql.functions import concat_ws, lit, current_timestamp, when, trim, col

rows_read = None
rows_written = None
rows_rejected = None

try:

    # ---- BUILD BRONZE PATH ----

    today = datetime.now()

    bronze_path = (
        f"{FILES_ROOT_PATH}/{TARGET_PATH}/"
        f"{today.strftime('%Y')}/"
        f"{today.strftime('%m')}/"
        f"{today.strftime('%d')}"
    )

    print(f"Bronze Path : {bronze_path}")

    # ---- READ BRONZE ----

    df = (
        spark.read
             .format("parquet")
             .load(bronze_path)
    )

    rows_read = df.count()

    print(f"Rows Read : {rows_read}")

    # ---- GENERIC DATA CLEANSING ----

    print("="*70)
    print("GENERIC DATA CLEANSING")
    print("="*70)

    df = df.dropDuplicates()

    if PRIMARY_KEY is not None and PRIMARY_KEY != "":
        df = remove_duplicate_records(df, PRIMARY_KEY)

    df = trim_string_columns(df)

    for c in df.columns:
        if dict(df.dtypes)[c] == "string":
            df = df.withColumn(
                c,
                when(trim(col(c)) == "", None).otherwise(col(c))
            )

    print(f"Rows After Cleaning : {df.count()}")

    # ---- DATA QUALITY : QUARANTINE BAD RECORDS ----

    print("="*70)
    print("DATA QUALITY")
    print("="*70)

    if PRIMARY_KEY is not None and PRIMARY_KEY != "":

        good_df, bad_df = split_good_bad_records(df, PRIMARY_KEY)

        rows_rejected = bad_df.count()

        check_reject_percentage(
            total_rows=rows_read,
            rejected_rows=rows_rejected,
            threshold=REJECT_THRESHOLD_PCT
        )

        if rows_rejected > 0:

            rejected_path = (
                f"{FILES_ROOT_PATH}/Rejected/{TARGET_PATH}/"
                f"{today.strftime('%Y')}/{today.strftime('%m')}/{today.strftime('%d')}"
            )

            (
                bad_df.write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .save(rejected_path)
            )

            print(f"Quarantined {rows_rejected} bad record(s) -> {rejected_path}")

        df = good_df

    else:

        rows_rejected = 0

    # ---- CREATE INTEGRATION ID ----

    primary_key = PRIMARY_KEY
    source_system = SOURCE_SYSTEM

    df = df.withColumn(
        INTEGRATION_ID_COLUMN,
        concat_ws(
            "~",
            lit(source_system),
            col(primary_key).cast("string")
        )
    )

    # ---- ADD AUDIT COLUMNS ----

    df = (
        df
        .withColumn("SOURCE_SYSTEM", lit(source_system))
        .withColumn("PIPELINE_RUN_ID", lit(pipeline_run_id))
        .withColumn("LOAD_TYPE", lit(LOAD_TYPE))
        .withColumn("LOAD_TIMESTAMP", current_timestamp())
        .withColumn("CREATED_DT", current_timestamp())
        .withColumn("UPDATED_DT", current_timestamp())
        .withColumn("RECORD_STATUS", lit("ACTIVE"))
    )

    # ---- BUILD SILVER PATH ----

    silver_path = TARGET_PATH.replace(BRONZE_FOLDER_NAME, SILVER_FOLDER_NAME)

    silver_full_path = f"{FILES_ROOT_PATH}/{silver_path}"

    print(f"Silver Path : {silver_full_path}")

    # ---- WRITE TO SILVER ----

    print("="*70)
    print("WRITING TO SILVER")
    print("="*70)

    load_type = "FULL" if CDC_FLAG == "N" else "INCREMENTAL"

    merge_delta(
        source_df=df,
        target_path=silver_full_path,
        primary_key=PRIMARY_KEY,
        load_type=load_type
    )

    rows_written = df.count()

    print(f"Silver Load Completed. Rows Written : {rows_written}")

    # ---- DELTA MAINTENANCE : COMPACT FILES + ZORDER BY PRIMARY KEY ----
    # Cheap to run every time thanks to OPTIMIZE_WRITE_ENABLED /
    # AUTO_COMPACT_ENABLED already reducing small files on write;
    # this catches whatever's left, especially after many small
    # incremental merges.

    optimize_table(
        table_name=f"delta.`{silver_full_path}`",
        zorder_columns=[PRIMARY_KEY]
    )

    # ---- AUDIT LOG : SUCCESS ----

    end_time = datetime.now()

    write_audit_log(
        audit_table=AUDIT_TABLE,
        pipeline_run_id=pipeline_run_id,
        integration_id=integration_id,
        notebook_name="NB_BRONZE_TO_SILVER",
        status="SUCCESS",
        start_time=start_time,
        end_time=end_time,
        rows_read=rows_read,
        rows_written=rows_written,
        rows_rejected=rows_rejected
    )

    print("="*70)
    print("LOAD SUMMARY")
    print("="*70)

    print(f"Rows Read     : {rows_read}")
    print(f"Rows Written  : {rows_written}")
    print(f"Rows Rejected : {rows_rejected}")
    print(f"Duration      : {(end_time - start_time).total_seconds():.1f}s")

except Exception as e:

    end_time = datetime.now()

    write_audit_log(
        audit_table=AUDIT_TABLE,
        pipeline_run_id=pipeline_run_id,
        integration_id=integration_id,
        notebook_name="NB_BRONZE_TO_SILVER",
        status="FAILED",
        start_time=start_time,
        end_time=end_time,
        rows_read=rows_read,
        rows_written=rows_written,
        rows_rejected=rows_rejected,
        error_message=str(e)
    )

    print("="*70)
    print("NB_BRONZE_TO_SILVER FAILED")
    print("="*70)

    print(traceback.format_exc())

    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from notebookutils import mssparkutils

mssparkutils.notebook.exit(
    f"""
    {{
        "STATUS":"SUCCESS",
        "ROWS_READ":{rows_read},
        "ROWS_WRITTEN":{rows_written},
        "ROWS_REJECTED":{rows_rejected}
    }}
    """
)


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
