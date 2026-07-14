# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7f3feb17-1239-4f7a-b910-f14143f53674",
# META       "default_lakehouse_name": "LH_PHARMA",
# META       "default_lakehouse_workspace_id": "16650313-386a-4ca3-8c9e-ff3ca2b04f6d",
# META       "known_lakehouses": [
# META         {
# META           "id": "7f3feb17-1239-4f7a-b910-f14143f53674"
# META         }
# META       ]
# META     }
# META   }
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
    metadata_id = int(METADATA_ID)
    pipeline_run_id = PIPELINE_RUN_ID

except NameError:
    # Local notebook execution
    metadata_id = 1
    pipeline_run_id = "LOCAL_TEST"

print(f"Integration ID : {metadata_id}")
print(f"Pipeline Run ID : {pipeline_run_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

metadata_df = (
        spark.table("CTL.CTL_INTEGRATION_METADATA")
            .filter(col("metadata_id") == int(metadata_id))
    )

if metadata_df.count() == 0:
        raise Exception(f"Metadata not found for INTEGRATION_ID = {metadata_id}")

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

from datetime import datetime

today = datetime.now()

bronze_path = (
    f"Files/{TARGET_PATH}/"
    f"{today.strftime('%Y')}/"
    f"{today.strftime('%m')}/"
    f"{today.strftime('%d')}"
)

print(bronze_path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = (
    spark.read
         .format("parquet")
         .load(bronze_path)
)

print(f"Rows : {df.count()}")

df.printSchema()

df.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# GENERIC DATA CLEANSING
# ============================================================

from pyspark.sql.functions import *

print("="*70)
print("GENERIC DATA CLEANSING")
print("="*70)

initial_rows = df.count()

df = df.dropDuplicates()

if PRIMARY_KEY is not None and PRIMARY_KEY != "":
    df = df.dropDuplicates([PRIMARY_KEY])

for c in df.columns:
    if dict(df.dtypes)[c] == "string":
        df = df.withColumn(c, trim(col(c)))

for c in df.columns:
    if dict(df.dtypes)[c] == "string":
        df = df.withColumn(
            c,
            when(trim(col(c)) == "", None).otherwise(col(c))
        )

final_rows = df.count()

print(f"Rows Before Cleaning : {initial_rows}")
print(f"Rows After Cleaning  : {final_rows}")

df.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CREATE INTEGRATION ID
# ============================================================

from pyspark.sql.functions import concat_ws, lit, col

primary_key = metadata["PRIMARY_KEY_COLUMN"]
source_system = metadata["SOURCE_SYSTEM"]

df = df.withColumn(
    "INTEGRATION_ID",
    concat_ws(
        "~",
        lit(source_system),
        col(primary_key).cast("string")
    )
)

print("Integration ID Created")
df.select("INTEGRATION_ID", primary_key).show(5, False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# ADD AUDIT COLUMNS
# ============================================================

from pyspark.sql.functions import current_timestamp, lit

df = (
    df
    .withColumn("SOURCE_SYSTEM", lit(source_system))
    .withColumn("PIPELINE_RUN_ID", lit(pipeline_run_id))
    .withColumn("LOAD_TYPE", lit(metadata["LOAD_TYPE"]))
    .withColumn("LOAD_TIMESTAMP", current_timestamp())
    .withColumn("CREATED_DT", current_timestamp())
    .withColumn("UPDATED_DT", current_timestamp())
    .withColumn("RECORD_STATUS", lit("ACTIVE"))
)

print("Audit Columns Added")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# BUILD SILVER PATH
# ============================================================

silver_path = TARGET_PATH.replace("Bronze", "Silver")

print("="*70)
print("SILVER PATH")
print("="*70)

print(f"Silver Path : {silver_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CHECK IF SILVER EXISTS
# ============================================================

from delta.tables import DeltaTable

silver_full_path = f"Files/{silver_path}"

silver_exists = DeltaTable.isDeltaTable(spark, silver_full_path)

print(f"Silver Exists : {silver_exists}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# WRITE TO SILVER
# ============================================================

print("="*70)
print("WRITING TO SILVER")
print("="*70)

# First load
if not silver_exists:

    print("First Load")

    (
        df.write
          .format("delta")
          .mode("overwrite")
          .save(silver_full_path)
    )

# Existing table
else:

    # Full Load
    if CDC_FLAG == "N":

        print("Full Refresh")

        (
            df.write
              .format("delta")
              .mode("overwrite")
              .option("overwriteSchema","true")
              .save(silver_full_path)
        )

    # Incremental Load
    else:

        print("Incremental Merge")

        silver_delta = DeltaTable.forPath(
            spark,
            silver_full_path
        )

        (
            silver_delta.alias("T")
            .merge(
                df.alias("S"),
                f"T.{PRIMARY_KEY}=S.{PRIMARY_KEY}"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

print("Silver Load Completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# METRICS
# ============================================================

rows_written = df.count()

print("="*70)
print("LOAD SUMMARY")
print("="*70)

print(f"Target Path    : {silver_full_path}")
print(f"Load Type      : {LOAD_TYPE}")
print(f"CDC            : {CDC_FLAG}")

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
        
        "STATUS":"SUCCESS"
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
