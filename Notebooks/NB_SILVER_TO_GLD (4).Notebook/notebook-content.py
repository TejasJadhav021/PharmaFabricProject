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

# Welcome to your new notebook
# Type here in the cell editor to add code!
# ============================================================
# SILVER TO GOLD
# IMPORTS
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from delta.tables import DeltaTable

from functools import reduce

from datetime import datetime

import traceback

start_time = datetime.now()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# NOTEBOOK PARAMETERS
# ============================================================

integration_id = ""

pipeline_run_id = ""

try:
    integration_id = INTEGRATION_ID
except:
    integration_id = "1"

try:
    pipeline_run_id = PIPELINE_RUN_ID
except:
    pipeline_run_id = "LOCAL_RUN"

print("="*80)
print("NOTEBOOK PARAMETERS")
print("="*80)

print(f"Integration ID : {integration_id}")
print(f"Pipeline Run ID : {pipeline_run_id}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# READ METADATA
# ============================================================

from pyspark.sql.functions import col

metadata_df = (
    spark.table(METADATA_TABLE)
         .filter(col("INTEGRATION_ID") == int(integration_id))
)

if metadata_df.count() == 0:
    raise Exception(f"Metadata not found for INTEGRATION_ID = {integration_id}")

metadata = metadata_df.first()

#-------------------------------------------------------------
# Metadata Variables
#-------------------------------------------------------------

integration_id = metadata["INTEGRATION_ID"]

source_system = metadata["SOURCE_SYSTEM"]

source_object = metadata["SOURCE_OBJECT"]

target_table = metadata["TARGET_TABLE"]

gold_table = metadata["GOLD_TABLE"]

primary_key = metadata["PRIMARY_KEY_COLUMN"]

load_type = metadata["LOAD_TYPE"]

file_format = metadata["FILE_FORMAT"]

print("="*80)
print("METADATA LOADED")
print("="*80)

print(f"Integration ID : {integration_id}")
print(f"Source System  : {source_system}")
print(f"Source Object  : {source_object}")
print(f"Target Table   : {target_table}")
print(f"Gold Table     : {gold_table}")
print(f"Primary Key    : {primary_key}")
print(f"Load Type      : {load_type}")

metadata_df.show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# MAIN PROCESSING
# ============================================================

from pyspark.sql.utils import AnalysisException

rows_written = None

try:

    # ---- READ ALL SILVER TABLES FOR THIS GOLD TABLE ----

    gold_metadata_df = (
        spark.table(METADATA_TABLE)
             .filter(col("GOLD_TABLE") == gold_table)
             .orderBy("INTEGRATION_ID")
    )

    print("="*80)
    print(f"ALL SILVER TABLES FOR GOLD TABLE : {gold_table}")
    print("="*80)

    gold_metadata_df.show(truncate=False)

    # ---- READ SILVER FILES ----

    silver_dfs = []

    for row in gold_metadata_df.collect():

        silver_path = (
            f"{FILES_ROOT_PATH}/"
            + row["TARGET_PATH"].replace(BRONZE_FOLDER_NAME, SILVER_FOLDER_NAME)
        )

        print(f"Reading : {silver_path}")

        silver_df = (
            spark.read
                 .format("delta")
                 .load(silver_path)
        )

        print(f"Rows : {silver_df.count()}")

        silver_dfs.append(silver_df)

    print(f"Total Silver Tables Read : {len(silver_dfs)}")

    # ---- UNION ALL SILVER DATAFRAMES ----

    print("="*80)
    print("UNION SILVER TABLES")
    print("="*80)

    if len(silver_dfs) == 1:
        gold_df = silver_dfs[0]
    else:
        gold_df = reduce(lambda df1, df2: df1.unionByName(df2), silver_dfs)

    print(f"Total Rows : {gold_df.count()}")

    # ---- BUSINESS TRANSFORMATIONS ----

    print("="*80)
    print("BUSINESS TRANSFORMATIONS")
    print("="*80)

    if gold_table == "FACT_SALES":

        gold_df = (
            gold_df
            .withColumn("SALE_YEAR", year("SALE_DATE"))
            .withColumn("SALE_MONTH", month("SALE_DATE"))
            .withColumn("SALE_QUARTER", quarter("SALE_DATE"))
        )

    elif gold_table == "FACT_PRESCRIPTIONS":

        gold_df = (
            gold_df
            .withColumn("PRESCRIPTION_YEAR", year("PRESCRIPTION_DATE"))
            .withColumn("PRESCRIPTION_MONTH", month("PRESCRIPTION_DATE"))
        )

    print("Business Transformations Completed")

    # ---- CHECK IF GOLD TABLE EXISTS ----

    gold_table_name = f"{GOLD_SCHEMA_NAME}.{gold_table.upper()}"

    try:
        spark.table(gold_table_name)
        table_exists = True
    except AnalysisException:
        table_exists = False

    print(f"Gold Table : {gold_table_name} | Exists : {table_exists}")

    # ---- FIRST LOAD / MERGE / SCD2 ----

    partition_columns = GOLD_PARTITION_COLUMNS.get(gold_table, [])

    if gold_table in SCD2_DIM_TABLES:

        # SCD Type 2 load (full change history kept for this dim).
        # Dim tables are intentionally NOT partitioned - low row
        # counts, so partitioning would just create excess small files.

        print("="*80)
        print(f"SCD TYPE 2 LOAD : {gold_table_name}")
        print("="*80)

        tracked_columns = [
            c for c in gold_df.columns
            if c not in SCD2_EXCLUDED_COLUMNS
            and c != primary_key
            and c != INTEGRATION_ID_COLUMN
        ]

        print(f"SCD2 Tracked Columns : {tracked_columns}")

        merge_scd2(
            source_df=gold_df,
            table_name=gold_table_name,
            primary_key=primary_key,
            tracked_columns=tracked_columns,
            effective_start_col=SCD2_EFFECTIVE_START_COLUMN,
            effective_end_col=SCD2_EFFECTIVE_END_COLUMN,
            current_flag_col=SCD2_CURRENT_FLAG_COLUMN
        )

        zorder_columns = [primary_key]

    elif table_exists == False:

        print("="*80)
        print("FIRST LOAD")
        print("="*80)

        writer = (
            gold_df.write
                .format("delta")
                .mode("overwrite")
        )

        if partition_columns:
            writer = writer.partitionBy(*partition_columns)
            print(f"Partitioning by : {partition_columns}")

        writer.saveAsTable(gold_table_name)

        print(f"{gold_table_name} Created Successfully")

        zorder_columns = [INTEGRATION_ID_COLUMN]

    else:

        print("="*80)
        print("MERGE")
        print("="*80)

        deltaTable = DeltaTable.forName(spark, gold_table_name)

        (
            deltaTable.alias("T")
            .merge(
                gold_df.alias("S"),
                f"T.{INTEGRATION_ID_COLUMN} = S.{INTEGRATION_ID_COLUMN}"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        print(f"{gold_table_name} Updated Successfully")

        zorder_columns = [INTEGRATION_ID_COLUMN]

    rows_written = gold_df.count()

    # ---- DELTA MAINTENANCE : COMPACT FILES + ZORDER ----

    optimize_table(
        table_name=gold_table_name,
        zorder_columns=zorder_columns
    )

    # ---- VALIDATION ----

    print("="*80)
    print("VALIDATION")
    print("="*80)

    validation_df = spark.table(gold_table_name)

    print(f"Rows : {validation_df.count()}")

    # ---- AUDIT LOG : SUCCESS ----

    end_time = datetime.now()

    write_audit_log(
        audit_table=AUDIT_TABLE,
        pipeline_run_id=pipeline_run_id,
        integration_id=integration_id,
        notebook_name="NB_SILVER_TO_GLD",
        status="SUCCESS",
        start_time=start_time,
        end_time=end_time,
        rows_written=rows_written
    )

    result = {
        "STATUS": "SUCCESS",
        "INTEGRATION_ID": integration_id,
        "GOLD_TABLE": gold_table,
        "ROWS": rows_written
    }

    print(result)

except Exception as e:

    end_time = datetime.now()

    write_audit_log(
        audit_table=AUDIT_TABLE,
        pipeline_run_id=pipeline_run_id,
        integration_id=integration_id,
        notebook_name="NB_SILVER_TO_GLD",
        status="FAILED",
        start_time=start_time,
        end_time=end_time,
        rows_written=rows_written,
        error_message=str(e)
    )

    print("="*80)
    print("NB_SILVER_TO_GLD FAILED")
    print("="*80)

    print(traceback.format_exc())

    raise


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
