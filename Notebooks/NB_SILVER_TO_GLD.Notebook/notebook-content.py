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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# NOTEBOOK PARAMETERS
# ============================================================

metadata_id = ""

pipeline_run_id = ""

try:
    metadata_id = METADATA_ID
except:
    metadata_id = "1"

try:
    pipeline_run_id = PIPELINE_RUN_ID
except:
    pipeline_run_id = "LOCAL_RUN"

print("="*80)
print("NOTEBOOK PARAMETERS")
print("="*80)

print(f"metadata ID : {metadata_id}")
print(f"Pipeline Run ID : {pipeline_run_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# NOTEBOOK PARAMETERS
# ============================================================

metadata_id = ""

pipeline_run_id = ""

try:
    metadata_id = METADATA_ID
except:
    metadata_id = "1"

try:
    pipeline_run_id = PIPELINE_RUN_ID
except:
    pipeline_run_id = "LOCAL_RUN"

print("="*80)
print("NOTEBOOK PARAMETERS")
print("="*80)

print(f"metadata ID : {metadata_id}")
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
    spark.table("CTL.CTL_INTEGRATION_METADATA")
         .filter(col("METADATA_ID") == int(metadata_id))
)

if metadata_df.count() == 0:
    raise Exception(f"Metadata not found for METADATA_ID = {metadata_id}")

metadata = metadata_df.first()

#-------------------------------------------------------------
# Metadata Variables
#-------------------------------------------------------------

metadata_id = metadata["METADATA_ID"]

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

print(f"Integration ID : {metadata_id}")
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
# READ ALL SILVER TABLES FOR SAME GOLD TABLE
# ============================================================

gold_metadata_df = (
    spark.table("CTL.CTL_INTEGRATION_METADATA")
         .filter(col("GOLD_TABLE") == gold_table)
         .orderBy("METADATA_ID")
)

print("="*80)
print(f"ALL SILVER TABLES FOR GOLD TABLE : {gold_table}")
print("="*80)

gold_metadata_df.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# READ SILVER FILES
# ============================================================

silver_dfs = []

for row in gold_metadata_df.collect():

    silver_path = (

        f"Files/"

        + row["TARGET_PATH"]

            .replace(

                "Bronze",

                "Silver"

            )

    )

    print(f"Reading : {silver_path}")

    df = (
        spark.read
             .format("delta")
             .load(silver_path)
    )

    print(f"Rows : {df.count()}")

    silver_dfs.append(df)

print("="*80)
print(f"Total Silver Tables Read : {len(silver_dfs)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# UNION ALL SILVER DATAFRAMES
# ============================================================

print("="*80)
print("UNION SILVER TABLES")
print("="*80)

if len(silver_dfs) == 1:

    gold_df = silver_dfs[0]

else:

    gold_df = reduce(
        lambda df1, df2: df1.unionByName(df2),
        silver_dfs
    )

print(f"Total Rows : {gold_df.count()}")

gold_df.printSchema()

gold_df.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# BUSINESS TRANSFORMATIONS
# ============================================================

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

gold_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CHECK IF GOLD TABLE EXISTS
# ============================================================

from pyspark.sql.utils import AnalysisException

gold_table_name = f"dbo.{gold_table}"

try:

    spark.table(gold_table_name)

    table_exists = True

except AnalysisException:

    table_exists = False

print("="*80)
print("CHECK GOLD TABLE")
print("="*80)

print(f"Gold Table : {gold_table_name}")

print(f"Exists : {table_exists}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# FIRST LOAD / MERGE
# ============================================================

from delta.tables import DeltaTable



gold_table_name = f"dbo.{gold_table.upper()}"

if table_exists == False:

    print("="*80)
    print("FIRST LOAD")
    print("="*80)

    (
        gold_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(gold_table_name)
    )

    print(f"{gold_table_name} Created Successfully")

else:

    print("="*80)
    print("MERGE")
    print("="*80)

    deltaTable = DeltaTable.forName(
        spark,
        gold_table_name
    )

    (
        deltaTable.alias("T")
        .merge(
            gold_df.alias("S"),
            "T.INTEGRATION_ID = S.INTEGRATION_ID"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"{gold_table_name} Updated Successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# VALIDATION
# ============================================================

print("="*80)
print("VALIDATION")
print("="*80)

validation_df = spark.table(gold_table_name)

print(f"Rows : {validation_df.count()}")

validation_df.printSchema()

validation_df.show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# NOTEBOOK COMPLETED
# ============================================================

result = {
    "STATUS": "SUCCESS",
    "METADATA_ID": metadata_id,
    "GOLD_TABLE": gold_table,
    "ROWS": validation_df.count()
}

print(result)

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
