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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# NOTEBOOK : NB_SILVER_TO_GOLD
#
# PURPOSE
# Generic Silver → Gold Processing
#
# FEATURES
#
# • Metadata Driven
# • Supports Multiple Silver Tables
# • Dynamic Union
# • Generic Merge
# • Integration_ID Merge Key
# • Enterprise Audit
#
# ============================================================

from pyspark.sql.functions import *

from delta.tables import DeltaTable

from notebookutils import mssparkutils

from functools import reduce

from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# PARAMETERS
# ============================================================

try:

    gold_table = GOLD_TABLE

except:

    gold_table = "FACT_SALES"

try:

    pipeline_run_id = PIPELINE_RUN_ID

except:

    pipeline_run_id = "LOCAL"

print("="*80)

print("PARAMETERS")

print("="*80)

print("Gold Table :",gold_table)

print("Pipeline :",pipeline_run_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# READ METADATA
# ============================================================

metadata_df = (

    spark.table("CTL.CTL_INTEGRATION_METADATA")

    .filter(

        col("GOLD_TABLE")==gold_table

    )

    .orderBy("EXECUTION_ORDER")

)

if metadata_df.rdd.isEmpty():

    raise Exception(

        f"No Metadata Found For {gold_table}"

    )

print("="*80)

print("METADATA")

print("="*80)

metadata_df.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# READ SILVER
# ============================================================

silver_dfs=[]

for row in metadata_df.collect():

    silver_path = (

        f"Files/"

        + row["TARGET_PATH"]

            .replace(

                "Bronze",

                "Silver"

            )

    )

    print(silver_path)

    df=(

        spark.read

             .format("delta")

             .load(silver_path)

    )

    silver_dfs.append(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# UNION ALL SILVER DATAFRAMES
# ============================================================

from functools import reduce

print("="*80)
print("UNION SILVER TABLES")
print("="*80)

if len(silver_dfs)==1:

    df=silver_dfs[0]

else:

    df=reduce(

        lambda x,y:x.unionByName(

            y,

            allowMissingColumns=True

        ),

        silver_dfs

    )

rows_read=df.count()

print("Rows Read :",rows_read)

df.show(5,False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# REMOVE DUPLICATES
# ============================================================

print("="*80)
print("REMOVE DUPLICATES")
print("="*80)

before=df.count()

df=df.dropDuplicates(["INTEGRATION_ID"])

after=df.count()

print("Before :",before)

print("After :",after)

print("Duplicates Removed :",before-after)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# REMOVE DUPLICATES
# ============================================================

print("="*80)
print("REMOVE DUPLICATES")
print("="*80)

before=df.count()

df=df.dropDuplicates(["INTEGRATION_ID"])

after=df.count()

print("Before :",before)

print("After :",after)

print("Duplicates Removed :",before-after)

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

# Remove leading/trailing spaces

for c,d in df.dtypes:

    if d=="string":

        df=df.withColumn(

            c,

            trim(col(c))

        )

# Standardize NULL strings

for c,d in df.dtypes:

    if d=="string":

        df=df.withColumn(

            c,

            when(

                trim(col(c))=="",

                None

            ).otherwise(

                col(c)

            )

        )

print("Transformation Completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# GOLD AUDIT COLUMNS
# ============================================================

from pyspark.sql.functions import current_timestamp,lit

df=(

df

.withColumn(

"GOLD_LOAD_TIMESTAMP",

current_timestamp()

)

.withColumn(

"GOLD_PIPELINE_RUN_ID",

lit(pipeline_run_id)

)

.withColumn(

"CURRENT_FLAG",

lit("Y")

)

)

print("Gold Audit Columns Added")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# GOLD PATH
# ============================================================

gold_path=f"Files/Gold/{gold_table}"

print("="*80)

print("Gold Path")

print("="*80)

print(gold_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CHECK GOLD TABLE
# ============================================================

from delta.tables import DeltaTable

print("="*80)
print("CHECK GOLD TABLE")
print("="*80)

gold_exists = DeltaTable.isDeltaTable(
    spark,
    gold_path
)

print(f"Gold Exists : {gold_exists}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# MERGE TO GOLD
# ============================================================

print("="*80)
print("MERGE TO GOLD")
print("="*80)

# ------------------------------------------------------------
# FIRST LOAD
# ------------------------------------------------------------

if not gold_exists:

    print("First Load - Creating Gold Table")

    (
        df.write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema","true")
          .save(gold_path)
    )

# ------------------------------------------------------------
# INCREMENTAL LOAD
# ------------------------------------------------------------

else:

    print("Incremental Merge")

    gold_delta = DeltaTable.forPath(
        spark,
        gold_path
    )

    (
        gold_delta.alias("T")
        .merge(
            df.alias("S"),
            "T.INTEGRATION_ID = S.INTEGRATION_ID"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

print("Gold Load Completed Successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# VALIDATE GOLD
# ============================================================

gold_df = (

    spark.read

         .format("delta")

         .load(gold_path)

)

rows_written = gold_df.count()

print("="*80)
print("GOLD VALIDATION")
print("="*80)

print(f"Rows Written : {rows_written}")

gold_df.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("SHOW TABLES").show(100, False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.catalog.listTables()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(gold_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# NOTEBOOK EXIT
# ============================================================

result = f"""
{{
    "STATUS":"SUCCESS",
    "GOLD_TABLE":"{gold_table}",
    "ROWS_READ":{rows_read},
    "ROWS_WRITTEN":{rows_written},
    "PIPELINE_RUN_ID":"{pipeline_run_id}"
}}
"""

print(result)

mssparkutils.notebook.exit(result)

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
