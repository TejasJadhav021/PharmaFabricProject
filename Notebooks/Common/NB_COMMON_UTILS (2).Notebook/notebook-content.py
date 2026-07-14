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

# ==========================================================
# NB_COMMON_UTILS
# Enterprise Utility Library
# Microsoft Fabric Lakehouse
# ==========================================================

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

from pyspark.sql.functions import (
    col,
    trim,
    upper,
    lower,
    when,
    lit,
    current_timestamp,
    monotonically_increasing_id,
    sha2,
    concat_ws
)

from pyspark.sql.types import *

from delta.tables import DeltaTable

from datetime import datetime

import traceback


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================================================
# READ FUNCTIONS
# ==========================================================

def read_delta(path: str) -> DataFrame:
    """
    Reads a Delta table from a Lakehouse path.
    """

    return (
        spark.read
             .format("delta")
             .load(path)
    )


def read_parquet(path: str) -> DataFrame:
    """
    Reads parquet files.
    """

    return (
        spark.read
             .parquet(path)
    )


def read_table(table_name: str) -> DataFrame:
    """
    Reads a managed Fabric table.
    """

    return spark.table(table_name)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================================================
# WRITE FUNCTIONS
# ==========================================================

def write_delta(
    df: DataFrame,
    path: str,
    mode: str = "overwrite",
    partition_columns: list = None
):
    """
    Writes dataframe as Delta.
    """

    writer = (
        df.write
          .format("delta")
          .mode(mode)
    )

    if partition_columns:

        writer = writer.partitionBy(*partition_columns)

    writer.save(path)

    print(f"Data written successfully -> {path}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================================================
# DELTA FUNCTIONS
# ==========================================================

def delta_table_exists(path: str) -> bool:
    """
    Returns True if Delta table exists.
    """

    return DeltaTable.isDeltaTable(spark, path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================================================
# GENERIC DELTA MERGE
# ==========================================================

def merge_delta(
    source_df: DataFrame,
    target_path: str,
    primary_key: str,
    load_type: str = "INCREMENTAL"
):
    """
    Generic Delta Merge Function

    Parameters
    ----------
    source_df : Source DataFrame

    target_path : Delta Table Path

    primary_key : Primary Key Column

    load_type :
        FULL
        INCREMENTAL
    """

    # -------------------------------
    # FIRST LOAD
    # -------------------------------

    if not delta_table_exists(target_path):

        print(f"Target Delta Table does not exist.")

        (
            source_df.write
            .format("delta")
            .mode("overwrite")
            .save(target_path)
        )

        print("Initial Delta table created.")

        return

    # -------------------------------
    # FULL LOAD
    # -------------------------------

    if load_type.upper() == "FULL":

        print("Executing Full Load...")

        (
            source_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(target_path)
        )

        print("Full Load Completed.")

        return

    # -------------------------------
    # INCREMENTAL MERGE
    # -------------------------------

    print("Executing Incremental Merge...")

    delta_table = DeltaTable.forPath(
        spark,
        target_path
    )

    (
        delta_table.alias("T")
        .merge(
            source_df.alias("S"),
            f"T.{primary_key}=S.{primary_key}"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("Incremental Merge Completed.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================================================
# GENERIC SCD TYPE 2 MERGE
# ==========================================================

def merge_scd2(
    source_df: DataFrame,
    table_name: str,
    primary_key: str,
    tracked_columns: list,
    effective_start_col: str = "EFF_START_DATE",
    effective_end_col: str = "EFF_END_DATE",
    current_flag_col: str = "IS_CURRENT"
):
    """
    Generic Slowly Changing Dimension Type 2 merge for a managed
    Delta catalog table.

    How it works
    ------------
    1. A hash is computed over `tracked_columns` for every
       incoming source row. This hash is the "fingerprint" of
       that row's business attributes.

    2. The incoming hash is compared to the hash of the CURRENT
       version of the same primary_key already in the target
       table (current_flag_col = true).

         - No matching key in target -> brand new record.
         - Matching key, same hash    -> nothing changed, skip.
         - Matching key, different
           hash                       -> the dimension changed.

    3. For every changed / new key:
         - The existing CURRENT row (if any) is "expired": its
           effective_end_col is stamped with now() and
           current_flag_col is set to false. The old row is
           kept forever - that's the history.
         - A brand new row is appended with effective_start_col
           = now(), effective_end_col = NULL, current_flag_col
           = true. This becomes the new current version.

    Unlike merge_delta() (SCD Type 1, whenMatchedUpdateAll),
    this never overwrites a row in place - it always keeps the
    old version and adds a new one, so you can answer
    "what did this record look like on date X".

    Parameters
    ----------
    source_df : Cleaned source DataFrame for this load (one row
        per primary_key, current business values only).

    table_name : Target managed Delta table name, e.g. "dbo.DIM_PRODUCT".

    primary_key : Business/primary key column that identifies a
        dimension member across versions (e.g. "PRODUCT_ID").

    tracked_columns : Columns that should trigger a new version
        when their value changes. Exclude technical/audit columns
        (load timestamps, pipeline run id, etc.) - those change
        every run and would create a new version every time.

    effective_start_col, effective_end_col, current_flag_col :
        Names of the SCD2 technical columns to create/maintain.
    """

    from pyspark.sql.functions import current_timestamp, lit, sha2, concat_ws, col
    from pyspark.sql.utils import AnalysisException

    # -------------------------------
    # FINGERPRINT THE SOURCE ROWS
    # -------------------------------

    source_df = source_df.withColumn(
        "_SCD_HASH",
        sha2(
            concat_ws(
                "||",
                *[col(c).cast("string") for c in tracked_columns]
            ),
            256
        )
    )

    # -------------------------------
    # FIRST LOAD - TABLE DOES NOT EXIST YET
    # -------------------------------

    try:
        spark.table(table_name)
        table_exists = True
    except AnalysisException:
        table_exists = False

    if not table_exists:

        print(f"SCD2 target '{table_name}' does not exist. Creating initial load.")

        (
            source_df
            .withColumn(effective_start_col, current_timestamp())
            .withColumn(effective_end_col, lit(None).cast("timestamp"))
            .withColumn(current_flag_col, lit(True))
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(table_name)
        )

        print(f"{table_name} created (SCD2 initial load).")

        return

    # -------------------------------
    # FIND NEW / CHANGED RECORDS
    # -------------------------------

    target_table = DeltaTable.forName(spark, table_name)

    current_target = (
        target_table.toDF()
        .filter(col(current_flag_col) == True)
        .select(primary_key, "_SCD_HASH")
    )

    changed_or_new = (
        source_df.alias("S")
        .join(
            current_target.alias("T"),
            on=primary_key,
            how="left"
        )
        .filter(
            col("T._SCD_HASH").isNull()
            | (col("S._SCD_HASH") != col("T._SCD_HASH"))
        )
        .select("S.*")
    )

    change_count = changed_or_new.count()

    print(f"New / Changed Records Detected : {change_count}")

    if change_count == 0:
        print("No new or changed records. SCD2 merge skipped.")
        return

    # -------------------------------
    # STEP 1 : EXPIRE THE OLD CURRENT VERSION
    # -------------------------------

    (
        target_table.alias("T")
        .merge(
            changed_or_new.select(primary_key).distinct().alias("S"),
            f"T.{primary_key} = S.{primary_key} AND T.{current_flag_col} = true"
        )
        .whenMatchedUpdate(
            set={
                effective_end_col: "current_timestamp()",
                current_flag_col: "false"
            }
        )
        .execute()
    )

    # -------------------------------
    # STEP 2 : INSERT THE NEW CURRENT VERSION
    # -------------------------------

    (
        changed_or_new
        .withColumn(effective_start_col, current_timestamp())
        .withColumn(effective_end_col, lit(None).cast("timestamp"))
        .withColumn(current_flag_col, lit(True))
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(table_name)
    )

    print(f"{table_name} updated - {change_count} new SCD2 version(s) inserted.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================================================
# DATA CLEANING FUNCTIONS
# ==========================================================

def trim_string_columns(df: DataFrame) -> DataFrame:
    """
    Trim leading and trailing spaces from all string columns.
    """

    for field in df.schema.fields:

        if isinstance(field.dataType, StringType):

            df = df.withColumn(
                field.name,
                trim(col(field.name))
            )

    return df


def uppercase_columns(
    df: DataFrame,
    columns: list
) -> DataFrame:
    """
    Convert selected columns to upper case.
    """

    for c in columns:

        if c in df.columns:

            df = df.withColumn(
                c,
                upper(col(c))
            )

    return df


def lowercase_columns(
    df: DataFrame,
    columns: list
) -> DataFrame:
    """
    Convert selected columns to lower case.
    """

    for c in columns:

        if c in df.columns:

            df = df.withColumn(
                c,
                lower(col(c))
            )

    return df


def remove_duplicates(
    df: DataFrame,
    primary_key: str
) -> DataFrame:
    """
    Remove duplicate records based on the primary key.
    """

    return df.dropDuplicates([primary_key])


def replace_null_values(
    df: DataFrame,
    replacements: dict
) -> DataFrame:
    """
    Replace null values using a dictionary.

    Example:
        {"CITY":"UNKNOWN","STATE":"NA"}
    """

    return df.fillna(replacements)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================================================
# VALIDATION FUNCTIONS
# ==========================================================

from pyspark.sql.functions import col


def get_row_count(df: DataFrame) -> int:
    """
    Returns the total number of rows.
    """

    return df.count()


def validate_primary_key(
    df: DataFrame,
    primary_key: str
):
    """
    Validate that the primary key exists.
    """

    if primary_key not in df.columns:
        raise Exception(
            f"Primary Key '{primary_key}' not found in dataframe."
        )

    print(f"Primary Key validation successful -> {primary_key}")


def validate_schema(
    source_df: DataFrame,
    target_df: DataFrame
):
    """
    Validate source and target schemas.
    """

    source_cols = set(source_df.columns)
    target_cols = set(target_df.columns)

    missing = target_cols - source_cols

    if len(missing) > 0:

        raise Exception(
            f"Missing columns : {missing}"
        )

    print("Schema validation successful.")


def split_good_bad_records(
    df: DataFrame,
    primary_key: str
):
    """
    Split dataframe into Good and Bad records.

    Bad records:
        - NULL Primary Key

    Returns
    -------
    good_df,bad_df
    """

    good_df = df.filter(
        col(primary_key).isNotNull()
    )

    bad_df = df.filter(
        col(primary_key).isNull()
    )

    return good_df,bad_df


def check_reject_percentage(
    total_rows: int,
    rejected_rows: int,
    threshold: float = 5.0
):
    """
    Validate reject percentage.

    Raises exception if reject percentage exceeds threshold.
    """

    if total_rows == 0:

        return

    reject_pct = (rejected_rows / total_rows) * 100

    print(f"Reject Percentage : {reject_pct:.2f}%")

    if reject_pct > threshold:

        raise Exception(
            f"Reject Percentage ({reject_pct:.2f}%) exceeded threshold ({threshold}%)."
        )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================================================
# DATA QUALITY FUNCTIONS
# ==========================================================

from pyspark.sql.functions import col


def remove_null_primary_key(
    df: DataFrame,
    primary_key: str
) -> DataFrame:
    """
    Remove records having NULL Primary Key.
    """

    return df.filter(
        col(primary_key).isNotNull()
    )


def remove_duplicate_records(
    df: DataFrame,
    primary_key: str
) -> DataFrame:
    """
    Remove duplicate records based on Primary Key.
    """

    return df.dropDuplicates(
        [primary_key]
    )


def get_duplicate_count(
    df: DataFrame,
    primary_key: str
) -> int:
    """
    Returns duplicate record count.
    """

    total = df.count()

    distinct = df.select(primary_key).distinct().count()

    return total - distinct


def dataframe_is_empty(
    df: DataFrame
) -> bool:
    """
    Returns True if dataframe is empty.
    """

    return df.rdd.isEmpty()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================================================
# SURROGATE KEY FUNCTIONS
# ==========================================================

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


def generate_surrogate_key(
    df: DataFrame,
    surrogate_column: str = "SK_ID"
) -> DataFrame:
    """
    Generate Sequential Surrogate Key
    """

    window = Window.orderBy(monotonically_increasing_id())

    return (
        df.withColumn(
            surrogate_column,
            row_number().over(window)
        )
    )


def generate_hash_key(
    df: DataFrame,
    columns: list,
    hash_column: str = "HASH_KEY"
) -> DataFrame:
    """
    Generate SHA256 Hash Key
    """

    return (
        df.withColumn(
            hash_column,
            sha2(
                concat_ws("||", *columns),
                256
            )
        )
    )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================================================
# HELPER FUNCTIONS
# ==========================================================

def current_time():

    return datetime.now()


def print_header(title: str):

    print("=" * 70)

    print(title)

    print("=" * 70)


def show_dataframe(df: DataFrame, rows: int = 5):

    df.show(rows, truncate=False)


def dataframe_info(df: DataFrame):

    print(f"Rows : {df.count()}")

    print(f"Columns : {len(df.columns)}")

    df.printSchema()


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
