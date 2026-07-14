# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
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
