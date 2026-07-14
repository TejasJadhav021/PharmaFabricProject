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

PROJECT_NAME = "PharmaFlow360"

ENVIRONMENT = "DEV"

LAKEHOUSE_NAME = "LH_PHARMA"

METADATA_TABLE = "CTL.CTL_INTEGRATION_METADATA"

AUDIT_TABLE = "CTL.CTL_PIPELINE_AUDIT"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

BRONZE_PATH = "Files/Bronze"

SILVER_PATH = "Tables"

GOLD_PATH = "Tables"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set(
    "spark.databricks.delta.schema.autoMerge.enabled",
    "true"
)

spark.conf.set(
    "spark.sql.shuffle.partitions",
    "8"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("===================================")
print(" PharmaFlow360 Configuration ")
print("===================================")

print(PROJECT_NAME)

print(ENVIRONMENT)

print(LAKEHOUSE_NAME)

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
