# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8c3179ca-6f42-4378-8b2b-7a2e62924e9a",
# META       "default_lakehouse_name": "lh_weather",
# META       "default_lakehouse_workspace_id": "a65228af-1fd8-41a6-8294-aceec3a6ebd5",
# META       "known_lakehouses": [
# META         {
# META           "id": "8c3179ca-6f42-4378-8b2b-7a2e62924e9a"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

p_load_date = '20250515'
p_source_sytem_cd = "AZ"
p_in_table_name = "sec_group_members"
p_root_path = 'Files/landing'
p_out_table_name = "ld_sec_group_members"
p_debug = 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os
from datetime import datetime, timedelta

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#check for parameter date, if there is none use yesterday's date
if not p_load_date:
    yesterday = datetime.now() - timedelta(days=1)
    v_valid_dt = yesterday.strftime("%Y%m%d")
else:
    v_valid_dt = p_load_date

print(v_valid_dt)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not p_debug:
    v_debug = 0
else:
    v_debug = p_debug

print('v_debug: ', v_debug)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

v_workspace_name = mssparkutils.env.getWorkspaceName()
print('v_workspace_name:', v_workspace_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# List files in the folder
v_folder_path = p_root_path + '/' + p_source_sytem_cd + '/' + v_workspace_name + '/' + p_in_table_name + '/' + v_valid_dt
v_files = mssparkutils.fs.ls(v_folder_path)

# Sort files alphabetically by name
v_sorted_files = sorted(v_files, key=lambda x: x.name)

# Get path for the last file in alphabetical order
v_latest_file_path = v_sorted_files[-1].path
print('Latest file path in alphabetical order is: ', v_latest_file_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read the Parquet files recursively
parquet_df = spark.read.format("parquet").option("mergeSchema", "true").option("recursiveFileLookup", "true").option("ignoreCorruptFiles", "true").load(v_latest_file_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cast all columns to string
parquet_string_df = parquet_df.select([col(c).cast("string").alias(c) for c in parquet_df.columns])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create m_updated_at_dttm
parquet_upd_df = parquet_string_df.withColumn("m_updated_at_dttm", from_utc_timestamp(current_timestamp(), "Europe/Budapest"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if v_debug:
    display(parquet_upd_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write the Delta table
parquet_upd_df.write.format("delta")\
                .option("mergeSchema", "true")\
                .partitionBy("m_valid_dt")\
                .option("partitionOverwriteMode", "dynamic")\
                .mode("overwrite")\
                .saveAsTable(p_out_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
