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

# PARAMETERS CELL ********************

p_start_dt = '2025-01-01'
p_end_dt = '2025-12-31'
p_table_nm = "ld_date"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime, timedelta
import pandas as pd
from pyspark.sql import SparkSession

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Convert to date
v_start_dt = datetime.strptime(p_start_dt, "%Y-%m-%d").date()
v_end_dt = datetime.strptime(p_end_dt, "%Y-%m-%d").date()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2. Generate date list
date_list = [v_start_dt + timedelta(days=x) for x in range((v_end_dt - v_start_dt).days + 1)]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3. Create pandas df
df = pd.DataFrame({"date": pd.to_datetime(date_list)})
df["date_timestamp"] = df["date"]
df["year_num"] = df["date"].dt.year
df["year_txt"] = df["year_num"].astype(str)
df["quarter_num"] = df["date"].dt.quarter
df["quarter_txt"] = "Q" + df["quarter_num"].astype(str)
df["month_num"] = df["date"].dt.month
df["month_txt"] = df["date"].dt.strftime("%B")
df["day_num"] = df["date"].dt.day
df["day_txt"] = df["date"].dt.strftime("%A")
df["year_quarter_txt"] = df["year_txt"] + "_Q" + df["quarter_num"].astype(str)
df["year_month_txt"] = df["date"].dt.strftime("%Y_%m")

# Convert pandas df
spark = SparkSession.builder.getOrCreate()
spark_df = spark.createDataFrame(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Save as table
spark_df.write.mode("overwrite").format("delta").saveAsTable(p_table_nm)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
