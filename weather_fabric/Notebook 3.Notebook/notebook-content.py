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

df = spark.sql("SELECT * FROM lh_weather.dbo.ld_weather_forecast_bckp")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_filtered = df.filter(~(
    (df.m_valid_dt == '2025-03-26') & (df.forecast_date == '2025-03-27') & (df.country_EN == 'Hungary') & (df.city == 'Békéscsaba')  & (df.time_epoch == '1743051600') & (df.cloud == 0)
)).filter(~(
    (df.m_valid_dt == '2025-03-30') & (df.forecast_date == '2025-03-31') & (df.country_EN == 'Hungary') & (df.city == 'Békéscsaba')  & (df.time_epoch == '1743393600') & (df.is_day == 0)
)).filter(~(
    (df.m_valid_dt == '2025-03-27') & (df.forecast_date == '2025-03-28') & (df.country_EN == 'Hungary') & (df.city == 'Békéscsaba')  & (df.time_epoch == '1743138000') & (df.cloud == 0)
))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(df.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(df_filtered.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write the Delta table
df_filtered.write.format("delta")\
                .option("mergeSchema", "true")\
                .partitionBy("m_valid_dt")\
                .option("partitionOverwriteMode", "dynamic")\
                .mode("overwrite")\
                .saveAsTable('ld_weather_forecast')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
