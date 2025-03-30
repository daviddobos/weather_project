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
# META       "default_lakehouse_workspace_id": "a65228af-1fd8-41a6-8294-aceec3a6ebd5"
# META     }
# META   }
# META }

# CELL ********************

df = spark.read.parquet("Files/landing/weatherapi/weather_ws_dev/weather_measure/20250322/weather_measure_20250322_20250323T123321.parquet")

# Show the first few rows of the DataFrame
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
