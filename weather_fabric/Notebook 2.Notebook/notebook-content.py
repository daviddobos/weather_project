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

import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, timedelta
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import date_format
import msal

# import requests
# import pandas as pd
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import *
# import json
# from datetime import datetime, timedelta
# from pyspark.sql import DataFrame
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from deep_translator import GoogleTranslator  # Importing GoogleTranslator from deep-translator



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.parquet("Files/landing/weatherapi/weather_ws_dev/weather_astro_forecast/20250405/weather_astro_forecast_20250405_20250406T041748.parquet")
# df now is a Spark DataFrame containing parquet data from "Files/landing/weatherapi/weather_ws_dev/weather_astro_forecast/20250405/weather_astro_forecast_20250405_20250406T041748.parquet".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.parquet("Files/landing/weatherapi/weather_ws_dev/weather_forecast/20250324/weather_forecast_20250324_20250325T051809.parquet")
# df now is a Spark DataFrame containing parquet data from "Files/landing/weatherapi/weather_ws_dev/weather_forecast/20250324/weather_forecast_20250324_20250325T051809.parquet".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get distinct values of the country column
distinct_values_df = df.select(col('country')).distinct()


display(distinct_values_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

texts_to_translate = [row['country'] for row in distinct_values_df.collect()]

# Batch translate with Deep Translator
try:
    translated_texts = GoogleTranslator(source='auto', target='en').translate_batch(texts_to_translate)
except Exception as e:
    print(f"Batch translation error: {e}")
    translated_texts = texts_to_translate  # Fallback to the original text
# Pandas DataFrame with the translations
import pandas as pd
translated_pd_df = pd.DataFrame({
    'country': texts_to_translate,
    'country_EN': translated_texts
})

translated_spark_df = spark.createDataFrame(translated_pd_df)

display(translated_spark_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

translated_final_df = df.join(translated_spark_df, on='country', how="left")
display(translated_final_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

v_file_path = 'Files/landing/weatherapi/weather_ws_dev/weather_forecast/20250324/weather_forecast_20250324_20250325T051809.parquet'
translated_final_df.write.format("parquet").mode("overwrite").save(v_file_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
