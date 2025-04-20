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

# PARAMETERS CELL ********************

# Parameters
p_load_dt = None
p_load_days_no = '4'
p_save_path_root = 'Files/landing'
p_source_system_cd = 'weatherapi'
p_table_name = 'weather_astro_forecast'

base_url = "http://api.weatherapi.com/v1/forecast.json"

p_keyvault_url = 'https://dd-thesis-keyvault.vault.azure.net/'
p_secret_name_secret = 'dd-thesis-sp-secret'
p_secret_name_tenant = 'dd-thesis-sp-tenant-id'
p_secret_name_client = 'dd-thesis-sp-client-id'

p_api_secret_name = 'weatherapi-api-key'

p_debug = 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
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
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from deep_translator import GoogleTranslator  # Importing GoogleTranslator from deep-translator

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#check for parameter date, if there is none use yesterday's date
if not p_load_dt:
    yesterday = datetime.now() - timedelta(days=1)
    v_valid_dt = yesterday.strftime("%Y%m%d")
else:
    v_valid_dt = p_load_dt
    v_valid_dt = datetime.strptime(v_valid_dt, "%Y%m%d")
    v_valid_dt = v_valid_dt.strftime("%Y%m%d")
print('v_valid_dt:', v_valid_dt)

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

v_m_valid_dt = datetime.strptime(v_valid_dt, "%Y%m%d").strftime("%Y-%m-%d")

print('v_m_valid_dt:', v_m_valid_dt)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#check for parameter date, if there is none use yesterday's date
v_now_dttm = datetime.now().strftime("%Y%m%dT%H%M%S")

print('v_now_dttm:', v_now_dttm)

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

# Output path
v_file_path = p_save_path_root + '/' + p_source_system_cd + '/' + v_workspace_name + '/' + p_table_name + '/' + v_valid_dt + '/' + p_table_name + '_' + v_valid_dt + '_' + v_now_dttm + '.parquet' 
print('v_file_path:', v_file_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fetch the credentials using the service principal
tenant_id = notebookutils.credentials.getSecret(p_keyvault_url, p_secret_name_tenant)
client_id = notebookutils.credentials.getSecret(p_keyvault_url, p_secret_name_client)
client_secret = notebookutils.credentials.getSecret(p_keyvault_url, p_secret_name_secret)

# Create a credential using the service principal
credential = ClientSecretCredential(tenant_id, client_id, client_secret)

# Initialize SecretClient with Key Vault URL and the credentials
client = SecretClient(vault_url=p_keyvault_url, credential=credential)

api_key = client.get_secret(p_api_secret_name).value

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Cities to request weather for
city_df = spark.read.format("delta").load("Tables/dbo/city")

# Prepare the locations for bulk request dynamically from the city table
locations = [{"q": row.City, "custom_id": f"{row.Country}-{row.County}-{row.City}"} for row in city_df.collect()]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

all_astro_forecast_dfs = []

# Save to or update the table
def save_data(df):
    df = df.withColumn("m_valid_dt", lit(v_m_valid_dt))

    # Create m_updated_at_dttm
    extracted_df = df.withColumn("m_extracted_at_dttm", from_utc_timestamp(current_timestamp(), "Europe/Budapest")) \
                           .withColumn("m_updated_at_dttm", from_utc_timestamp(current_timestamp(), "Europe/Budapest"))

    # Output
    extracted_df.write.format("parquet").mode("overwrite").save(v_file_path)

# Function to send request for a batch of cities
def send_bulk_request(locations_batch):
    for location_data in locations_batch:
        location_name = location_data["q"]
        custom_id = location_data["custom_id"]

        params = {
            "key": api_key,
            "q": location_name,
            "days": p_load_days_no, 
            "aqi": "no",  # Air Quality Index data
            "alerts": "no"  # Disable weather alerts
        }
        
        # Send the GET request to the WeatherAPI
        response = requests.get(base_url, params=params)

        if response.status_code == 200:
            # Parse the JSON response
            data = response.json()
            print(f"Response JSON for location {location_name}:")
            print(json.dumps(data, indent=4))

            # Process forecast data for each location
            try:
                location = data["location"]
                forecast_data = data["forecast"]["forecastday"]

                for forecast_day in forecast_data:
                    forecast_date = forecast_day["date"]
                    astro_data = forecast_day["astro"]

                    # Astro data
                    astro_forecast_df = spark.read.json(spark.sparkContext.parallelize([astro_data]))

                    # Add city, country, forecast_date, and p_load_dt columns to astro data
                    astro_forecast_df = astro_forecast_df.withColumn("city", lit(location["name"]))
                    astro_forecast_df = astro_forecast_df.withColumn("country", lit(location["country"]))
                    astro_forecast_df = astro_forecast_df.withColumn("forecast_date", lit(forecast_date))

                    all_astro_forecast_dfs.append(astro_forecast_df)
                    # # Air quality data
                    # air_df = spark.read.json(spark.sparkContext.parallelize([astro_data]))

                    # # Add city, country, forecast_date, and p_load_dt columns to astro data
                    # astro_df = astro_df.withColumn("city", lit(location["name"]))
                    # astro_df = astro_df.withColumn("country", lit(location["country"]))
                    # astro_df = astro_df.withColumn("forecast_date", lit(forecast_date))
                    # astro_df = astro_df.withColumn("p_load_dt", lit(p_load_dt))

                    # # Save air quality data to Delta table
                    # astro_df.write \
                    #     .format("delta") \
                    #     .mode("append") \
                    #     .option("mergeSchema", "true") \
                    #     .save(forecast_astro_table_path)


            except KeyError as e:
                print(f"No astro forecast data found for {location_name}. Skipping astro forecast processing.")
        else:
            print(f"Error: {response.status_code}, {response.text}")


# Split the locations into batches of 50
batch_size = 50
location_batches = [locations[i:i + batch_size] for i in range(0, len(locations), batch_size)]

# Process each batch
for batch in location_batches:
    send_bulk_request(batch)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Combine and save dataframes
combined_astro_forecast_df = all_astro_forecast_dfs[0]
for df in all_astro_forecast_dfs[1:]:
    combined_astro_forecast_df = combined_astro_forecast_df.union(df)

deduplicated_combined_astro_forecast_df = combined_astro_forecast_df.dropDuplicates()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get distinct values of the country column
distinct_values_df = deduplicated_combined_astro_forecast_df.select(col('country')).distinct()

if v_debug:
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

if v_debug:
    display(translated_spark_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

translated_final_df = deduplicated_combined_astro_forecast_df.join(translated_spark_df, on='country', how="left")

if v_debug:
    translated_final_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

save_data(translated_final_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
