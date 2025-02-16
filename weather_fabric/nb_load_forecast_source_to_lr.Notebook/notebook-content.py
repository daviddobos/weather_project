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

# Parameters
p_load_dt = ''
p_load_days_no = '7'
p_save_path_root = 'Files/landing'
p_source_system_cd = 'weatherapi'
p_table_name = 'weather_forecast'

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

#check for parameter date, if there is none use yesterday's date
# if not p_load_dt:
#     yesterday = datetime.now() - timedelta(days=1) - timedelta(days=int(p_load_days_no))
#     v_valid_dt = yesterday.strftime("%Y%m%d")
# else:
#     v_valid_dt = p_load_dt
#     v_valid_dt = datetime.strptime(v_valid_dt, "%Y%m%d") - timedelta(days=int(p_load_days_no))
#     v_valid_dt = v_valid_dt.strftime("%Y%m%d")
# print('v_valid_dt:', v_valid_dt)

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

all_forecast_dfs = []

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
                    #air_data = forecast_day[]

                    # Hourly forecast data
                    forecast_data = forecast_day["hour"]
                    forecast_df = spark.read.json(spark.sparkContext.parallelize(forecast_data))

                    # Add city, country, forecast_date, and p_load_dt columns
                    forecast_df = forecast_df.withColumn("city", lit(location["name"]))
                    forecast_df = forecast_df.withColumn("country", lit(location["country"]))
                    forecast_df = forecast_df.withColumn("forecast_date", lit(forecast_date))

                    # Select the necessary columns
                    selected_columns = [
                        "chance_of_rain", "chance_of_snow", "cloud", "feelslike_c", "gust_kph",
                        "humidity", "heatindex_c", "is_day", "precip_mm", "pressure_mb", "temp_c", 
                        "time", "time_epoch", "uv", "vis_km", "wind_dir", "wind_kph", "windchill_c", 
                        "city", "country", "forecast_date"
                    ]
                    forecast_df = forecast_df.select(*selected_columns)
                    all_forecast_dfs.append(forecast_df)

                    # Astro data
                    astro_df = spark.read.json(spark.sparkContext.parallelize([astro_data]))

                    # Add city, country, forecast_date, and p_load_dt columns to astro data
                    astro_df = astro_df.withColumn("city", lit(location["name"]))
                    astro_df = astro_df.withColumn("country", lit(location["country"]))
                    astro_df = astro_df.withColumn("forecast_date", lit(forecast_date))
                    astro_df = astro_df.withColumn("p_load_dt", lit(p_load_dt))

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
                print(f"No forecast data found for {location_name}. Skipping forecast processing.")
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
final_forecast_df = all_forecast_dfs[0]
for df in all_forecast_dfs[1:]:
    final_forecast_df = final_forecast_df.union(df)

if v_debug:
    final_forecast_df.show()

save_data(final_forecast_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
