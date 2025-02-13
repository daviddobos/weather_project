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
p_load_dt = '20250125'
p_load_days_no = '0'
p_save_path_root = 'Files/landing'
p_source_system_cd = 'weatherapi'
p_table_name = 'weather_measure'

base_url = "http://api.weatherapi.com/v1/history.json"

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

import msal
import requests
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import json
from datetime import datetime, timedelta
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient

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
if not p_load_dt:
    yesterday = datetime.now() - timedelta(days=1) - timedelta(days=int(p_load_days_no))
    v_valid_dt = yesterday.strftime("%Y%m%d")
else:
    v_valid_dt = p_load_dt
    v_valid_dt = datetime.strptime(v_valid_dt, "%Y%m%d") - timedelta(days=int(p_load_days_no))
    v_valid_dt = v_valid_dt.strftime("%Y%m%d")
print('v_valid_dt:', v_valid_dt)

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

all_measure_dfs = []

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

    bulk_request_body = {"locations": locations_batch}
    params = {"key": api_key, "dt": v_m_valid_dt}
    
    # Send the POST request for the bulk API call
    response = requests.post(base_url + "?q=bulk", params=params, json=bulk_request_body)

    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        if v_debug:
            print(f"Response JSON for date {v_m_valid_dt}:")
            print(json.dumps(data, indent=4))

        # Process each location's data
        for location_data in data["bulk"]:
            try:
                location = location_data["query"]["location"]
                if "forecastday" in location_data["query"]["forecast"]:
                    forecast_day = location_data["query"]["forecast"]["forecastday"][0]
                    forecast_date = forecast_day["date"]

                    # Hour data
                    hour_data = forecast_day["hour"]
                    measure_df = spark.read.json(spark.sparkContext.parallelize(hour_data))

                    # Add city, country, forecast_date, and p_load_dt columns
                    measure_df = measure_df.withColumn("city", lit(location["name"]))
                    measure_df = measure_df.withColumn("country", lit(location["country"]))
                    measure_df = measure_df.withColumn("forecast_date", lit(forecast_date))

                    # Select the necessary columns
                    selected_columns = [
                        "chance_of_rain", "chance_of_snow", "cloud", "feelslike_c", "gust_kph",
                        "humidity", "heatindex_c", "is_day", "precip_mm", "pressure_mb", "temp_c", 
                        "time", "time_epoch", "uv", "vis_km", "wind_dir", "wind_kph", "windchill_c", 
                        "city", "country", "forecast_date"
                    ]
                    measure_df = measure_df.select(*selected_columns)

                    all_measure_dfs.append(measure_df)

            except KeyError as e:
                print(f"No history data found for {location_data['query']['q']}. Skipping history processing.")
    else:
        print(f"Error: {response.status_code}, {response.text}")


# Split the locations into batches of 50
batch_size = 50
location_batches = [locations[i:i + batch_size] for i in range(0, len(locations), batch_size)]

# Process each batch
for batch in location_batches:
    send_bulk_request(batch)

# Combine and save dataframes
final_measure_df = all_measure_dfs[0]
for df in all_measure_dfs[1:]:
    final_measure_df = final_measure_df.union(df)

if v_debug:
    final_measure_df.show()

save_data(final_measure_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
