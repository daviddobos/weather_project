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
p_load_dt = ''
p_load_days_no = ''
api_key = "f91f17bc73414f4ba3e123841243011"
location = ""
date = "2024-05-01"

base_url = "http://api.weatherapi.com/v1/history.json"

schema_path = 'Tables/dbo'
measures_table_nm = 'weather_hourly_measures'
astro_table_nm = 'weather_astro'
measures_table_path = schema_path + "/" + measures_table_nm
astro_table_path = schema_path + "/" + astro_table_nm

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from datetime import datetime, timedelta

# Inicialize spark session
spark = SparkSession.builder \
    .appName("WeatherAPI") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define current date
current_date = datetime.now()

# Define target date
target_date = current_date - timedelta(days=int(p_load_days_no))

# Format date
date = target_date.strftime('%Y-%m-%d')

# Cities to request weather for
city_df = spark.read.format("delta").load("Tables/dbo/city")

# Prepare the locations for bulk request dynamically from the city table
locations = [{"q": row.City, "custom_id": f"{row.Country}-{row.County}-{row.City}"} for row in city_df.collect()]

# Function to send request for a batch of cities
def send_bulk_request(locations_batch):
    bulk_request_body = {"locations": locations_batch}
    params = {"key": api_key, "dt": date}
    
    # Send the POST request for the bulk API call
    response = requests.post(base_url + "?q=bulk", params=params, json=bulk_request_body)

    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        print(f"Response JSON for date {date}:")
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
                    hour_df = spark.read.json(spark.sparkContext.parallelize(hour_data))

                    # Add city, country, forecast_date, and p_load_dt columns
                    hour_df = hour_df.withColumn("city", lit(location["name"]))
                    hour_df = hour_df.withColumn("country", lit(location["country"]))
                    hour_df = hour_df.withColumn("forecast_date", lit(forecast_date))
                    hour_df = hour_df.withColumn("p_load_dt", lit(p_load_dt))

                    # Select the necessary columns
                    selected_columns = [
                        "chance_of_rain", "chance_of_snow", "cloud", "feelslike_c", "gust_kph",
                        "humidity", "heatindex_c", "is_day", "precip_mm", "pressure_mb", "temp_c", 
                        "time", "time_epoch", "uv", "vis_km", "wind_dir", "wind_kph", "windchill_c", 
                        "city", "country", "forecast_date", "p_load_dt"
                    ]
                    hour_df = hour_df.select(*selected_columns)

                    # Save to Delta table
                    hour_df.write \
                        .format("delta") \
                        .mode("append") \
                        .option("mergeSchema", "true") \
                        .save(measures_table_path)

                    # Astro data
                    astro_data = [forecast_day["astro"]]
                    astro_df = spark.read.json(spark.sparkContext.parallelize(astro_data))

                    # Add city, country, forecast_date, and p_load_dt columns
                    astro_df = astro_df.withColumn("city", lit(location["name"]))
                    astro_df = astro_df.withColumn("country", lit(location["country"]))
                    astro_df = astro_df.withColumn("forecast_date", lit(forecast_date))
                    astro_df = astro_df.withColumn("p_load_dt", lit(p_load_dt))

                    # Save astro data to Delta table
                    astro_df.write \
                        .format("delta") \
                        .mode("append") \
                        .option("mergeSchema", "true") \
                        .save(astro_table_path)

            except KeyError as e:
                print(f"No forecast data found for {location_data['query']['q']}. Skipping forecast processing.")
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
