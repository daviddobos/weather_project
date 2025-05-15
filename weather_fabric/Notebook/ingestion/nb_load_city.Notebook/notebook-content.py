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

#Parameters
p_county_nm = 'Békés'
 
source_file_path = 'Files/city.csv'

schema_path = 'Tables/dbo'
target_table_nm = 'city'
target_table_path = schema_path + "/" + target_table_nm
print(target_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create Spark session
spark = SparkSession.builder.appName("SaveFilteredCities").getOrCreate()

# Read csv file
city_lesser_df = spark.read.csv(source_file_path, header=True, inferSchema=True)
city_lesser_df.show()

# Filtering the records
filtered_cities_df = city_lesser_df.filter(col("County") == p_county_nm)

# Save filtered records
filtered_cities_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .save(target_table_path)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
