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
# META     },
# META     "environment": {
# META       "environmentId": "516c9a07-f6e6-4247-b48e-18f7c6bfe52d",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

p_target_language_cd = 'en'
p_table_nm = 'weather_astro'
p_column_nm = 'country'

table_path = "Tables/dbo/" + p_table_nm

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# from pyspark.sql import SparkSession
# from deep_translator import GoogleTranslator  # Importing GoogleTranslator from deep-translator
# from pyspark.sql.functions import col
# from pyspark.sql.types import StringType

# # Initialize Spark session
# spark = SparkSession.builder.appName("TranslateColumn").getOrCreate()

# # Load the table
# df = spark.read.format("delta").load(table_path)

# # Define translation logic with mapPartitions
# def translate_partition(rows):
#     for row in rows:
#         try:
#             row_dict = row.asDict()
#             text = row_dict[p_column_nm]
#             # Using deep-translator to translate the text
#             translated_text = GoogleTranslator(source='auto', target=p_target_language_cd).translate(text)
#             row_dict[f"{p_column_nm}_{p_target_language_cd.upper()}"] = translated_text
#         except Exception as e:
#             print(f"Translation error for text '{text}': {e}")
#             row_dict[f"{p_column_nm}_{p_target_language_cd.upper()}"] = text  # Fallback to original text
#         yield row_dict

# # Apply translation to the DataFrame
# translated_rdd = df.rdd.mapPartitions(translate_partition)
# translated_df = spark.createDataFrame(translated_rdd)

# # Show the updated DataFrame
# translated_df.show(n=1000)

# # Overwrite the table with the translations
# translated_df.write \
#     .format("delta") \
#     .mode("overwrite") \
#     .partitionBy('time') \
#     .option("overwriteSchema", "true") \
#     .save(table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from deep_translator import GoogleTranslator  # Importing GoogleTranslator from deep-translator
from pyspark.sql.functions import col
from pyspark.sql.types import StringType

# Initialize Spark session
spark = SparkSession.builder.appName("TranslateColumn").getOrCreate()

# Load the table
df = spark.read.format("delta").load(table_path)

# Define translation logic with mapPartitions
def translate_partition(rows):
    texts_to_translate = []
    row_dicts = []
    
    for row in rows:
        try:
            row_dict = row.asDict()
            text = row_dict[p_column_nm]
            texts_to_translate.append(text)
            row_dicts.append(row_dict)
        except Exception as e:
            print(f"Error processing row: {e}")
            row_dicts.append(row.asDict())  # Fallback to original row

    # Translate in batch
    if texts_to_translate:
        try:
            translated_texts = GoogleTranslator(source='auto', target=p_target_language_cd).translate_batch(texts_to_translate)
            for i, translated_text in enumerate(translated_texts):
                row_dicts[i][f"{p_column_nm}_{p_target_language_cd.upper()}"] = translated_text
        except Exception as e:
            print(f"Batch translation error: {e}")
            for i in range(len(row_dicts)):
                row_dicts[i][f"{p_column_nm}_{p_target_language_cd.upper()}"] = texts_to_translate[i]  # Fallback to original text

    return iter(row_dicts)

# Apply translation to the DataFrame
translated_rdd = df.rdd.mapPartitions(translate_partition)
translated_df = spark.createDataFrame(translated_rdd)

# Show the updated DataFrame
translated_df.show(n=1000)

# Overwrite the table with the translations
translated_df.write \
    .format("delta") \
    .option("mergeSchema", "true") \
    .partitionBy('forecast_date') \
    .option("partitionOverwriteMode", "dynamic") \
    .mode("overwrite") \
    .save(table_path)

# Stop the Spark session
spark.stop()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
