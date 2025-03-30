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
p_table_nm = 'ld_weather_astro'
p_column_nm = 'country'

table_path = 'Tables/dbo/' + p_table_nm
p_debug = 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from deep_translator import GoogleTranslator  # Importing GoogleTranslator from deep-translator
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

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

# Load the table
df = spark.read.format("delta").load(table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Egyedi értékek kigyűjtése a megadott oszlopból
distinct_values_df = df.select(col(p_column_nm)).distinct()

# Eredmény megjelenítése
display(distinct_values_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # Load the table
# df = spark.read.format("delta").load(table_path)

# Define translation logic with mapPartitions
# def translate_partition(rows):
#     texts_to_translate = []
#     row_dicts = []
    
#     for row in rows:
#         try:
#             row_dict = row.asDict()
#             text = row_dict[p_column_nm]
#             texts_to_translate.append(text)
#             row_dicts.append(row_dict)
#         except Exception as e:
#             print(f"Error processing row: {e}")
#             row_dicts.append(row.asDict())  # Fallback to original row

#     # Translate in batch
#     if texts_to_translate:
#         try:
#             translated_texts = GoogleTranslator(source='auto', target=p_target_language_cd).translate_batch(texts_to_translate)
#             for i, translated_text in enumerate(translated_texts):
#                 row_dicts[i][f"{p_column_nm}_{p_target_language_cd.upper()}"] = translated_text
#         except Exception as e:
#             print(f"Batch translation error: {e}")
#             for i in range(len(row_dicts)):
#                 row_dicts[i][f"{p_column_nm}_{p_target_language_cd.upper()}"] = texts_to_translate[i]  # Fallback to original text

#     return iter(row_dicts)

# # Apply translation to the DataFrame
# translated_rdd = distinct_values_df.rdd.mapPartitions(translate_partition)
# translated_df = spark.createDataFrame(translated_rdd)

# if v_debug:
#     translated_df.show(n=1000)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(p_target_language_cd)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Egyedi értékek kigyűjtése a Spark DataFrame-ből
texts_to_translate = [row[p_column_nm] for row in distinct_values_df.collect()]

# 2. Batch fordítás Deep Translatorral
try:
    translated_texts = GoogleTranslator(source='auto', target=p_target_language_cd).translate_batch(texts_to_translate)
except Exception as e:
    print(f"Batch translation error: {e}")
    translated_texts = texts_to_translate  # Fallback eredeti szövegre

# 3. Pandas DataFrame létrehozása a fordításokkal
import pandas as pd
translated_pd_df = pd.DataFrame({
    p_column_nm: texts_to_translate,
    f"{p_column_nm}_{p_target_language_cd.upper()}": translated_texts
})

# 4. Visszaalakítás Spark DataFrame-re
translated_spark_df = spark.createDataFrame(translated_pd_df)

# 3. Debug mód esetén kiírás
if v_debug:
    display(translated_spark_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

p_target_language_cd = p_target_language_cd.upper()
translated_column = f"{p_column_nm}_{p_target_language_cd}"

if v_debug:
    print('p_target_language_cd:', p_target_language_cd)
    print('translated_column:', translated_column)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# TODO: find a solution to fill the translated column with new data instead of remaking it
df = df.drop(translated_column)

# Join df with translated_df on the 'country' column
df = df.join(translated_spark_df, on=p_column_nm, how="left")

if v_debug:
# Show the result to verify
    display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df.write \
#     .format("delta") \
#     .option("mergeSchema", "true") \
#     .partitionBy('m_valid_dt') \
#     .option("partitionOverwriteMode", "dynamic") \
#     .mode("overwrite") \
#     .saveAsTable(p_table_nm)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
