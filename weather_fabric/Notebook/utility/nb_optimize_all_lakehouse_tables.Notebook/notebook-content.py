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

p_database_name = 'lh_weather'
p_schema_name = 'dbo'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Function to get all the tables in the lakehouse
def get_all_tables(database, schema):
    tables_df = spark.sql(f"SHOW TABLES IN {database}.{schema}")
    return tables_df.select("tableName").rdd.flatMap(lambda x: x).collect()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Function to perform OPTIMIZE on a table
def optimize_table(database, schema, table_name):
    print(f"Running OPTIMIZE on {database}.{schema}.{table_name}")
    spark.sql(f"OPTIMIZE {database}.{schema}.{table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get all tables in the lakehouse
tables = get_all_tables(p_database_name, p_schema_name)

# OPTIMIZE each table
for table in tables:
    optimize_table(p_database_name, p_schema_name, table)

print("OPTIMIZE completed for all tables.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
