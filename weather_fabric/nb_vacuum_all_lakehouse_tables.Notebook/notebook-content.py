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

# Replace 'your_database_name' with the actual database name
p_database_name = 'lh_weather'
p_schema_name = 'dbo'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tables_df = spark.sql(f"SHOW TABLES IN {p_database_name}.{p_schema_name}")
tables_df.show()

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

# Function to perform VACUUM on a table
def vacuum_table(database, schema, table_name):
    print(f"Running VACUUM on {database}.{schema}.{table_name}")
    spark.sql(f"VACUUM {database}.{schema}.{table_name} RETAIN 168 HOURS")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get all tables in the lakehouse
tables = get_all_tables(p_database_name, p_schema_name)

# VACUUM each table
for table in tables:
    vacuum_table(p_database_name, p_schema_name, table)

print("VACUUM completed for all tables.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
