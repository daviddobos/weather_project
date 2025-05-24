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
# META       "default_lakehouse_workspace_id": "a65228af-1fd8-41a6-8294-aceec3a6ebd5",
# META       "known_lakehouses": [
# META         {
# META           "id": "8c3179ca-6f42-4378-8b2b-7a2e62924e9a"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

weather_report_flag = 1
dq_report_flag = 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Import necessary modules
import sempy_labs as labs
import sempy.fabric as fabric

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define dataset names
direct_lake_dataset_name = 'sm_weather'
data_quality_dataset_name = 'sm_data_quality'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get the corresponding reports workspace name
ws_env = fabric.resolve_workspace_name()
print(ws_env)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if weather_report_flag == 1:
    labs.refresh_semantic_model(dataset=direct_lake_dataset_name, workspace= ws_env, refresh_type="automatic")


if dq_report_flag == 1:
    labs.refresh_semantic_model(dataset=data_quality_dataset_name, workspace= ws_env, refresh_type="automatic")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
