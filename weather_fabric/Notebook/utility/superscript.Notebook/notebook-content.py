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

# PARAMETERS CELL ********************

p_workspace_id = 'a65228af-1fd8-41a6-8294-aceec3a6ebd5'
p_debug = 1
p_lh_id = ''
p_default_lh_workspace_id = ''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import notebookutils
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

v_notebook_names = []

try:
    notebooks = notebookutils.notebook.list(p_workspace_id)
    for nb in notebooks:
        if nb.get("type") == "Notebook":
            v_notebook_names.append(nb["displayName"])
except Exception as e:
    print(f"Exception: {e}")


if "nb_update_default_lh" in v_notebook_names:
    v_notebook_names.remove("nb_update_default_lh")

if p_debug:
    for name in v_notebook_names:
        print(name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for nb_name in v_notebook_names:

    notebookutils.notebook.updateDefinition(
        name=nb_name,
        workspaceId=p_workspace_id,
        defaultLakehouse=p_lh_id,
        defaultLakehouseWorkspace=p_default_lh_workspace_id
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
