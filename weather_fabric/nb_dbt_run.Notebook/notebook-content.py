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

# CELL ********************

# MAGIC %%configure -f
# MAGIC { 
# MAGIC             "defaultLakehouse": {
# MAGIC                 "name": 'lh_weather'
# MAGIC             }
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
import os
import re
import time
import sempy.fabric as fabric
import requests
from git import Repo
from pyspark.sql.functions import *
import subprocess

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

dbt_repo_dir = 'weather_dbt'
dbt_profile = 'weather_dbt'
target_warehouse = 'wh_weather'
log_lakehouse = 'lh_weather'
host = 'xe6b73af6wce5cpkwv3dpk3vzi-v4uffjwyd6tedauuvtxmhjxl2u.datawarehouse.fabric.microsoft.com'
schema = 'dbt'
threads = '4'
max_retries = 5    #leave it as int for the while loop later in the code
retry_error_pattern = re.escape("Error code 0x68 (104) (SQLExecDirectW)")
keyvault = 'https://dd-thesis-keyvault.vault.azure.net/'
secret_name_tenant = 'dd-thesis-sp-tenant-id'
secret_name_client = 'dd-thesis-sp-client-id'
secret_name_secret = 'dd-thesis-sp-secret'
secret_name_git_pat = 'dd-thesis-github-token'
git_provider = 'github' # the variable value should be 'github' in case the repo is there, in case of any other value the script will use Azure DevOps

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# get current workspace id
workspace_id = spark.conf.get("trident.workspace.id")

# using Fabric Rest Client to authenticate make API calls
client = fabric.FabricRestClient()


# GET personal access token for github texhnical user
git_pat = notebookutils.credentials.getSecret(str(keyvault),str(secret_name_git_pat))
print(git_pat)
# quick and dirty, will have to make changes to pull all the repo information from the workspace like what we have for ADO
repo = "weather_project"
repo_URL = f"https://oauth2:{git_pat}@github.com/daviddobos/"
repo_path = repo_URL + repo + ".git"
ws_env = mssparkutils.env.getWorkspaceName()
if "feature" in ws_env:
    branch = ws_env
else:
    branch = ws_env[len(ws_env) - 3:]
print(branch)
print(repo_URL)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Repo.clone_from(repo_path, f"{repo}", branch=branch)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tenant = notebookutils.credentials.getSecret(str(keyvault),str(secret_name_tenant))
client = notebookutils.credentials.getSecret(str(keyvault),str(secret_name_client))
secret = notebookutils.credentials.getSecret(str(keyvault),str(secret_name_secret))

file_path = f"{repo}""/"f"{dbt_repo_dir}""/profiles.yml"
print(file_path)
if os.path.isfile(file_path):

    os.remove(f"{file_path}")

fp = open(f"{file_path}", "a")

content = ""f"{dbt_profile}"":\n  target: "f"{branch}""\n  outputs:    \n    "f"{branch}"":\n      authentication: serviceprincipal\n      tenant_id: "f"{tenant}""\n      client_id: "f"{client}""\n      client_secret: "f"{secret}""\n      database: "f"{target_warehouse}""\n      host: "f"{host}""\n      driver: ODBC Driver 18 for SQL Server\n      schema: "f"{schema}""\n      threads: "f"{threads}""\n      retries: "f"{max_retries}""\n      type: fabric"

fp.write(f"{content}")
fp.close()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

os.chdir(f"{repo}""/"f"{dbt_repo_dir}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# get lakehouse abfspath
lakehouse_abfs_path = notebookutils.lakehouse.get(f'{log_lakehouse}')['properties']['abfsPath']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%capture cap_debug
# MAGIC 
# MAGIC !dbt debug

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

current_date_debug = str(spark.range(1).select(date_format(from_utc_timestamp(current_timestamp(), "Europe/Budapest"),"yyyy/MM/dd")).head()[0])

current_time_debug = str(spark.range(1).select(date_format(from_utc_timestamp(current_timestamp(), "Europe/Budapest"),"yyyyMMddHHmmss")).head()[0])

debug_file_path = lakehouse_abfs_path + "/Files/dbt_logs/debug/" + current_date_debug + "/dbt_debug_output_" + current_time_debug + ".txt"

content_debug = (cap_debug.stdout.replace("\x1b[0m", "")).replace("\x1b[32m", "")

notebookutils.fs.put(debug_file_path, content_debug, True)

debug_retry_count = 0

while debug_retry_count < max_retries:
    if re.search("error", content_debug.lower()):
        print(f"Error found, retrying dbt debug... Attempt {debug_retry_count + 1}")
                
        # Wait for 3 seconds before retrying
        time.sleep(3)
        
        # Execute dbt retry
        debug_result = subprocess.run(["dbt", "debug"], capture_output=True, text=True)
        content_debug = (((debug_result.stdout.replace("\x1b[0m", "")).replace("\x1b[31m", "")).replace("\x1b[32m", "")).replace("\x1b[33m", "")
        
        # Create a retry log file path
        retry_file_path = debug_file_path.replace(".txt", "_retry" + str(debug_retry_count + 1) + ".txt")
        
        # Log the retry output
        notebookutils.fs.put(retry_file_path, content_debug, True)
        
        debug_retry_count += 1
    else:
        break

if debug_retry_count == max_retries:
    print("Max retries reached. Exiting.")
    raise Exception("dbt debug threw an error! Please check dbt debug log in " + log_lakehouse + "/" + retry_file_path)
elif re.search("error", content_debug.lower()):
    raise Exception("dbt debug threw an error! Please check dbt debug log in " + log_lakehouse + "/" + debug_file_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

!dbt deps

!dbt seed

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%capture cap_run
# MAGIC 
# MAGIC !dbt run #--exclude "040_dq" "050_dq_rpt"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

current_date_run = str(spark.range(1).select(date_format(from_utc_timestamp(current_timestamp(), "Europe/Budapest"),"yyyy/MM/dd")).head()[0])

current_time_run = str(spark.range(1).select(date_format(from_utc_timestamp(current_timestamp(), "Europe/Budapest"),"yyyyMMddHHmmss")).head()[0])

run_file_path = lakehouse_abfs_path + "/Files/dbt_logs/run/" + current_date_run + "/dbt_run_output_" + current_time_run + ".txt"

content_run = (((cap_run.stdout.replace("\x1b[0m", "")).replace("\x1b[31m", "")).replace("\x1b[32m", "")).replace("\x1b[33m", "")

notebookutils.fs.put(run_file_path, content_run, True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

retry_count = 0

while retry_count < max_retries:
    if re.search(retry_error_pattern, content_run) is not None:
        print(f"Error found, retrying dbt run... Attempt {retry_count + 1}")
        
        # Execute dbt retry
        result = subprocess.run(["dbt", "retry"], capture_output=True, text=True)
        content_run = (((result.stdout.replace("\x1b[0m", "")).replace("\x1b[31m", "")).replace("\x1b[32m", "")).replace("\x1b[33m", "")
        
        # Create a retry log file path
        retry_file_path = run_file_path.replace(".txt", "_retry" + str(retry_count + 1) + ".txt")
        
        # Log the retry output
        notebookutils.fs.put(retry_file_path, content_run, True)
        
        retry_count += 1
    else:
        break

if retry_count == max_retries:
    print("Max retries reached. Exiting.")
    raise Exception("dbt run threw an error! Please check dbt run log in " + log_lakehouse + "/" + retry_file_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if re.search(retry_error_pattern, content_run.lower()) is not None:
    raise Exception("dbt run encountered 'Error code 0x68 (104) (SQLExecDirectW)'. Please check the retry log in " + retry_file_path)
elif re.search("error=0", content_run.lower()) is None:
    raise Exception("dbt run threw an error! Please check dbt run log in " + log_lakehouse + "/" + run_file_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
