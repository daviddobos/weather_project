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

# Azure Entra ID credentials
p_load_date = '20250515'
p_keyvault = 'https://dd-thesis-keyvault.vault.azure.net/'
p_secret_name_tenant = 'dd-thesis-sp-tenant-id'
p_secret_name_client = 'dd-thesis-sp-client-id'
p_secret_name_secret = 'dd-thesis-sp-secret'
p_save_path_root = 'Files/landing'
p_source_system_cd = 'AZ'
p_table_name = 'sec_group_members'
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
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#check for parameter date, if there is none use yesterday's date
if not p_load_date:
    yesterday = datetime.now() - timedelta(days=1)
    v_valid_dt = yesterday.strftime("%Y%m%d")
else:
    v_valid_dt = p_load_date

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

# Constants
TENANT_ID = notebookutils.credentials.getSecret(str(p_keyvault), str(p_secret_name_tenant))
CLIENT_ID = notebookutils.credentials.getSecret(str(p_keyvault), str(p_secret_name_client))
CLIENT_SECRET = notebookutils.credentials.getSecret(str(p_keyvault), str(p_secret_name_secret))
GRAPH_ENDPOINT = "https://graph.microsoft.com/v1.0"  # Microsoft Graph API endpoint

# Authenticate using MSAL
def get_access_token():
    authority = f"https://login.microsoftonline.com/{TENANT_ID}"
    app = msal.ConfidentialClientApplication(CLIENT_ID, authority=authority, client_credential=CLIENT_SECRET)
    scopes = ["https://graph.microsoft.com/.default"]
    token_response = app.acquire_token_for_client(scopes=scopes)
    if "access_token" in token_response:
        return token_response["access_token"]
    else:
        raise Exception("Could not acquire access token.")

# Query security groups and load into DataFrame
def query_security_groups_to_dataframe(access_token):
    headers = {"Authorization": f"Bearer {access_token}"}
    query = "startswith(displayName, 'sg-dd-thesis')"  
    url = f"{GRAPH_ENDPOINT}/groups?$filter={query}"
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        groups = response.json().get("value", [])
        data = [{"group_id": group["id"], "group_display_name": group["displayName"]} for group in groups]
        df = pd.DataFrame(data)
        return df
    else:
        raise Exception(f"Error: {response.status_code} - {response.text}")

# Query members for each security group
def query_security_group_members_to_dataframe(access_token, groups_dataframe):
    headers = {"Authorization": f"Bearer {access_token}"}
    all_members = []

    for _, group in groups_dataframe.iterrows():
        group_id = group["group_id"]
        group_display_name = group["group_display_name"]
        url = f"{GRAPH_ENDPOINT}/groups/{group_id}/members"
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            members = response.json().get("value", [])
            for member in members:
                all_members.append({
                    "group_id": group_id,
                    "group_display_name": group_display_name,
                    "member_id": member.get("id"),
                    "member_display_name": member.get("displayName", None),
                    "mail": member.get("mail", None),
                })
        else:
            print(f"Error fetching members for group {group_display_name} ({group_id}): {response.status_code} - {response.text}")

    members_df = pd.DataFrame(all_members)
    return members_df

# Save to or update the table
def save_membership_data(members_df):
    # Add today's date to the DataFrame
    members_df["m_valid_dt"] = v_m_valid_dt
    
    # Convert Pandas DataFrame to Spark DataFrame
    spark_members_df = spark.createDataFrame(members_df)

    # Create m_updated_at_dttm
    spark_extracted_df = spark_members_df \
                            .withColumn("m_extracted_at_dttm", from_utc_timestamp(current_timestamp(), "Europe/Budapest")) \
                            .withColumn("m_updated_at_dttm", from_utc_timestamp(current_timestamp(), "Europe/Budapest"))

    # Output
    spark_extracted_df.write.format("parquet").mode("overwrite").save(v_file_path)
    if v_debug:
        display(spark_extracted_df)

try:
    token = get_access_token()
    groups_df = query_security_groups_to_dataframe(token)
    if v_debug:
        print("Queried Groups:")
        print(groups_df)

    members_df = query_security_group_members_to_dataframe(token, groups_df)
    if v_debug:
        print("Group Members:")
        print(members_df)

    # Save or update the table
    save_membership_data(members_df)
except Exception as e:
    print(f"Error: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
