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

p_keyvault_url = 'https://dd-thesis-keyvault.vault.azure.net/'
p_secret_name_tenant = 'dd-thesis-sp-tenant-id'
p_secret_name_client = 'dd-thesis-sp-client-id'
p_secret_name_secret = 'dd-thesis-sp-secret'
p_load_success_flg = 0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import msal
from azure.identity import ClientSecretCredential

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define your Azure AD app credentials
TENANT_ID = notebookutils.credentials.getSecret(str(p_keyvault_url), str(p_secret_name_tenant))
CLIENT_ID = notebookutils.credentials.getSecret(str(p_keyvault_url), str(p_secret_name_client))
CLIENT_SECRET = notebookutils.credentials.getSecret(str(p_keyvault_url), str(p_secret_name_secret))
GRAPH_ENDPOINT = "https://graph.microsoft.com/v1.0"  # Microsoft Graph API endpoint

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define email sender and recipient
SENDER_EMAIL = "david.dobos@dezerius.com"  # Use a shared mailbox or licensed user
RECIPIENT_EMAIL = "dobosdavid.mail@gmail.com"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#message based on load success flag
if p_load_success_flg:
    email_content = "<p>✅ Successful Load! Data processing was completed successfully.</p>"
    subject = "Successful Data Load"
else:
    email_content = "<p>❌ Error Occurred! Data processing has failed.</p>"
    subject = "Data Load Failure"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Email body payload
email_data = {
    "message": {
        "subject": subject,
        "body": {
            "contentType": "HTML",
            "content": email_content
        },
        "toRecipients": [
            {
                "emailAddress": {
                    "address": RECIPIENT_EMAIL
                }
            }
        ]
    }
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

authority = f"https://login.microsoftonline.com/{TENANT_ID}"
app = msal.ConfidentialClientApplication(CLIENT_ID, authority=authority, client_credential=CLIENT_SECRET)
scopes = ["https://graph.microsoft.com/.default"]
token_response = app.acquire_token_for_client(scopes=scopes)
if "access_token" in token_response:
    access_token = token_response["access_token"]
else:
    raise Exception("Could not acquire access token.")


# Microsoft Graph API endpoint to send an email
url = f"https://graph.microsoft.com/v1.0/users/{SENDER_EMAIL}/sendMail"


# HTTP Headers
headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}

# Send the request
response = requests.post(url, headers=headers, json=email_data)

# Check the response
if response.status_code == 202:
    print("✅ Email sent successfully!")
else:
    print(f"❌ Failed to send email: {response.status_code}, {response.text}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
