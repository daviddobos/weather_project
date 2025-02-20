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

p_email_sender = 'dobosdavid.mail@gmail.com'
p_email_receiver = 'david.dobos@dezerius.com'

p_keyvault_url = 'https://dd-thesis-keyvault.vault.azure.net/'
p_secret_name_tenant = 'dd-thesis-sp-tenant-id'
p_secret_name_client = 'dd-thesis-sp-client-id'
p_secret_name_secret = 'dd-thesis-sp-secret'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from msgraph import GraphServiceClient
from msgraph.generated.users.item.send_mail.send_mail_post_request_body import SendMailPostRequestBody
from msgraph.generated.models.message import Message
from msgraph.generated.models.item_body import ItemBody
from msgraph.generated.models.body_type import BodyType
from msgraph.generated.models.recipient import Recipient
from msgraph.generated.models.email_address import EmailAddress
from msgraph.generated.models.internet_message_header import InternetMessageHeader
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
import asyncio 
import msal

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fetch the credentials using the service principal
tenant_id = notebookutils.credentials.getSecret(p_keyvault_url, p_secret_name_tenant)
client_id = notebookutils.credentials.getSecret(p_keyvault_url, p_secret_name_client)
client_secret = notebookutils.credentials.getSecret(p_keyvault_url, p_secret_name_secret)

# Create a credential using the service principal
credential = ClientSecretCredential(tenant_id, client_id, client_secret)

# Initialize SecretClient with Key Vault URL and the credentials
client = SecretClient(vault_url=p_keyvault_url, credential=credential)

# Authenticate using ClientSecretCredential
credentials = ClientSecretCredential(tenant_id, client_id, client_secret)
graph_client = GraphServiceClient(credentials)



# Authenticate using MSAL
def get_access_token():
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    app = msal.ConfidentialClientApplication(client_id, authority=authority, client_credential=client_secret)
    scopes = ["https://graph.microsoft.com/.default"]
    token_response = app.acquire_token_for_client(scopes=scopes)
    if "access_token" in token_response:
        return token_response["access_token"]
    else:
        raise Exception("Could not acquire access token.")

get_access_token()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Query security groups and load into DataFrame
def query_security_groups_to_dataframe(access_token):
    headers = {"Authorization": f"Bearer {access_token}"}
    query = "startswith(displayName, 'SEC-')"  # Query groups whose display name starts with 'SEC-'
    url = f"https://graph.microsoft.com/v1.0/users/dd-thesis-sp/sendMail"
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        groups = response.json().get("value", [])
        data = [{"group_id": group["id"], "group_display_name": group["displayName"]} for group in groups]
        df = pd.DataFrame(data)
        return df
    else:
        raise Exception(f"Error: {response.status_code} - {response.text}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Construct the email
request_body = SendMailPostRequestBody(
    message=Message(
        subject="Pipeline Run Status",
        body=ItemBody(
            content_type=BodyType.Html,
            content="<p>The pipeline has successfully completed.</p>",
        ),
        from_=EmailAddress(address=p_email_sender),  # Specify the sender (shared mailbox)
        to_recipients=[
            Recipient(
                email_address=EmailAddress(
                    address=p_email_receiver,
                ),
            ),
        ],
        internet_message_headers=[
            InternetMessageHeader(name="x-custom-header-group-name", value="AutomatedPipeline"),
            InternetMessageHeader(name="x-custom-header-group-id", value="Pipeline001"),
        ],
    )
)

# Send email from shared mailbox
async def send_email():
    # Use get_by_id() to get user details for shared mailbox
    user = await graph_client.users.get_by_id(p_email_sender).get()
    # Send the email from the shared mailbox
    await graph_client.users[user.id].send_mail(request_body)
    print("Email sent successfully!")

# Run the function
await send_email()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # Construct the email
# request_body = SendMailPostRequestBody(
#     message=Message(
#         subject="Pipeline Run Status",
#         body=ItemBody(
#             content_type=BodyType.Html,
#             content="<p>The pipeline has successfully completed.</p>",
#         ),
#         to_recipients=[
#             Recipient(
#                 email_address=EmailAddress(
#                     address=p_email_receiver,
#                 ),
#             ),
#         ],
#         internet_message_headers=[
#             InternetMessageHeader(name="x-custom-header-group-name", value="AutomatedPipeline"),
#             InternetMessageHeader(name="x-custom-header-group-id", value="Pipeline001"),
#         ],
#     )
# )

# # Send email
# await graph_client.me.send_mail.post(request_body)
# print("Email sent successfully!")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

!pip install mailersend

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

api_key = "mlsn.7df756c683ed26a020c84c59b9c13c6e39a4195b6f8b25027cf4e3fb1faf4bd3"

from mailersend import emails

# assigning NewEmail() without params defaults to MAILERSEND_API_KEY env var
mailer = emails.NewEmail(api_key)

# define an empty dict to populate with mail values
mail_body = {}

mail_from = {
    "name": "Your Name",
    "email": p_email_sender,
}

recipients = [
    {
        "name": "Your Client",
        "email": p_email_receiver,
    }
]

mailer.set_mail_from(mail_from, mail_body)
mailer.set_mail_to(recipients, mail_body)
mailer.set_subject("Hello!", mail_body)
mailer.set_html_content("This is the HTML content", mail_body)
mailer.set_plaintext_content("This is the text content", mail_body)

# using print() will also return status code and data
mailer.send(mail_body)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
from azure.identity import ClientSecretCredential

p_keyvault_url = 'https://dd-thesis-keyvault.vault.azure.net/'
p_secret_name_tenant = 'dd-thesis-sp-tenant-id'
p_secret_name_client = 'dd-thesis-sp-client-id'
p_secret_name_secret = 'dd-thesis-sp-secret'

# Define your Azure AD app credentials
TENANT_ID = notebookutils.credentials.getSecret(str(p_keyvault_url), str(p_secret_name_tenant))
CLIENT_ID = notebookutils.credentials.getSecret(str(p_keyvault_url), str(p_secret_name_client))
CLIENT_SECRET = notebookutils.credentials.getSecret(str(p_keyvault_url), str(p_secret_name_secret))
GRAPH_ENDPOINT = "https://graph.microsoft.com/v1.0"  # Microsoft Graph API endpoint

# Define email sender and recipient
SENDER_EMAIL = "david.dobos@dezerius.com"  # Use a shared mailbox or licensed user
RECIPIENT_EMAIL = "dobosdavid.mail@gmail.com"

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

# Email body payload
email_data = {
    "message": {
        "subject": "Test Email from Service Principal",
        "body": {
            "contentType": "HTML",
            "content": "<p>Hello, this is a test email sent from a service principal.</p>"
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
