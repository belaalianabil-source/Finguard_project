# Databricks notebook source
# =====================================================
# 1. DEFINE CONNECTION VARIABLES
# =====================================================

# Name of the secret scope where we will store sensitive information.
# A secret scope is like a secure container for API keys and passwords.
secret_scope_name = "finguard-scope"

# COMMAND ----------

# =====================================================
# 2. GET DATABRICKS WORKSPACE CONTEXT
# =====================================================

# Get information about the current Databricks notebook environment.
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()

# Get the URL of the Databricks workspace.
# This URL is used to send requests to the Databricks REST API.
api_url = ctx.apiUrl().getOrElse(None)

# Try to get the API token from the current notebook context.
# The token is used to authenticate requests to Databricks.
api_token = ctx.apiToken().getOrElse(None)

# Check that both values are available before continuing.
if not api_url or not api_token:
    raise RuntimeError("Databricks URL or API token is unavailable.")

# Print only the workspace URL, never the secret token.
print("Databricks workspace URL retrieved successfully.")

# COMMAND ----------

# =====================================================
# 3. IMPORT LIBRARIES AND CONFIGURE THE CONNECTION
# =====================================================

# requests allows Python to send HTTP requests to an API.
import requests

# json allows Python to convert dictionaries into JSON strings
# and JSON strings back into Python dictionaries.
import json

# Save the Databricks workspace URL.
DATABRICKS_INSTANCE = api_url.rstrip("/")

# Save the token used to authenticate API requests.
DATABRICKS_TOKEN = api_token

# Specify the name of the secret scope.
scope_name = secret_scope_name

# Specify the intended secret storage backend.
# This variable is descriptive here; it is not used by the API calls below.
backend_type = "DATABRICKS"

# Prepare the HTTP headers used for authentication and JSON requests.
headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

# COMMAND ----------

# =====================================================
# 4. CREATE THE SECRET SCOPE
# =====================================================

# Define the Databricks API endpoint for creating secret scopes.
url = f"{DATABRICKS_INSTANCE}/api/2.0/secrets/scopes/create"

# Prepare the request body containing the scope name.
payload = {
    "scope": scope_name
}

# Send a POST request to Databricks.
# json=payload automatically converts the dictionary to JSON.
response = requests.post(
    url,
    headers=headers,
    json=payload,
    timeout=30
)

# Check whether the request succeeded.
if response.status_code == 200:
    print(f"Secret scope '{scope_name}' created successfully.")

else:
    # Display the status and error details if creation fails.
    # The scope might already exist, or your account might lack permission.
    print("Secret scope creation failed.")
    print("Status code:", response.status_code)
    print("Response:", response.text)

# COMMAND ----------

# =====================================================
# 5. DEFINE KAFKA CONNECTION VARIABLES
# =====================================================

# Bootstrap server address provided by Confluent Cloud.
kafka_bootstrap_servers = (
    "pkc-619z3.us-east1.gcp.confluent.cloud:9092"
)

# Name of the Kafka topic containing credit card transactions.
kafka_topic = "credit_card_transactions"

# Kafka API key used for authentication.
# Replace the placeholder with your newly rotated key.
kafka_api_key = "WI7A22YEEPEPVUUP"

# Kafka API secret used for authentication.
# Replace the placeholder with your newly rotated secret.
kafka_api_secret = "cfltSvcKdn+wvc9V2gAvYX4n+xBaV6+VfUxo74AlJLXF3fpCkkfCu+gibP2dxA6w"

# Combine all Kafka connection details into one JSON string.
# This allows us to store the configuration as a single Databricks secret.
kafka_connection_details = json.dumps({
    "bootstrap_servers": kafka_bootstrap_servers,
    "topic": kafka_topic,
    "api_key": kafka_api_key,
    "api_secret": kafka_api_secret
})

# Never print kafka_connection_details because it contains credentials.
print("Kafka connection configuration prepared.")

# COMMAND ----------

# =====================================================
# 6. STORE KAFKA CONNECTION DETAILS
# =====================================================

# Specify the secret scope that will contain the secret.
scope = secret_scope_name

# Define the name used to retrieve this secret later.
secret_key = "kafka_connection_details"

# Set the secret value to the JSON configuration created above.
secret_value = kafka_connection_details

# Define the Databricks API endpoint for storing a secret.
url = f"{DATABRICKS_INSTANCE}/api/2.0/secrets/put"

# Prepare the request body.
payload = {
    "scope": scope,
    "key": secret_key,
    "string_value": secret_value
}

# Send the secret to Databricks using the REST API.
response = requests.post(
    url,
    headers=headers,
    json=payload,
    timeout=30
)

# Check whether Databricks stored the secret successfully.
if response.status_code == 200:
    print(f"Secret '{secret_key}' stored successfully.")

else:
    # Display the API error if the request fails.
    print("Failed to store the secret.")
    print("Status code:", response.status_code)
    print("Response:", response.text)

# COMMAND ----------

# =====================================================
# 7. VERIFY SECRET RETRIEVAL
# =====================================================

try:
    # Retrieve the stored secret from the specified scope.
    # Databricks returns the secret as a string.
    retrieved_json = dbutils.secrets.get(
        scope=secret_scope_name,
        key="szvr squy gknk bjku"
    )

    # Convert the JSON string back into a Python dictionary.
    parsed = json.loads(retrieved_json)

    # Check that all required configuration fields exist.
    required_keys = [
        "bootstrap_servers",
        "topic",
        "api_key",
        "api_secret"
    ]

    assert all(key in parsed for key in required_keys)

    # Confirm success without displaying any credentials.
    print("Secret retrieved and verified successfully.")

    # Display only the non-sensitive topic name.
    print("Kafka topic:", parsed["topic"])

except Exception as e:
    # Report the error without printing the secret itself.
    print("Secret verification failed.")
    print("Error type:", type(e).__name__)

# COMMAND ----------


import requests
import json

scope = secret_scope_name          # Already existing scope
secret_key = 'gmail_api_key'       # Name of the secret entry
secret_value = 'szvr squy gknk bjku' # Value to store securely

# -------------------------------------------------
# API Endpoint
# -------------------------------------------------
url = f"{DATABRICKS_INSTANCE}/api/2.0/secrets/put"

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

payload = {
    "scope": scope,
    "key": secret_key,
    "string_value": secret_value
}

# -------------------------------------------------
# Send Request
# -------------------------------------------------
response = requests.post(url, headers=headers, data=json.dumps(payload))

# -------------------------------------------------
# Output
# -------------------------------------------------
if response.status_code == 200:
    print(f"Secret '{secret_key}' created successfully in scope '{scope}'.")
else:
    print("Failed to create secret.")
    print("Status:", response.status_code)
    print("Response:", response.text)