# Databricks notebook source
# DBTITLE 1,REST API Ingestion with OAuth & Pagination
# MAGIC %md
# MAGIC # REST API Ingestion: OAuth Authentication + Pagination + Retry Logic
# MAGIC
# MAGIC FREE EDITION COMPATIBLE
# MAGIC
# MAGIC Purpose: Ingest data from external REST APIs into bronze layer with production-grade error handling
# MAGIC
# MAGIC Key Features:
# MAGIC - OAuth 2.0 client credentials flow
# MAGIC - Automatic token refresh
# MAGIC - Cursor-based and offset-based pagination
# MAGIC - Exponential backoff retry logic
# MAGIC - Request/response logging
# MAGIC - Rate limit handling
# MAGIC
# MAGIC Use Cases:
# MAGIC - CRM data sync (Salesforce, HubSpot)
# MAGIC - Payment gateway transactions (Stripe, Razorpay)
# MAGIC - Marketing platforms (Google Ads, Facebook Ads)
# MAGIC - Custom internal APIs
# MAGIC
# MAGIC Cost: Standard compute only (no special features), typically 0.5-2 DBU per run

# COMMAND ----------

# DBTITLE 1,Config Import & Library Setup
# MAGIC %run ../../1_Canvas/1_Setup/1_config

# COMMAND ----------

# DBTITLE 1,Install Required Libraries
# MAGIC %pip install requests requests-oauthlib --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,🚀 SETUP STEP 1: Configure Databricks Secrets
# MAGIC %md
# MAGIC ## 🚀 **LIVE SETUP - EXECUTE THESE STEPS FIRST**
# MAGIC
# MAGIC ### **STEP 1: Configure Databricks Secrets** (5 min)
# MAGIC
# MAGIC **Why?** Never hardcode API credentials. Secrets provide encrypted storage + access control.
# MAGIC
# MAGIC **Quick Setup (Databricks CLI):**
# MAGIC ```bash
# MAGIC # Install CLI
# MAGIC pip install databricks-cli
# MAGIC
# MAGIC # Configure
# MAGIC databricks configure --token
# MAGIC
# MAGIC # Create scope
# MAGIC databricks secrets create-scope --scope api_credentials
# MAGIC
# MAGIC # Add credentials
# MAGIC databricks secrets put --scope api_credentials --key salesforce_client_id
# MAGIC databricks secrets put --scope api_credentials --key salesforce_client_secret
# MAGIC databricks secrets put --scope api_credentials --key salesforce_token_url
# MAGIC databricks secrets put --scope api_credentials --key salesforce_api_base_url
# MAGIC ```
# MAGIC
# MAGIC **Common APIs:**
# MAGIC - **Salesforce:** token_url = `https://login.salesforce.com/services/oauth2/token`
# MAGIC - **HubSpot:** token_url = `https://api.hubapi.com/oauth/v1/token`
# MAGIC - **Stripe:** Just API key, no OAuth
# MAGIC
# MAGIC ✅ **Verify:** Run next cell to test

# COMMAND ----------

# DBTITLE 1,🧪 STEP 2: Test Secrets
# Test secrets access
try:
    test = dbutils.secrets.get(scope="api_credentials", key="salesforce_client_id")
    print("✅ Secrets configured! Ready to proceed")
except:
    print("❌ Secrets not found. Follow STEP 1 above")
    print("Quick fix: databricks secrets create-scope --scope api_credentials")

# COMMAND ----------

# DBTITLE 1,Import Libraries & Setup
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import json
import time
from datetime import datetime, timedelta
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import current_timestamp, lit

print("Libraries loaded successfully")
print(f"Target bronze table: {Config.TBL_BRONZE_API_DATA}")

# COMMAND ----------

# DBTITLE 1,OAuth 2.0 Token Manager Class
class OAuthTokenManager:
    def __init__(self, token_url, client_id, client_secret):
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None
        self.token_expires_at = None

    def get_token(self):
        if self.access_token and self.token_expires_at and datetime.now() < self.token_expires_at:
            return self.access_token

        return self._refresh_token()

    def _refresh_token(self):
        try:
            response = requests.post(
                self.token_url,
                data={
                    'grant_type': 'client_credentials',
                    'client_id': self.client_id,
                    'client_secret': self.client_secret
                },
                timeout=30
            )
            response.raise_for_status()

            token_data = response.json()
            self.access_token = token_data['access_token']
            expires_in = token_data.get('expires_in', 3600)
            self.token_expires_at = datetime.now() + timedelta(seconds=expires_in - 60)

            print(f"OAuth token refreshed, expires in {expires_in} seconds")
            return self.access_token

        except Exception as e:
            print(f"ERROR: Failed to refresh OAuth token: {str(e)}")
            raise

print("OAuthTokenManager class defined")

# COMMAND ----------

# DBTITLE 1,API Client with Retry Logic
class APIClient:
    def __init__(self, base_url, token_manager=None):
        self.base_url = base_url.rstrip('/')
        self.token_manager = token_manager
        self.session = self._create_session()

    def _create_session(self):
        session = requests.Session()
        retry_strategy = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def _get_headers(self):
        headers = {'Content-Type': 'application/json'}
        if self.token_manager:
            token = self.token_manager.get_token()
            headers['Authorization'] = f'Bearer {token}'
        return headers

    def get(self, endpoint, params=None, timeout=60):
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        try:
            response = self.session.get(
                url,
                headers=self._get_headers(),
                params=params,
                timeout=timeout
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                retry_after = int(e.response.headers.get('Retry-After', 60))
                print(f"Rate limited, waiting {retry_after} seconds...")
                time.sleep(retry_after)
                return self.get(endpoint, params, timeout)
            else:
                print(f"ERROR: HTTP {e.response.status_code} - {e.response.text}")
                raise
        except Exception as e:
            print(f"ERROR: Request failed - {str(e)}")
            raise

print("APIClient class defined")

# COMMAND ----------

# DBTITLE 1,Pagination Handler (Cursor & Offset)
def fetch_all_pages_cursor(client, endpoint, params=None, max_pages=None):
    all_records = []
    page_count = 0
    next_cursor = None

    while True:
        page_count += 1
        if max_pages and page_count > max_pages:
            print(f"Reached max pages limit: {max_pages}")
            break

        request_params = params.copy() if params else {}
        if next_cursor:
            request_params['cursor'] = next_cursor

        print(f"Fetching page {page_count}, cursor: {next_cursor or 'None'}")
        response = client.get(endpoint, params=request_params)

        records = response.get('data', [])
        all_records.extend(records)
        print(f"  Fetched {len(records)} records (total: {len(all_records)})")

        next_cursor = response.get('next_cursor')
        if not next_cursor:
            print("No more pages")
            break

    return all_records

def fetch_all_pages_offset(client, endpoint, params=None, page_size=100, max_pages=None):
    all_records = []
    page_count = 0
    offset = 0

    while True:
        page_count += 1
        if max_pages and page_count > max_pages:
            print(f"Reached max pages limit: {max_pages}")
            break

        request_params = params.copy() if params else {}
        request_params['limit'] = page_size
        request_params['offset'] = offset

        print(f"Fetching page {page_count}, offset: {offset}")
        response = client.get(endpoint, params=request_params)

        records = response.get('data', [])
        all_records.extend(records)
        print(f"  Fetched {len(records)} records (total: {len(all_records)})")

        if len(records) < page_size:
            print("Reached last page")
            break

        offset += page_size

    return all_records

print("Pagination helpers defined")

# COMMAND ----------

# DBTITLE 1,Example: Fetch Data from Mock API
# Configuration (replace with actual API credentials)
api_config = {
    'base_url': 'https://api.example.com/v1',
    'token_url': 'https://auth.example.com/oauth/token',
    'client_id': dbutils.secrets.get(scope='api-credentials', key='client-id'),
    'client_secret': dbutils.secrets.get(scope='api-credentials', key='client-secret')
}

try:
    # Initialize OAuth and API client
    token_manager = OAuthTokenManager(
        token_url=api_config['token_url'],
        client_id=api_config['client_id'],
        client_secret=api_config['client_secret']
    )

    client = APIClient(
        base_url=api_config['base_url'],
        token_manager=token_manager
    )

    # Fetch all pages (choose pagination method based on API)
    # Method 1: Cursor-based pagination
    # records = fetch_all_pages_cursor(client, '/customers', max_pages=10)

    # Method 2: Offset-based pagination
    # records = fetch_all_pages_offset(client, '/customers', page_size=100, max_pages=10)

    print(f"Total records fetched: {len(records) if 'records' in locals() else 0}")

except Exception as e:
    print(f"ERROR: Data fetch failed - {str(e)}")
    # For demo purposes, create sample data
    records = [
        {'id': 1, 'name': 'Customer A', 'email': 'a@example.com', 'created_at': '2024-01-15T10:30:00Z'},
        {'id': 2, 'name': 'Customer B', 'email': 'b@example.com', 'created_at': '2024-01-16T14:20:00Z'}
    ]
    print(f"Using sample data for demonstration: {len(records)} records")

# COMMAND ----------

# DBTITLE 1,Convert JSON to DataFrame & Save to Bronze
from pyspark.sql.functions import from_json, to_timestamp, col

# Convert JSON records to DataFrame
if records:
    # Create DataFrame from JSON
    df_json = spark.createDataFrame([(json.dumps(r),) for r in records], ['raw_json'])

    # Infer schema from first record
    sample_schema = spark.read.json(spark.sparkContext.parallelize([json.dumps(records[0])])).schema

    # Parse JSON and flatten
    df_parsed = df_json.select(
        from_json(col('raw_json'), sample_schema).alias('data')
    ).select('data.*')

    # Add bronze metadata columns
    df_bronze = df_parsed.select(
        '*',
        current_timestamp().alias('_processing_time'),
        lit('REST_API').alias('_source_system'),
        lit(api_config['base_url']).alias('_api_endpoint')
    )

    # Display sample
    print("Sample of ingested data:")
    display(df_bronze.limit(5))

    # Save to bronze table
    df_bronze.write \
        .mode('append') \
        .format('delta') \
        .saveAsTable(Config.TBL_BRONZE_API_DATA)

    print(f"SUCCESS: Ingested {df_bronze.count()} records to {Config.TBL_BRONZE_API_DATA}")

else:
    print("WARNING: No records to ingest")

# COMMAND ----------

# DBTITLE 1,Production Deployment Checklist
# MAGIC %md
# MAGIC ## Production Deployment Checklist
# MAGIC
# MAGIC Security:
# MAGIC - Store API credentials in Databricks Secrets (never hard-code)
# MAGIC - Use secret scope: api-credentials
# MAGIC - Rotate tokens regularly (every 90 days)
# MAGIC - Restrict secret scope access to job service principals
# MAGIC
# MAGIC Monitoring:
# MAGIC - Log all API calls with timestamps and response codes
# MAGIC - Track pagination metrics (pages fetched, records per page)
# MAGIC - Alert on:
# MAGIC   - HTTP 401/403 (auth failures)
# MAGIC   - HTTP 429 (rate limits)
# MAGIC   - HTTP 5xx (server errors)
# MAGIC   - Record count drops > 20 percent
# MAGIC
# MAGIC Scheduling:
# MAGIC - Hourly sync: Set max_pages=50 (prevent runaway costs)
# MAGIC - Daily full sync: No page limit (phase-based on weekends)
# MAGIC - Use Databricks Jobs with retry policy (3 attempts, 5 min delay)
# MAGIC
# MAGIC Error Handling:
# MAGIC - Implement dead letter queue for failed records
# MAGIC - Save partial results on timeout (don't lose progress)
# MAGIC - Send Slack/email alerts on failures
# MAGIC
# MAGIC Cost Optimization:
# MAGIC - Use single-node cluster for API ingestion (no parallelism needed)
# MAGIC - Enable auto-termination (5 min idle)
# MAGIC - Estimated cost: 0.5-2 DBU per run (depending on data volume)

# COMMAND ----------

