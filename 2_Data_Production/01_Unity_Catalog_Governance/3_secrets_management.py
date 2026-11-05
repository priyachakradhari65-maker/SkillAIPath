# Databricks notebook source
# DBTITLE 1,Secrets Management - Production Security
# MAGIC %md
# MAGIC # Secrets Management: Databricks Secrets
# MAGIC **SkillAIPath Data Warehouse - Production Security**
# MAGIC
# MAGIC ## THE PROBLEM: "Credentials are hardcoded in notebooks"
# MAGIC
# MAGIC **Monday Morning - Security Audit**
# MAGIC
# MAGIC **Security Team:** "We found database passwords hardcoded in 15 notebooks. Any data breach exposes credentials."
# MAGIC
# MAGIC **Data Engineer:** "We need to connect to external systems. How else do we store passwords?"
# MAGIC
# MAGIC **Security Lead:** "Databricks Secrets. Encrypted key-value store, access-controlled, audit-logged."
# MAGIC
# MAGIC **Engineer:** "How do notebooks access secrets without seeing the values?"
# MAGIC
# MAGIC **Lead:** "dbutils.secrets.get() returns the value at runtime. Notebook users see [REDACTED] in logs."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## THE SOLUTION: Databricks Secrets
# MAGIC
# MAGIC **What It Is:**
# MAGIC * Encrypted key-value store for sensitive data (passwords, API keys, tokens)
# MAGIC * Scope-based access control (dev, uat, prod scopes)
# MAGIC * Audit logging (who accessed what secret when)
# MAGIC * Works with Azure Key Vault, AWS Secrets Manager, Databricks-backed
# MAGIC
# MAGIC **Key Concepts:**
# MAGIC * Secret Scope: Namespace for grouping secrets (e.g., sap-prod)
# MAGIC * Secret Key: Name of the secret (e.g., mysql_password)
# MAGIC * Secret Value: The actual credential (never displayed in logs)
# MAGIC
# MAGIC **When to Use:**
# MAGIC * Database connection strings
# MAGIC * API keys for external services
# MAGIC * OAuth tokens
# MAGIC * Encryption keys
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC * Databricks CLI installed (for secret creation)
# MAGIC * Workspace admin access (for scope creation)
# MAGIC * Understanding of environment separation (dev/uat/prod)

# COMMAND ----------

# DBTITLE 1,Setup - Create Secret Scope
# NOTE: Secret scopes are created via Databricks CLI or UI, NOT via notebook

print("=" * 70)
print("DATABRICKS SECRETS - SETUP GUIDE")
print("=" * 70)
print()
print("Step 1: Create Secret Scope (One-Time Setup)")
print("-" * 70)
print()
print("OPTION A: Databricks CLI")
print("  databricks secrets create-scope --scope sap-dev")
print("  databricks secrets create-scope --scope sap-prod")
print()
print("OPTION B: Workspace UI")
print("  Navigate to: https://<workspace-url>#secrets/createScope")
print("  Scope Name: sap-dev")
print("  Manage Principal: Select 'Creator' or 'All Users'")
print()
print("=" * 70)
print()
print("Step 2: Add Secrets to Scope")
print("-" * 70)
print()
print("CLI Command:")
print("  databricks secrets put --scope sap-dev --key mysql_password")
print("  (Interactive prompt for secret value)")
print()
print("Example Secrets:")
print("  - mysql_password (database password)")
print("  - api_token_salesforce (Salesforce API token)")
print("  - aws_access_key_id (S3 access key)")
print("  - azure_storage_account_key (Blob storage key)")
print("=" * 70)
print()
print("Step 3: Grant Permissions (Optional)")
print("-" * 70)
print("  databricks secrets put-acl --scope sap-prod --principal user@company.com --permission READ")
print()
print("Permission Levels:")
print("  - READ: Can read secret values via dbutils.secrets.get()")
print("  - WRITE: Can add/update/delete secrets in scope")
print("  - MANAGE: Can manage ACLs for the scope")
print("=" * 70)

# COMMAND ----------

# DBTITLE 1,List Available Secret Scopes
# List all secret scopes you have access to

print("Available Secret Scopes:")
print("-" * 70)

try:
    scopes = dbutils.secrets.listScopes()

    if scopes:
        for scope in scopes:
            print(f"  Scope: {scope.name}")

            # List secrets in this scope (keys only, values are never shown)
            secrets = dbutils.secrets.list(scope.name)
            if secrets:
                print(f"    Secrets: {', '.join([s.key for s in secrets])}")
            else:
                print(f"    Secrets: (none)")
            print()
    else:
        print("  No secret scopes found.")
        print("  Create a scope using: databricks secrets create-scope --scope <name>")
except Exception as e:
    print(f"  ERROR: {e}")
    print("  You may not have permissions to list scopes.")

print("=" * 70)
print("NOTE: Secret values are NEVER displayed. Only keys are listed.")
print("=" * 70)

# COMMAND ----------

# DBTITLE 1,Read Secret Value - Database Connection
# Example: Read MySQL password from secrets

print("=" * 70)
print("READING SECRETS - DATABASE CONNECTION EXAMPLE")
print("=" * 70)
print()

# Define secret scope and key
scope_name = "sap-dev"
secret_key = "mysql_password"

print(f"Scope: {scope_name}")
print(f"Key: {secret_key}")
print()

try:
    # Read secret (value is NEVER printed, even if you try)
    password = dbutils.secrets.get(scope=scope_name, key=secret_key)

    # Attempting to print the secret shows [REDACTED]
    print(f"Secret Value: {password}")
    print()
    print("IMPORTANT: Databricks automatically redacts secret values in logs.")
    print("The actual password is available to the code, but not visible to users.")
    print()

    # Use the secret in a connection string (value is available to JDBC, not to users)
    jdbc_url = "jdbc:mysql://sap-db.mysql.database.azure.com:3306/warehouse"
    connection_properties = {
        "user": "sap_admin",
        "password": password,  # Secret is used here
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    print("Connection properties prepared (password is redacted):")
    print(f"  JDBC URL: {jdbc_url}")
    print(f"  User: {connection_properties['user']}")
    print(f"  Password: [REDACTED]")
    print()

    # Example: Read data from external database
    # df = spark.read.jdbc(url=jdbc_url, table="customers", properties=connection_properties)
    # df.show(5)

    print("SUCCESS: Secret retrieved and used in connection.")

except Exception as e:
    print(f"ERROR: {e}")
    print()
    print("Common Issues:")
    print("  1. Secret scope does not exist")
    print("  2. Secret key does not exist in scope")
    print("  3. You don't have READ permission on the scope")
    print()
    print("To create the secret:")
    print(f"  databricks secrets put --scope {scope_name} --key {secret_key}")

print("=" * 70)

# COMMAND ----------

# DBTITLE 1,Pattern - API Key Authentication
# Example: Use secret for API authentication

import requests
import json

print("=" * 70)
print("API AUTHENTICATION PATTERN")
print("=" * 70)
print()

scope_name = "sap-dev"
api_key_secret = "salesforce_api_token"

print(f"Retrieving API token from: {scope_name}.{api_key_secret}")
print()

try:
    # Get API token from secrets
    api_token = dbutils.secrets.get(scope=scope_name, key=api_key_secret)

    # Use token in API request headers
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }

    print("Headers prepared (token is redacted):")
    print(f"  Authorization: Bearer [REDACTED]")
    print(f"  Content-Type: application/json")
    print()

    # Example API call (commented out - replace with actual endpoint)
    # response = requests.get(
    #     "https://api.salesforce.com/services/data/v55.0/query",
    #     headers=headers,
    #     params={"q": "SELECT Id, Name FROM Account LIMIT 10"}
    # )
    #
    # if response.status_code == 200:
    #     data = response.json()
    #     print(f"SUCCESS: Retrieved {len(data['records'])} records")
    # else:
    #     print(f"ERROR: API returned status {response.status_code}")

    print("Pattern Demonstrated:")
    print("  1. Retrieve secret via dbutils.secrets.get()")
    print("  2. Use secret in API headers/params")
    print("  3. Secret value never appears in logs")
    print("  4. No hardcoded credentials in notebooks")

except Exception as e:
    print(f"ERROR: {e}")
    print()
    print("To create the API token secret:")
    print(f"  databricks secrets put --scope {scope_name} --key {api_key_secret}")

print("=" * 70)

# COMMAND ----------

# DBTITLE 1,Best Practices and Common Mistakes
# MAGIC %md
# MAGIC ## Best Practices
# MAGIC
# MAGIC ### Secret Scope Naming
# MAGIC * Use environment prefixes: `sap-dev`, `sap-uat`, `sap-prod`
# MAGIC * Separate scopes per environment (never share prod secrets with dev)
# MAGIC * Use descriptive scope names (`project-env` pattern)
# MAGIC
# MAGIC ### Secret Key Naming
# MAGIC * Lowercase with underscores: `mysql_password`, `api_token_salesforce`
# MAGIC * Include service name: `azure_storage_key`, `aws_access_key_id`
# MAGIC * Avoid generic names: `password`, `token` (not specific enough)
# MAGIC
# MAGIC ### Access Control
# MAGIC * Principle of least privilege (READ-only for most users)
# MAGIC * Prod secrets: Restrict to production service principals only
# MAGIC * Dev secrets: More permissive (all team members READ access)
# MAGIC * MANAGE permissions: Only admins
# MAGIC
# MAGIC ### Rotation Policy
# MAGIC * Rotate secrets every 90 days (compliance requirement)
# MAGIC * Update secrets via CLI (databricks secrets put with new value)
# MAGIC * Test rotation in dev before applying to prod
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Common Mistakes
# MAGIC
# MAGIC **1. Hardcoding Credentials**
# MAGIC * WRONG: `password = "YOUR_PASSWORD_HERE"`
# MAGIC * RIGHT: `password = dbutils.secrets.get(scope="sap-prod", key="mysql_password")`
# MAGIC
# MAGIC **2. Printing Secret Values**
# MAGIC * WRONG: `print(f"Password is: {dbutils.secrets.get(...)}")`
# MAGIC * Databricks redacts it, but DON'T try to print secrets
# MAGIC
# MAGIC **3. Sharing Secrets Across Environments**
# MAGIC * WRONG: Using `sap-prod` scope in dev notebooks
# MAGIC * RIGHT: Environment-specific scopes (dev scope for dev, prod scope for prod)
# MAGIC
# MAGIC **4. No Secret Rotation**
# MAGIC * WRONG: Set secret once, never update
# MAGIC * RIGHT: Rotate every 90 days, document rotation date
# MAGIC
# MAGIC **5. Overly Permissive ACLs**
# MAGIC * WRONG: All users have WRITE access to prod secrets
# MAGIC * RIGHT: READ-only for SkillAIPath Developers, WRITE for admins only
# MAGIC
# MAGIC **6. Not Using Secrets for API Keys**
# MAGIC * WRONG: `api_key = "YOUR_API_KEY_HERE"`
# MAGIC * RIGHT: `api_key = dbutils.secrets.get(scope="sap-prod", key="stripe_api_key")`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Interview Talking Points
# MAGIC
# MAGIC **Scenario: "How do you manage database credentials across environments?"**
# MAGIC
# MAGIC Answer: "We use Databricks Secrets with environment-specific scopes. Each environment (dev/uat/prod) has its own scope (sap-dev, sap-prod). Notebooks retrieve credentials at runtime via dbutils.secrets.get(), which returns [REDACTED] in logs. This prevents credential exposure in version control or notebook exports. We rotate secrets every 90 days and restrict prod scope access to service principals only."
# MAGIC
# MAGIC **Scenario: "What happens if a notebook tries to print a secret value?"**
# MAGIC
# MAGIC Answer: "Databricks automatically redacts secret values in all logs, cell outputs, and error messages. Even if code explicitly prints a secret, users see [REDACTED]. The actual value is only available to the executing code (e.g., JDBC driver, API client), never to humans viewing the notebook."
# MAGIC
# MAGIC **Scenario: "How do you handle secret rotation without breaking pipelines?"**
# MAGIC
# MAGIC Answer: "We update secrets via Databricks CLI (databricks secrets put) which overwrites the existing value. Since notebooks reference secrets by scope and key (not the value itself), no code changes are needed. We test rotation in dev first, then apply to uat and prod. For zero-downtime rotation, we use blue-green deployment: create a new secret key (mysql_password_v2), update code to use new key, delete old key after validation."

# COMMAND ----------

# DBTITLE 1,Summary - Security Checklist
# Security Checklist for SkillAIPath Production

print("=" * 70)
print("SECRETS MANAGEMENT - PRODUCTION CHECKLIST")
print("=" * 70)
print()

checklist = [
    ("Secret scopes created for each environment", "sap-dev, sap-uat, sap-prod"),
    ("All database passwords stored as secrets", "mysql_password, postgres_password"),
    ("All API keys stored as secrets", "salesforce_token, stripe_api_key"),
    ("Cloud storage keys stored as secrets", "aws_access_key_id, azure_storage_key"),
    ("ACLs configured (READ for SkillAIPath Developers, WRITE for admins)", "Principle of least privilege"),
    ("No hardcoded credentials in notebooks", "Search codebase for password=, api_key="),
    ("Secrets rotation policy documented", "Every 90 days, tested in dev first"),
    ("Prod secrets restricted to service principals", "No human access to prod scope"),
    ("Secret access audit logs reviewed monthly", "system.access.secrets_audit_log"),
    ("Backup secrets stored securely", "Encrypted backup in corporate vault")
]

for i, (item, detail) in enumerate(checklist, 1):
    print(f"{i:2}. {item}")
    print(f"    {detail}")
    print()

print("=" * 70)
print("PRODUCTION READINESS: Review checklist before go-live")
print("=" * 70)
print()
print("Next Steps:")
print("  1. Create secret scopes via Databricks CLI")
print("  2. Migrate hardcoded credentials to secrets")
print("  3. Test secret retrieval in dev environment")
print("  4. Configure ACLs for each scope")
print("  5. Document secret rotation procedure")
print("  6. Schedule quarterly access reviews")
print("=" * 70)