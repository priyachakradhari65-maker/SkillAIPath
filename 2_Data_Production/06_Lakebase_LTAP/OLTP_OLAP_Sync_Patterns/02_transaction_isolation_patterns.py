# Databricks notebook source
# MAGIC %md
# MAGIC # Transaction Isolation in Lakebase OLTP
# MAGIC
# MAGIC Production patterns for managing concurrent transactions with different isolation levels.
# MAGIC
# MAGIC ## Key Concepts
# MAGIC - Read Committed vs Serializable isolation
# MAGIC - Lock management and deadlock prevention
# MAGIC - Optimistic vs pessimistic concurrency control
# MAGIC - Transaction timeout handling

# COMMAND ----------

# MAGIC %run ../../1_Canvas/1_Setup/1_config

# COMMAND ----------

# DBTITLE 1,Setup OLTP Tables with Lakebase
from pyspark.sql.functions import col, current_timestamp, expr

# Create account balance table for transaction testing
spark.sql(f'''
CREATE TABLE IF NOT EXISTS {Config.CATALOG}.silver.oltp_account_balance (
    account_id BIGINT,
    account_name STRING,
    balance DECIMAL(18,2),
    currency STRING,
    last_updated TIMESTAMP,
    version INT
) USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.isolationLevel' = 'Serializable'
)
''')

# Create transaction log table
spark.sql(f'''
CREATE TABLE IF NOT EXISTS {Config.CATALOG}.silver.oltp_transaction_log (
    transaction_id STRING,
    account_id BIGINT,
    transaction_type STRING,
    amount DECIMAL(18,2),
    timestamp TIMESTAMP,
    status STRING,
    retry_count INT
) USING DELTA
''')

print("OLTP tables created with proper isolation settings")

# COMMAND ----------

# DBTITLE 1,Pattern 1 - Optimistic Concurrency with Version Check
def transfer_with_optimistic_locking(from_account, to_account, amount):
    # Read current version
    from_balance = spark.sql(f'''
        SELECT account_id, balance, version
        FROM {Config.CATALOG}.silver.oltp_account_balance
        WHERE account_id = {from_account}
    ''').first()
    
    if from_balance.balance < amount:
        return "INSUFFICIENT_FUNDS"
    
    current_version = from_balance.version
    
    # Attempt update with version check (optimistic lock)
    rows_updated = spark.sql(f'''
        UPDATE {Config.CATALOG}.silver.oltp_account_balance
        SET balance = balance - {amount},
            version = version + 1,
            last_updated = current_timestamp()
        WHERE account_id = {from_account}
        AND version = {current_version}
    ''')
    
    if rows_updated.count() == 0:
        return "VERSION_CONFLICT_RETRY"
    
    # Credit destination
    spark.sql(f'''
        UPDATE {Config.CATALOG}.silver.oltp_account_balance
        SET balance = balance + {amount},
            version = version + 1,
            last_updated = current_timestamp()
        WHERE account_id = {to_account}
    ''')
    
    return "SUCCESS"

# Test optimistic locking
result = transfer_with_optimistic_locking(1001, 1002, 500.00)
print(f"Transfer result: {result}")

# COMMAND ----------

# DBTITLE 1,Pattern 2 - Serializable Transaction with Retry Logic
from time import sleep

def atomic_transfer_with_retry(from_account, to_account, amount, max_retries=3):
    for attempt in range(max_retries):
        try:
            # Start transaction block
            spark.sql("BEGIN TRANSACTION")
            
            # Check balance with FOR UPDATE (pessimistic lock)
            balance_df = spark.sql(f'''
                SELECT balance FROM {Config.CATALOG}.silver.oltp_account_balance
                WHERE account_id = {from_account}
            ''')
            
            current_balance = balance_df.first().balance
            
            if current_balance < amount:
                spark.sql("ROLLBACK")
                return "INSUFFICIENT_FUNDS", 0
            
            # Debit source
            spark.sql(f'''
                UPDATE {Config.CATALOG}.silver.oltp_account_balance
                SET balance = balance - {amount},
                    last_updated = current_timestamp()
                WHERE account_id = {from_account}
            ''')
            
            # Credit destination
            spark.sql(f'''
                UPDATE {Config.CATALOG}.silver.oltp_account_balance
                SET balance = balance + {amount},
                    last_updated = current_timestamp()
                WHERE account_id = {to_account}
            ''')
            
            # Commit transaction
            spark.sql("COMMIT")
            return "SUCCESS", attempt + 1
            
        except Exception as e:
            spark.sql("ROLLBACK")
            if "ConcurrentModificationException" in str(e) and attempt < max_retries - 1:
                sleep(0.1 * (2 ** attempt))  # Exponential backoff
                continue
            else:
                return f"ERROR: {str(e)}", attempt + 1
    
    return "MAX_RETRIES_EXCEEDED", max_retries

# Test with retry logic
status, attempts = atomic_transfer_with_retry(1001, 1002, 250.00)
print(f"Status: {status}, Attempts: {attempts}")

# COMMAND ----------

# DBTITLE 1,Pattern 3 - Batch Transaction Processing with Conflict Detection
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType

# Simulate batch of transactions
transactions = [
    (1001, 1002, 100.00),
    (1003, 1004, 200.00),
    (1001, 1005, 50.00),   # Conflict - same source account
    (1002, 1003, 75.00),
]

successful = []
failed = []

for from_acc, to_acc, amt in transactions:
    result, _ = atomic_transfer_with_retry(from_acc, to_acc, amt)
    
    if result == "SUCCESS":
        successful.append((from_acc, to_acc, amt, "SUCCESS"))
    else:
        failed.append((from_acc, to_acc, amt, result))

print("="*70)
print("BATCH TRANSACTION RESULTS")
print("="*70)
print(f"Successful: {len(successful)}")
print(f"Failed: {len(failed)}")

if failed:
    print("\nFailed Transactions:")
    for f in failed:
        print(f"  {f[0]} -> {f[1]}: {f[2]} ({f[3]})")

# COMMAND ----------

# DBTITLE 1,Monitoring - Transaction Metrics
# Track transaction success rate
spark.sql(f'''
    SELECT 
        DATE(timestamp) as date,
        transaction_type,
        status,
        COUNT(*) as transaction_count,
        AVG(retry_count) as avg_retries
    FROM {Config.CATALOG}.silver.oltp_transaction_log
    GROUP BY DATE(timestamp), transaction_type, status
    ORDER BY date DESC
''').display()