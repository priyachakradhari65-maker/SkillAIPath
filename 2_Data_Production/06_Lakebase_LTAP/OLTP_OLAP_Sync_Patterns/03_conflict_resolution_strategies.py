# Databricks notebook source
# MAGIC %md
# MAGIC # Conflict Resolution Strategies for OLTP
# MAGIC
# MAGIC Advanced patterns for handling concurrent write conflicts in Lakebase OLTP workloads.
# MAGIC
# MAGIC ## Strategies Covered
# MAGIC - Last Writer Wins (LWW)
# MAGIC - Merge-on-Read with conflict detection
# MAGIC - Application-level conflict resolution
# MAGIC - Delta Lake optimistic concurrency

# COMMAND ----------

# MAGIC %run ../../1_Canvas/1_Setup/1_config

# COMMAND ----------

# DBTITLE 1,Strategy 1 - Last Writer Wins with Timestamp
from pyspark.sql.functions import current_timestamp, col

# Create orders table for conflict testing
spark.sql(f'''
CREATE OR REPLACE TABLE {Config.CATALOG}.silver.oltp_orders (
    order_id BIGINT,
    customer_id BIGINT,
    order_status STRING,
    order_total DECIMAL(18,2),
    updated_by STRING,
    updated_at TIMESTAMP
) USING DELTA
''')

# Insert test data
spark.sql(f'''
INSERT INTO {Config.CATALOG}.silver.oltp_orders VALUES
(1, 101, 'PENDING', 500.00, 'system', current_timestamp()),
(2, 102, 'CONFIRMED', 750.00, 'system', current_timestamp())
''')

def update_order_status_lww(order_id, new_status, updated_by):
    # Last Writer Wins - Always succeeds, timestamp determines final state
    spark.sql(f'''
        MERGE INTO {Config.CATALOG}.silver.oltp_orders AS target
        USING (
            SELECT 
                {order_id} AS order_id,
                '{new_status}' AS order_status,
                '{updated_by}' AS updated_by,
                current_timestamp() AS updated_at
        ) AS source
        ON target.order_id = source.order_id
        WHEN MATCHED THEN UPDATE SET
            target.order_status = source.order_status,
            target.updated_by = source.updated_by,
            target.updated_at = source.updated_at
    ''')
    
    return "SUCCESS"

# Test LWW
result = update_order_status_lww(1, 'SHIPPED', 'user_001')
print(f"LWW Update: {result}")

# COMMAND ----------

# DBTITLE 1,Strategy 2 - Application-Level Conflict Detection
def update_with_conflict_check(order_id, expected_status, new_status, updated_by):
    # Read current state
    current_order = spark.sql(f'''
        SELECT order_status, updated_at
        FROM {Config.CATALOG}.silver.oltp_orders
        WHERE order_id = {order_id}
    ''').first()
    
    if current_order is None:
        return "ORDER_NOT_FOUND"
    
    if current_order.order_status != expected_status:
        return f"CONFLICT: Expected {expected_status}, found {current_order.order_status}"
    
    # Update with condition
    rows_updated = spark.sql(f'''
        UPDATE {Config.CATALOG}.silver.oltp_orders
        SET 
            order_status = '{new_status}',
            updated_by = '{updated_by}',
            updated_at = current_timestamp()
        WHERE order_id = {order_id}
        AND order_status = '{expected_status}'
    ''')
    
    if rows_updated.count() == 0:
        return "CONFLICT: State changed during update"
    
    return "SUCCESS"

# Test conflict detection
result = update_with_conflict_check(1, 'PENDING', 'CONFIRMED', 'user_002')
print(f"Conflict Check: {result}")

# COMMAND ----------

# DBTITLE 1,Strategy 3 - Merge with Conflict Log
# Create conflict log table
spark.sql(f'''
CREATE TABLE IF NOT EXISTS {Config.CATALOG}.silver.oltp_conflict_log (
    conflict_id STRING,
    table_name STRING,
    record_id BIGINT,
    field_name STRING,
    old_value STRING,
    new_value_1 STRING,
    new_value_2 STRING,
    resolution STRING,
    timestamp TIMESTAMP
) USING DELTA
''')

def merge_with_conflict_logging(order_id, updates_dict, updated_by):
    # Read current state
    current = spark.sql(f'''
        SELECT * FROM {Config.CATALOG}.silver.oltp_orders
        WHERE order_id = {order_id}
    ''').first()
    
    conflicts = []
    
    # Check each field for conflicts
    for field, new_value in updates_dict.items():
        old_value = getattr(current, field, None)
        
        # Business rule: Only allow certain transitions
        if field == 'order_status':
            valid_transitions = {
                'PENDING': ['CONFIRMED', 'CANCELLED'],
                'CONFIRMED': ['SHIPPED', 'CANCELLED'],
                'SHIPPED': ['DELIVERED'],
            }
            
            if old_value not in valid_transitions:
                conflicts.append(f"Invalid source state: {old_value}")
            elif new_value not in valid_transitions.get(old_value, []):
                conflicts.append(f"Invalid transition: {old_value} -> {new_value}")
    
    if conflicts:
        # Log conflicts
        for conflict in conflicts:
            spark.sql(f'''
                INSERT INTO {Config.CATALOG}.silver.oltp_conflict_log VALUES (
                    uuid(), 'oltp_orders', {order_id}, 'order_status',
                    '{current.order_status}', '{updates_dict.get("order_status")}', NULL,
                    'REJECTED: {conflict}', current_timestamp()
                )
            ''')
        return f"CONFLICTS: {conflicts}"
    
    # Apply update
    update_set = ", ".join([f"{k} = '{v}'" for k, v in updates_dict.items()])
    spark.sql(f'''
        UPDATE {Config.CATALOG}.silver.oltp_orders
        SET {update_set},
            updated_by = '{updated_by}',
            updated_at = current_timestamp()
        WHERE order_id = {order_id}
    ''')
    
    return "SUCCESS"

# Test merge with conflict logging
result = merge_with_conflict_logging(1, {'order_status': 'DELIVERED'}, 'user_003')
print(f"Merge Result: {result}")

# COMMAND ----------

# DBTITLE 1,View Conflict Log
spark.sql(f'''
    SELECT * FROM {Config.CATALOG}.silver.oltp_conflict_log
    ORDER BY timestamp DESC
''').display()