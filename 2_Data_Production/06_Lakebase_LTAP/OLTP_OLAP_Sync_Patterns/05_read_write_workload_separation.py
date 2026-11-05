# Databricks notebook source
# MAGIC %md
# MAGIC # Read/Write Workload Separation in Lakebase OLTP
# MAGIC
# MAGIC Patterns for isolating read-heavy analytics from write-heavy transactional workloads.
# MAGIC
# MAGIC ## Strategies
# MAGIC - Read replicas using Delta sharing
# MAGIC - Materialized views for reporting
# MAGIC - Write-ahead logging with async sync
# MAGIC - Query routing based on workload type

# COMMAND ----------

# MAGIC %run ../../1_Canvas/1_Setup/1_config

# COMMAND ----------

# DBTITLE 1,Pattern 1 - Create Read Replica with Materialized View
# Primary OLTP table (write-heavy)
spark.sql(f'''
CREATE TABLE IF NOT EXISTS {Config.CATALOG}.silver.oltp_orders_primary (
    order_id BIGINT,
    customer_id BIGINT,
    order_date DATE,
    order_total DECIMAL(18,2),
    status STRING,
    last_updated TIMESTAMP
) USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
''')

# Create materialized view for analytics (read-only)
spark.sql(f'''
CREATE MATERIALIZED VIEW IF NOT EXISTS {Config.CATALOG}.gold.oltp_orders_analytics AS
SELECT 
    DATE(order_date) as order_date,
    status,
    COUNT(*) as order_count,
    SUM(order_total) as total_revenue,
    AVG(order_total) as avg_order_value
FROM {Config.CATALOG}.silver.oltp_orders_primary
GROUP BY DATE(order_date), status
''')

print("Read replica materialized view created")

# COMMAND ----------

# DBTITLE 1,Pattern 2 - Async Replication for Reporting Database
from pyspark.sql.functions import col, current_timestamp

# Create reporting database (async replica)
spark.sql(f'''
CREATE TABLE IF NOT EXISTS {Config.CATALOG}.gold.oltp_orders_reporting (
    order_id BIGINT,
    customer_id BIGINT,
    order_date DATE,
    order_total DECIMAL(18,2),
    status STRING,
    replicated_at TIMESTAMP
) USING DELTA
''')

def replicate_to_reporting():
    # Read from primary (with lag tolerance)
    primary_df = spark.sql(f'''
        SELECT 
            order_id,
            customer_id,
            order_date,
            order_total,
            status,
            current_timestamp() as replicated_at
        FROM {Config.CATALOG}.silver.oltp_orders_primary
        WHERE last_updated > (
            SELECT COALESCE(MAX(replicated_at), '1900-01-01')
            FROM {Config.CATALOG}.gold.oltp_orders_reporting
        )
    ''')
    
    # Write to reporting replica
    primary_df.write.mode("append").saveAsTable(
        f"{Config.CATALOG}.gold.oltp_orders_reporting"
    )
    
    return primary_df.count()

# Run replication
rows_replicated = replicate_to_reporting()
print(f"Replicated {rows_replicated} rows to reporting database")

# COMMAND ----------

# DBTITLE 1,Pattern 3 - Query Routing Based on Workload Type
class OLTPQueryRouter:
    def __init__(self, primary_table, replica_table):
        self.primary_table = primary_table
        self.replica_table = replica_table
        self.write_count = 0
        self.read_count = 0
    
    def route_query(self, query_type, sql_query):
        if query_type == "WRITE":
            # Route to primary
            self.write_count += 1
            print(f"Routing to PRIMARY (writes: {self.write_count})")
            return spark.sql(sql_query.replace("TARGET_TABLE", self.primary_table))
        
        elif query_type == "READ_REALTIME":
            # Route to primary for real-time reads
            self.read_count += 1
            print(f"Routing to PRIMARY (realtime reads: {self.read_count})")
            return spark.sql(sql_query.replace("TARGET_TABLE", self.primary_table))
        
        else:  # READ_ANALYTICS
            # Route to replica for analytics
            self.read_count += 1
            print(f"Routing to REPLICA (analytics reads: {self.read_count})")
            return spark.sql(sql_query.replace("TARGET_TABLE", self.replica_table))
    
    def print_stats(self):
        print("="*70)
        print("QUERY ROUTING STATISTICS")
        print("="*70)
        print(f"Total Writes: {self.write_count}")
        print(f"Total Reads: {self.read_count}")
        print(f"Write/Read Ratio: {self.write_count / max(self.read_count, 1):.2f}")

# Initialize router
router = OLTPQueryRouter(
    f"{Config.CATALOG}.silver.oltp_orders_primary",
    f"{Config.CATALOG}.gold.oltp_orders_reporting"
)

# Test routing
router.route_query("WRITE", "INSERT INTO TARGET_TABLE VALUES (1, 101, current_date(), 500.00, 'PENDING', current_timestamp())")
router.route_query("READ_ANALYTICS", "SELECT COUNT(*) FROM TARGET_TABLE")
router.route_query("READ_REALTIME", "SELECT * FROM TARGET_TABLE WHERE order_id = 1")

router.print_stats()

# COMMAND ----------

# DBTITLE 1,Pattern 4 - Change Data Capture for Real-Time Sync
# Enable CDC on primary table
spark.sql(f'''
    ALTER TABLE {Config.CATALOG}.silver.oltp_orders_primary
    SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
''')

# Read CDC feed
cdc_df = spark.readStream.format("delta") \
    .option("readChangeData", "true") \
    .option("startingVersion", "0") \
    .table(f"{Config.CATALOG}.silver.oltp_orders_primary")

# Write to streaming replica
cdc_query = cdc_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", f"/tmp/checkpoints/oltp_cdc_{Config.ENVIRONMENT}") \
    .outputMode("append") \
    .toTable(f"{Config.CATALOG}.gold.oltp_orders_cdc_stream")

print("CDC streaming replication started")
print("Streaming query ID:", cdc_query.id)

# COMMAND ----------

# DBTITLE 1,Monitoring - Workload Separation Metrics
# Check replication lag
spark.sql(f'''
    SELECT 
        TIMESTAMPDIFF(SECOND, MAX(replicated_at), current_timestamp()) as lag_seconds
    FROM {Config.CATALOG}.gold.oltp_orders_reporting
''').display()

# Check primary vs replica query performance
spark.sql(f'''
    SELECT 
        'PRIMARY' as source,
        COUNT(*) as row_count
    FROM {Config.CATALOG}.silver.oltp_orders_primary
    UNION ALL
    SELECT 
        'REPLICA' as source,
        COUNT(*) as row_count
    FROM {Config.CATALOG}.gold.oltp_orders_reporting
''').display()