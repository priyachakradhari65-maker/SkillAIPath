# Databricks notebook source
# DBTITLE 1,Query Performance Profiling & Optimization
# MAGIC %md
# MAGIC # Query Performance Profiling & Optimization
# MAGIC
# MAGIC FREE EDITION COMPATIBLE
# MAGIC
# MAGIC Purpose: Identify and optimize slow queries to reduce DBU consumption and improve user experience
# MAGIC
# MAGIC Key Techniques:
# MAGIC - Query execution plan analysis (EXPLAIN)
# MAGIC - Identifying full table scans
# MAGIC - Data skipping effectiveness
# MAGIC - Broadcast join optimization
# MAGIC - Caching strategies
# MAGIC
# MAGIC Performance = Cost:
# MAGIC - Faster queries consume fewer DBUs
# MAGIC - Optimized queries can be 10x-100x cheaper
# MAGIC - Target: < 10 seconds for most queries
# MAGIC
# MAGIC Tools:
# MAGIC - EXPLAIN EXTENDED (view query plan)
# MAGIC - Query Profile (Databricks SQL UI)
# MAGIC - system.billing.usage (historical query costs)

# COMMAND ----------

# DBTITLE 1,Config Import
# MAGIC %run /Workspace/Shared/SkillAIPath-June/00_UTILS/01_config

# COMMAND ----------

# DBTITLE 1,Import Libraries
import time
from pyspark.sql.functions import col, lit, when, count, sum, avg, max, min, first, last, concat, concat_ws, trim, upper, lower, to_date, to_timestamp, date_format, year, month, day, row_number, rank, dense_rank, lag, lead, explode, array, struct, map_from_entries, coalesce, isnull, isnotnull, regexp_replace, substring

print("Libraries loaded")

# COMMAND ----------

# DBTITLE 1,Query Profiling Basics
# MAGIC %md
# MAGIC ## Query Profiling Workflow
# MAGIC
# MAGIC 1. Run EXPLAIN on suspect query
# MAGIC 2. Look for:
# MAGIC    - Full table scans (Scan parquet)
# MAGIC    - Large shuffles (Exchange)
# MAGIC    - Missing filters (WHERE clause not pushed down)
# MAGIC    - Broadcast joins on large tables
# MAGIC 3. Apply optimizations
# MAGIC 4. Re-run EXPLAIN to verify improvements
# MAGIC 5. Measure execution time and DBU consumption

# COMMAND ----------

# DBTITLE 1,Example: Slow Query (Before Optimization)
# MAGIC %sql
# MAGIC -- SLOW QUERY: Full table scan without partition filtering
# MAGIC -- Don't run this in production!
# MAGIC -- EXPLAIN EXTENDED
# MAGIC -- SELECT
# MAGIC --     product_name,
# MAGIC --     SUM(amount) AS total_revenue
# MAGIC -- FROM sap_dev.bronze.sales s
# MAGIC -- JOIN sap_dev.silver.dim_product p ON s.product_id = p.product_id
# MAGIC -- WHERE p.category = 'Electronics'
# MAGIC -- GROUP BY product_name
# MAGIC -- ORDER BY total_revenue DESC
# MAGIC -- LIMIT 10;
# MAGIC
# MAGIC -- Problem: Scans entire sales table (no date filter)
# MAGIC -- Expected: 5-10 seconds, 2-5 DBUs

# COMMAND ----------

# DBTITLE 1,Optimized Query (With Partition Filter)
# MAGIC %sql
# MAGIC -- OPTIMIZED QUERY: Partition pruning + predicate pushdown
# MAGIC EXPLAIN EXTENDED
# MAGIC SELECT
# MAGIC     p.product_name,
# MAGIC     SUM(s.amount) AS total_revenue
# MAGIC FROM sap_dev.bronze.sales s
# MAGIC JOIN sap_dev.silver.dim_product p
# MAGIC     ON s.product_id = p.product_id
# MAGIC     AND p.is_current = TRUE  -- Filter dimension early
# MAGIC WHERE s.sale_date >= CURRENT_DATE() - INTERVAL 90 DAYS  -- Partition pruning
# MAGIC   AND p.category = 'Electronics'
# MAGIC GROUP BY p.product_name
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 10;
# MAGIC
# MAGIC -- Improvement: Scans only 90 days (1/4 of data)
# MAGIC -- Expected: 1-2 seconds, 0.2-0.5 DBUs (10x faster)

# COMMAND ----------

# DBTITLE 1,Benchmark Query Performance
def benchmark_query(query, description):
    """
    Run query and measure execution time.
    """
    print(f"\n--- {description} ---")
    start_time = time.time()

    df = spark.sql(query)
    result_count = df.count()  # Force execution

    elapsed = time.time() - start_time
    print(f"Rows returned: {result_count}")
    print(f"Execution time: {elapsed:.2f} seconds")
    print(f"Estimated DBU: {elapsed / 60 * 0.1:.3f} DBU (rough estimate)")

    return elapsed

# Test with optimized query
optimized_query = """
SELECT
    p.product_name,
    SUM(s.amount) AS total_revenue
FROM sap_dev.bronze.sales s
JOIN sap_dev.silver.dim_product p
    ON s.product_id = p.product_id
    AND p.is_current = TRUE
WHERE s.sale_date >= CURRENT_DATE() - INTERVAL 30 DAYS
  AND p.category = 'Electronics'
GROUP BY p.product_name
ORDER BY total_revenue DESC
LIMIT 10
"""

try:
    benchmark_query(optimized_query, "Optimized Query (30 days, partition filter)")
except Exception as e:
    print(f"Error (table may not exist): {e}")

# COMMAND ----------

# DBTITLE 1,Broadcast Join Optimization
# MAGIC %sql
# MAGIC -- Force broadcast join for small dimension tables
# MAGIC -- Use when dimension < 10 MB
# MAGIC
# MAGIC SELECT /*+ BROADCAST(p) */
# MAGIC     s.sale_date,
# MAGIC     p.category,
# MAGIC     COUNT(*) AS transaction_count,
# MAGIC     SUM(s.amount) AS total_revenue
# MAGIC FROM sap_dev.bronze.sales s
# MAGIC JOIN sap_dev.silver.dim_product p
# MAGIC     ON s.product_id = p.product_id
# MAGIC WHERE s.sale_date >= CURRENT_DATE() - INTERVAL 7 DAYS
# MAGIC GROUP BY s.sale_date, p.category
# MAGIC ORDER BY s.sale_date DESC, total_revenue DESC;
# MAGIC
# MAGIC -- Benefit: Avoids shuffle (Exchange) for small tables
# MAGIC -- Typical speedup: 2-5x for dimension joins

# COMMAND ----------

# DBTITLE 1,Cache Frequently Queried Tables
# Cache small, frequently accessed tables in memory
# Best for: dimension tables, lookup tables, reference data

def cache_table(table_name):
    """
    Cache table in memory for faster queries.
    """
    try:
        spark.sql(f"CACHE TABLE {table_name}")

        # Check cache status
        cached_tables = spark.sql("SHOW TABLES").filter(col("isTemporary") == True)

        print(f"Table {table_name} cached successfully")
        print("Queries on this table will now read from memory (faster)")
        print("NOTE: Cache is lost when cluster restarts")

        return True
    except Exception as e:
        print(f"Error caching {table_name}: {e}")
        return False

# Example: Cache product dimension
try:
    cache_table('sap_dev.silver.dim_product')
except:
    print("Skipping cache (table may not exist)")

# Uncache when done
# spark.sql("UNCACHE TABLE sap_dev.silver.dim_product")

# COMMAND ----------

# DBTITLE 1,Aggregation Pushdown Pattern
# MAGIC %sql
# MAGIC -- Pre-aggregate before join (reduces shuffle data)
# MAGIC -- GOOD PATTERN:
# MAGIC
# MAGIC WITH daily_sales AS (
# MAGIC     SELECT
# MAGIC         product_id,
# MAGIC         sale_date,
# MAGIC         SUM(amount) AS daily_revenue,
# MAGIC         COUNT(*) AS daily_transactions
# MAGIC     FROM sap_dev.bronze.sales
# MAGIC     WHERE sale_date >= CURRENT_DATE() - INTERVAL 30 DAYS
# MAGIC     GROUP BY product_id, sale_date
# MAGIC )
# MAGIC SELECT
# MAGIC     p.product_name,
# MAGIC     p.category,
# MAGIC     SUM(ds.daily_revenue) AS total_revenue,
# MAGIC     SUM(ds.daily_transactions) AS total_transactions
# MAGIC FROM daily_sales ds
# MAGIC JOIN sap_dev.silver.dim_product p
# MAGIC     ON ds.product_id = p.product_id
# MAGIC GROUP BY p.product_name, p.category
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 20;
# MAGIC
# MAGIC -- Benefit: Aggregates before join (smaller shuffle)
# MAGIC -- Typical speedup: 3-10x vs aggregating after join

# COMMAND ----------

# DBTITLE 1,Common Query Anti-Patterns
# MAGIC %md
# MAGIC ## Query Anti-Patterns to Avoid
# MAGIC
# MAGIC Anti-Pattern 1: No Partition Filtering
# MAGIC - BAD: SELECT * FROM sales WHERE product_id = 123
# MAGIC - GOOD: SELECT * FROM sales WHERE sale_date >= '2024-06-01' AND product_id = 123
# MAGIC - Impact: 10-100x slower without date filter
# MAGIC
# MAGIC Anti-Pattern 2: SELECT * on Large Tables
# MAGIC - BAD: SELECT * FROM sales WHERE sale_date = CURRENT_DATE()
# MAGIC - GOOD: SELECT product_id, amount, customer_id FROM sales WHERE sale_date = CURRENT_DATE()
# MAGIC - Impact: 2-5x slower with unnecessary columns
# MAGIC
# MAGIC Anti-Pattern 3: COUNT(DISTINCT) on High-Cardinality
# MAGIC - BAD: SELECT COUNT(DISTINCT customer_id) FROM sales (billions of rows)
# MAGIC - GOOD: Use APPROX_COUNT_DISTINCT() for estimates
# MAGIC - Impact: 5-20x slower for exact counts
# MAGIC
# MAGIC Anti-Pattern 4: Join Without Broadcast Hint on Small Tables
# MAGIC - BAD: SELECT * FROM large_table JOIN small_dimension_table
# MAGIC - GOOD: SELECT /*+ BROADCAST(small_dimension_table) */ ...
# MAGIC - Impact: 2-5x slower with shuffle
# MAGIC
# MAGIC Anti-Pattern 5: Aggregating Before Filtering
# MAGIC - BAD: SELECT category, SUM(amount) FROM sales GROUP BY category HAVING SUM(amount) > 10000
# MAGIC - GOOD: Filter first with WHERE, then aggregate
# MAGIC - Impact: 2-10x slower
# MAGIC
# MAGIC Anti-Pattern 6: UDFs Instead of Built-in Functions
# MAGIC - BAD: Custom Python UDF for date manipulation
# MAGIC - GOOD: Use built-in SQL date functions
# MAGIC - Impact: 10-100x slower (UDF serialization overhead)

# COMMAND ----------

# DBTITLE 1,Optimization Checklist
# MAGIC %md
# MAGIC ## Query Optimization Checklist
# MAGIC
# MAGIC Before submitting any query:
# MAGIC - [ ] Add partition filter (date range) to WHERE clause
# MAGIC - [ ] Select only needed columns (avoid SELECT *)
# MAGIC - [ ] Use BROADCAST hint for small dimension tables
# MAGIC - [ ] Pre-aggregate before joins when possible
# MAGIC - [ ] Use APPROX_COUNT_DISTINCT for large datasets
# MAGIC - [ ] Cache frequently queried small tables
# MAGIC - [ ] Run EXPLAIN to verify partition pruning
# MAGIC - [ ] Check Query Profile for data skew
# MAGIC
# MAGIC Performance Targets:
# MAGIC - Simple queries (1 table, < 1 day): < 5 seconds
# MAGIC - Medium queries (joins, 1-30 days): < 30 seconds
# MAGIC - Complex queries (multiple joins, 90 days): < 2 minutes
# MAGIC - Heavy analytics (full year, complex aggregations): < 10 minutes
# MAGIC
# MAGIC Cost Targets:
# MAGIC - Simple queries: < 0.1 DBU
# MAGIC - Medium queries: < 0.5 DBU
# MAGIC - Complex queries: < 2 DBU
# MAGIC - Heavy analytics: < 10 DBU
# MAGIC
# MAGIC If query exceeds targets:
# MAGIC 1. Add more filters (narrow date range)
# MAGIC 2. Pre-compute in gold layer (materialized views)
# MAGIC 3. Use sampling for exploratory analysis
# MAGIC 4. Consider incremental processing (not full refresh)
# MAGIC
# MAGIC Expected Savings:
# MAGIC - Partition filtering: 70-90 percent cost reduction
# MAGIC - Column pruning: 20-50 percent reduction
# MAGIC - Broadcast joins: 30-70 percent reduction
# MAGIC - Caching: 50-90 percent reduction (repeated queries)
# MAGIC - Total potential savings: 80-95 percent

# COMMAND ----------

