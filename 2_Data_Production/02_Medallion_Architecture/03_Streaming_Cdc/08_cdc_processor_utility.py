# Databricks notebook source
# DBTITLE 1,CDC Processor Utility - Production-Grade
# MAGIC %md
# MAGIC # CDC Processor Utility
# MAGIC **SkillAIPath - Change Data Capture Patterns**
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC Reusable CDC processing functions for incremental data loads:
# MAGIC * MERGE operations (INSERT/UPDATE/DELETE)
# MAGIC * Watermark-based incremental reads
# MAGIC * SCD Type 1 & Type 2 updates
# MAGIC * Idempotency controls
# MAGIC
# MAGIC  **INTERVIEW**: "I built a reusable CDC processor that handles MERGE operations with idempotency controls. This eliminated duplicate code across 10+ dimension loads and ensured exactly-once processing."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## CDC Patterns Covered
# MAGIC
# MAGIC **Pattern 1: Upsert (MERGE with INSERT/UPDATE)**
# MAGIC ```sql
# MAGIC MERGE INTO target t
# MAGIC USING source s ON t.id = s.id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC ```
# MAGIC
# MAGIC **Pattern 2: SCD Type 2 (Track History)**
# MAGIC ```sql
# MAGIC -- Expire old records
# MAGIC UPDATE target SET EndDate = current_date, IsCurrent = 0
# MAGIC WHERE id IN (SELECT id FROM changed_records)
# MAGIC
# MAGIC -- Insert new versions
# MAGIC INSERT INTO target (id, ..., StartDate, EndDate, IsCurrent)
# MAGIC SELECT id, ..., current_date, '9999-12-31', 1 FROM source
# MAGIC ```
# MAGIC
# MAGIC **Pattern 3: Watermark-Based Incremental**
# MAGIC ```python
# MAGIC last_watermark = get_last_watermark('table_name')
# MAGIC df_incremental = df.filter(col('modified_at') > last_watermark)
# MAGIC ```

# COMMAND ----------

# MAGIC %run ../../../1_Canvas/1_Setup/1_Config

# COMMAND ----------

# DBTITLE 1,Setup - Config Import
 

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, lit, when, count, sum, avg, max, min, first, last, concat, concat_ws, trim, upper, lower, to_date, to_timestamp, date_format, year, month, day, row_number, rank, dense_rank, lag, lead, explode, array, struct, map_from_entries, coalesce, isnull, isnotnull, regexp_replace, substring, current_date, current_timestamp
from datetime import datetime

print("="*70)
print("CDC Processor Utility loaded")
print("="*70)
print(f"Environment: {Config.ENVIRONMENT}")
print(f"Catalog: {Config.CATALOG}")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Function 1: Upsert MERGE (INSERT + UPDATE)
def upsert_merge(
    source_df: DataFrame,
    target_table: str,
    merge_keys: list,
    update_columns: list = None,
    batch_date: str = None
):
    """
    Perform MERGE operation: UPDATE existing records, INSERT new ones

    Args:
        source_df: Source DataFrame with changes
        target_table: Target Delta table name
        merge_keys: List of columns for matching (e.g. ['customer_id'])
        update_columns: Columns to update (None = all columns)
        batch_date: Optional batch date for audit trail

    Returns:
        dict: Merge statistics

    Example:
        stats = upsert_merge(
            source_df=df_changes,
            target_table=Config.TBL_DIM_PRODUCT,
            merge_keys=['product_id'],
            batch_date='2024-01-15'
        )
    """
    from delta.tables import DeltaTable

    # Create temporary view for source
    temp_view = f"temp_source_{target_table.split('.')[-1]}"
    source_df.createOrReplaceTempView(temp_view)

    # Build merge condition
    merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])

    # Build update SET clause
    if update_columns:
        update_set = ", ".join([f"target.{col} = source.{col}" for col in update_columns])
    else:
        # Update all columns except merge keys
        all_cols = source_df.columns
        update_cols = [col for col in all_cols if col not in merge_keys]
        update_set = ", ".join([f"target.{col} = source.{col}" for col in update_cols])

    # Execute MERGE
    merge_sql = f"""
        MERGE INTO {target_table} AS target
        USING {temp_view} AS source
        ON {merge_condition}
        WHEN MATCHED THEN
            UPDATE SET {update_set}
        WHEN NOT MATCHED THEN
            INSERT *
    """

    print(f"\n Executing MERGE on {target_table}...")
    result = spark.sql(merge_sql)

    # Get statistics
    stats = {
        "target_table": target_table,
        "merge_keys": merge_keys,
        "source_rows": source_df.count(),
        "batch_date": batch_date,
        "executed_at": datetime.now().isoformat()
    }

    print(f" MERGE completed")
    print(f"   Source rows: {stats['source_rows']:,}")

    return stats

print(" upsert_merge() function loaded")

# COMMAND ----------

# DBTITLE 1,Function 2: SCD Type 2 MERGE (Track History)
def scd2_merge(
    source_df: DataFrame,
    target_table: str,
    business_keys: list,
    compare_columns: list,
    surrogate_key_col: str = None
):
    """
    Perform SCD Type 2 MERGE: Expire old records, insert new versions

    Args:
        source_df: Source DataFrame with latest data
        target_table: Target Delta table name
        business_keys: Natural keys (e.g. ['customer_id'])
        compare_columns: Columns to check for changes
        surrogate_key_col: Surrogate key column (auto-generated if None)

    Returns:
        dict: SCD Type 2 statistics

    Example:
        stats = scd2_merge(
            source_df=df_customers,
            target_table=Config.TBL_DIM_CUSTOMER,
            business_keys=['customer_id'],
            compare_columns=['customer_name', 'email', 'phone']
        )
    """
    from delta.tables import DeltaTable

    print(f"\n Executing SCD Type 2 MERGE on {target_table}...")

    # Step 1: Identify changed records
    temp_source = "temp_scd2_source"
    source_df.createOrReplaceTempView(temp_source)

    # Build change detection condition
    change_conditions = []
    for col in compare_columns:
        change_conditions.append(f"target.{col} != source.{col}")
    change_condition = " OR \n        ".join(change_conditions)

    business_key_condition = " AND ".join([f"target.{key} = source.{key}" for key in business_keys])

    # Step 2: Expire old records (set EndDate, IsCurrent = 0)
    expire_sql = f"""
        UPDATE {target_table} AS target
        SET
            {Config.SCD_END_DATE_COL} = current_date(),
            {Config.SCD_IS_CURRENT_COL} = 0
        WHERE {Config.SCD_IS_CURRENT_COL} = 1
          AND EXISTS (
              SELECT 1 FROM {temp_source} AS source
              WHERE {business_key_condition}
                AND ({change_condition})
          )
    """

    expired_rows = spark.sql(expire_sql)
    print(f"   Expired old versions")

    # Step 3: Insert new versions
    # Get max surrogate key
    if surrogate_key_col:
        max_sk_query = f"SELECT COALESCE(MAX({surrogate_key_col}), 0) as max_sk FROM {target_table}"
        max_sk = spark.sql(max_sk_query).first()['max_sk']
    else:
        max_sk = 0

    # Add SCD columns
    df_new_versions = source_df \
        .withColumn(Config.SCD_START_DATE_COL, current_date()) \
        .withColumn(Config.SCD_END_DATE_COL, lit(Config.SCD_HIGH_DATE).cast("date")) \
        .withColumn(Config.SCD_IS_CURRENT_COL, lit(1))

    if surrogate_key_col:
        # Generate new surrogate keys
        df_new_versions = df_new_versions \
            .withColumn("row_num", row_number().over(Window.orderBy(business_keys[0]))) \
            .withColumn(surrogate_key_col, col("row_num") + lit(max_sk)) \
            .drop("row_num")

    # MERGE to insert new + update existing unchanged
    temp_new = "temp_scd2_new"
    df_new_versions.createOrReplaceTempView(temp_new)

    merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in business_keys])

    merge_sql = f"""
        MERGE INTO {target_table} AS target
        USING {temp_new} AS source
        ON {merge_condition} AND target.{Config.SCD_IS_CURRENT_COL} = 1
        WHEN MATCHED AND ({change_condition}) THEN
            -- This shouldn't happen (should be expired), but safety check
            UPDATE SET target.{Config.SCD_IS_CURRENT_COL} = 0
        WHEN NOT MATCHED THEN
            INSERT *
    """

    result = spark.sql(merge_sql)
    print(f"   Inserted new versions")

    stats = {
        "target_table": target_table,
        "business_keys": business_keys,
        "source_rows": source_df.count(),
        "executed_at": datetime.now().isoformat()
    }

    print(f" SCD Type 2 MERGE completed")
    return stats

print(" scd2_merge() function loaded")

# COMMAND ----------

# DBTITLE 1,Function 3: Watermark Manager (Incremental Reads)
def get_last_watermark(table_name: str, watermark_table: str = None) -> str:
    """
    Get last processed watermark for incremental reads

    Args:
        table_name: Table being processed
        watermark_table: Watermark tracking table (default: Config.TBL_WATERMARKS)

    Returns:
        str: Last watermark timestamp (ISO format)

    Example:
        last_run = get_last_watermark('stg_sales')
        df_incremental = df.filter(col('modified_at') > last_run)
    """
    if watermark_table is None:
        watermark_table = f"{Config.DATABASE}.cdc_watermarks"

    try:
        result = spark.sql(f"""
            SELECT watermark_value
            FROM {watermark_table}
            WHERE table_name = '{table_name}'
            ORDER BY updated_at DESC
            LIMIT 1
        """)

        if result.count() > 0:
            return result.first()['watermark_value']
        else:
            # No watermark found, return epoch
            return '1970-01-01 00:00:00'
    except Exception as e:
        if "Table or view not found" in str(e):
            print(f" Watermark table not found, creating: {watermark_table}")
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {watermark_table} (
                    table_name STRING,
                    watermark_value STRING,
                    updated_at TIMESTAMP,
                    updated_by STRING
                ) USING DELTA
            """)
            return '1970-01-01 00:00:00'
        else:
            raise e

def update_watermark(table_name: str, new_watermark: str, watermark_table: str = None):
    """
    Update watermark after successful processing

    Args:
        table_name: Table being processed
        new_watermark: New watermark value (timestamp)
        watermark_table: Watermark tracking table

    Example:
        update_watermark('stg_sales', '2024-01-15 23:59:59')
    """
    if watermark_table is None:
        watermark_table = f"{Config.DATABASE}.cdc_watermarks"

    current_user = spark.sql("SELECT current_user()").first()[0]

    spark.sql(f"""
        INSERT INTO {watermark_table}
        VALUES (
            '{table_name}',
            '{new_watermark}',
            current_timestamp(),
            '{current_user}'
        )
    """)

    print(f" Watermark updated: {table_name} -> {new_watermark}")

print(" get_last_watermark() and update_watermark() functions loaded")

# COMMAND ----------

# DBTITLE 1,Usage Examples
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Usage Examples
# MAGIC
# MAGIC ### Example 1: Upsert Product Dimension
# MAGIC
# MAGIC ```python
# MAGIC # Read incremental changes from staging
# MAGIC df_product_changes = spark.table(Config.TBL_STG_PRODUCT).filter(
# MAGIC     col('BatchDate') == '2024-01-15'
# MAGIC )
# MAGIC
# MAGIC # Upsert into dimension
# MAGIC stats = upsert_merge(
# MAGIC     source_df=df_product_changes,
# MAGIC     target_table=Config.TBL_DIM_PRODUCT,
# MAGIC     merge_keys=['product_id'],
# MAGIC     batch_date='2024-01-15'
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Example 2: SCD Type 2 Customer Dimension
# MAGIC
# MAGIC ```python
# MAGIC # Read latest customer data
# MAGIC df_customers = spark.table(Config.TBL_STG_SALES).filter(
# MAGIC     col('BatchDate') == '2024-01-15'
# MAGIC ).select(
# MAGIC     'customer_id', 'customer_name', 'email', 'phone'
# MAGIC ).distinct()
# MAGIC
# MAGIC # Apply SCD Type 2
# MAGIC stats = scd2_merge(
# MAGIC     source_df=df_customers,
# MAGIC     target_table=Config.TBL_DIM_CUSTOMER,
# MAGIC     business_keys=['customer_id'],
# MAGIC     compare_columns=['customer_name', 'email', 'phone'],
# MAGIC     surrogate_key_col='CustomerSK'
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Example 3: Watermark-Based Incremental Load
# MAGIC
# MAGIC ```python
# MAGIC # Get last processed timestamp
# MAGIC last_watermark = get_last_watermark('stg_sales')
# MAGIC
# MAGIC # Read only new/changed records
# MAGIC df_incremental = spark.table('external_source').filter(
# MAGIC     col('modified_at') > last_watermark
# MAGIC )
# MAGIC
# MAGIC # Process data...
# MAGIC df_incremental.write.mode('append').saveAsTable(Config.TBL_STG_SALES)
# MAGIC
# MAGIC # Update watermark
# MAGIC max_timestamp = df_incremental.agg(max('modified_at')).first()[0]
# MAGIC update_watermark('stg_sales', str(max_timestamp))
# MAGIC ```
# MAGIC
# MAGIC ### Example 4: Delete Pattern (Soft Delete)
# MAGIC
# MAGIC ```python
# MAGIC # Mark records as deleted (soft delete)
# MAGIC from delta.tables import DeltaTable
# MAGIC
# MAGIC dt = DeltaTable.forName(spark, Config.TBL_DIM_PRODUCT)
# MAGIC
# MAGIC df_deleted_ids = spark.table('deleted_product_ids')
# MAGIC
# MAGIC dt.alias('target').merge(
# MAGIC     df_deleted_ids.alias('source'),
# MAGIC     'target.product_id = source.product_id'
# MAGIC ).whenMatchedUpdate(
# MAGIC     set={'is_deleted': lit(1), 'deleted_at': current_timestamp()}
# MAGIC ).execute()
# MAGIC
# MAGIC print(" Soft delete completed")
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Production Best Practices
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Production Best Practices
# MAGIC
# MAGIC ### 1. Idempotency
# MAGIC
# MAGIC **Problem:** Pipeline retries can cause duplicate processing
# MAGIC
# MAGIC **Solution:** Check watermark before processing
# MAGIC
# MAGIC ```python
# MAGIC last_run = get_last_watermark('stg_sales')
# MAGIC if batch_timestamp <= last_run:
# MAGIC     print(f"⏭ Batch already processed (watermark: {last_run})")
# MAGIC     dbutils.notebook.exit('{"status": "skipped"}')
# MAGIC ```
# MAGIC
# MAGIC ### 2. Atomic Operations
# MAGIC
# MAGIC **Problem:** Partial failures leave data inconsistent
# MAGIC
# MAGIC **Solution:** Use Delta MERGE (atomic by design)
# MAGIC
# MAGIC ```python
# MAGIC #  GOOD: Single MERGE operation (atomic)
# MAGIC stats = upsert_merge(source_df, target_table, ['id'])
# MAGIC
# MAGIC #  BAD: Separate DELETE + INSERT (not atomic)
# MAGIC spark.sql(f"DELETE FROM {target_table} WHERE id IN (...)")
# MAGIC df.write.mode('append').saveAsTable(target_table)
# MAGIC ```
# MAGIC
# MAGIC ### 3. Error Handling
# MAGIC
# MAGIC **Problem:** Silent failures hide data quality issues
# MAGIC
# MAGIC **Solution:** Validate counts and log anomalies
# MAGIC
# MAGIC ```python
# MAGIC source_count = df_changes.count()
# MAGIC merge_stats = upsert_merge(df_changes, target_table, ['id'])
# MAGIC
# MAGIC if merge_stats['source_rows'] != source_count:
# MAGIC     raise Exception(f"Row count mismatch: expected {source_count}, got {merge_stats['source_rows']}")
# MAGIC ```
# MAGIC
# MAGIC ### 4. Performance Optimization
# MAGIC
# MAGIC **Problem:** Large MERGE operations are slow
# MAGIC
# MAGIC **Solution:** Partition pruning + Z-ORDER
# MAGIC
# MAGIC ```python
# MAGIC # Optimize target table before MERGE
# MAGIC spark.sql(f"""
# MAGIC     OPTIMIZE {target_table}
# MAGIC     ZORDER BY (customer_id, product_id)
# MAGIC """)
# MAGIC
# MAGIC # Then run MERGE
# MAGIC stats = upsert_merge(df, target_table, ['customer_id', 'product_id'])
# MAGIC ```
# MAGIC
# MAGIC ### 5. Monitoring
# MAGIC
# MAGIC **Problem:** No visibility into CDC processing
# MAGIC
# MAGIC **Solution:** Log statistics to Delta table
# MAGIC
# MAGIC ```python
# MAGIC stats = upsert_merge(df, target_table, ['id'])
# MAGIC
# MAGIC # Log to monitoring table
# MAGIC spark.createDataFrame([stats]).write.mode('append').saveAsTable(
# MAGIC     f"{Config.DATABASE}.cdc_execution_log"
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Interview Talking Points
# MAGIC
# MAGIC **Q: "How do you handle incremental data loads?"**
# MAGIC
# MAGIC **A:** "I use a CDC processor utility with three patterns:
# MAGIC
# MAGIC 1. **Upsert MERGE** for dimensions that don't need history (products, categories)
# MAGIC 2. **SCD Type 2** for tracking historical changes (customer addresses, pricing)
# MAGIC 3. **Watermark-based reads** for incremental processing (only process new/changed records)
# MAGIC
# MAGIC All three patterns use Delta Lake MERGE for atomicity and include idempotency checks via watermark tracking. When a pipeline retries, it checks the last successful watermark and skips already-processed batches.
# MAGIC
# MAGIC Real example: Customer dimension gets 5K updates daily. SCD Type 2 expires old records (sets EndDate, IsCurrent=0) and inserts new versions. This lets us query historical customer state at any point in time for compliance reporting."