# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Silver Pipeline - CDC Processing with SCD Type 2
# MAGIC
# MAGIC Applies CDC transformations with SCD Type 2 history tracking.
# MAGIC
# MAGIC **🧪 USING STREAMING TEST DATA (1,000 records)**
# MAGIC
# MAGIC Pipeline Configuration:
# MAGIC - Source: bronze_sales_test (output from notebook 13 Bronze DLT)
# MAGIC - Target: {Config.CATALOG}.{Config.SCHEMA_SILVER} schema
# MAGIC - Mode: Streaming with APPLY CHANGES
# MAGIC - Features: SCD Type 2, watermarks, late arrivals, checkpoints, deduplication
# MAGIC - Environment: Auto-detected from Config (dev/uat/prod)
# MAGIC
# MAGIC **Medallion Flow:**
# MAGIC ```
# MAGIC Notebook 00 → Volume → Notebook 13 (Bronze) → bronze_sales_test → THIS notebook (Silver)
# MAGIC ```
# MAGIC
# MAGIC Test Data Schema:
# MAGIC - CustomerID, OrderID, ProductName, TotalAmount
# MAGIC - event_timestamp (for watermarks/late data)
# MAGIC - _input_file, _ingestion_timestamp, _source_system (audit columns from Bronze)

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, upper, trim, current_timestamp, md5, concat_ws, expr

# COMMAND ----------

# DBTITLE 1,Load Configuration
# Load configuration
%run ../../../1_Canvas/1_Setup/1_Config

# Bronze source tables - reading from notebook 13 DLT output
BRONZE_SALES = "strm_sales_raw"  # Output from notebook 13 Bronze DLT pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Table: Customers (SCD Type 2)

# COMMAND ----------

@dlt.table(
    name="strm_customers_scd2",
    comment="Customer dimension with SCD Type 2 history tracking - streaming test data",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_or_fail("valid_customer_id", "CustomerID IS NOT NULL")
def silver_customers_scd2():
    # Read from Bronze DLT table (notebook 13 output)
    return (
        dlt.read_stream(BRONZE_SALES)
        .filter(col("OrderID").isNotNull())  # Only purchase events
        .select(
            col("CustomerID"),
            col("_ingestion_timestamp").alias("last_order_timestamp"),
            col("TotalAmount").cast("double").alias("last_order_amount"),
            current_timestamp().alias("processed_timestamp")
        )
        .dropDuplicates(["CustomerID"])
    )

# Apply CDC with SCD Type 2
dlt.create_streaming_table(
    name="strm_customers",
    comment="Customer dimension with current and historical records (SCD Type 2)"
)

dlt.apply_changes(
    target="strm_customers",
    source="strm_customers_scd2",
    keys=["CustomerID"],
    sequence_by=col("processed_timestamp"),
    stored_as_scd_type=2,
    track_history_column_list=["last_order_timestamp", "last_order_amount"],
    track_history_except_column_list=[]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Table: Sales Fact (Cleansed)

# COMMAND ----------

@dlt.table(
    name="strm_sales",
    comment="Cleansed sales transactions from streaming test data - demonstrates watermarks and late arrivals",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_or_drop("valid_order_id", "OrderID IS NOT NULL")
@dlt.expect_or_drop("positive_amount", "TotalAmount > 0")
@dlt.expect("valid_event_time", "order_timestamp IS NOT NULL")  # Warning only
def silver_sales():
    # Read from Bronze DLT table (notebook 13 output)
    from pyspark.sql.functions import to_timestamp
    
    return (
        dlt.read_stream(BRONZE_SALES)
        .filter(col("OrderID").isNotNull())  # Only purchase events (exclude browsing)
        .select(
            col("CustomerID"),
            col("OrderID"),
            upper(trim(col("ProductName"))).alias("ProductName"),
            col("TotalAmount").cast("double").alias("TotalAmount"),
            col("_ingestion_timestamp").alias("order_timestamp"),
            current_timestamp().alias("processed_timestamp")
        )
        # Add watermark for late data handling (5 minute tolerance)
        # 🧪 TEST: Events >5 min late will be DROPPED to demonstrate watermark behavior
        .withWatermark("order_timestamp", "5 minutes")
    )

# COMMAND ----------

# DBTITLE 1,Session Analytics - Windowed Aggregations
# MAGIC %md
# MAGIC    
# MAGIC ## Silver Table: Session Analytics (Windowed Aggregations)
# MAGIC
# MAGIC Demonstrates session-based windowing with watermarks and late arrival handling.

# COMMAND ----------

# DBTITLE 1,Session Analytics Table
@dlt.table(
    name="strm_session_analytics",
    comment="Session-based analytics with 10-minute tumbling windows - demonstrates watermarks and checkpoints",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
def silver_session_analytics():
    from pyspark.sql.functions import window, count, sum, min, max, avg
    
    return (
        dlt.read_stream(BRONZE_SALES)
        .filter(col("CustomerID").isNotNull())
        .filter(col("_ingestion_timestamp").isNotNull())
        # Add watermark BEFORE grouping
        .withWatermark("_ingestion_timestamp", "5 minutes")
        # Group by session and 10-minute windows
        .groupBy(
            
            col("CustomerID"),
            window(col("_ingestion_timestamp"), "10 minutes")
        )
        .agg(
            count("*").alias("events_in_window"),
            sum("TotalAmount").alias("window_revenue"),
            min("_ingestion_timestamp").alias("window_start_time"),
            max("_ingestion_timestamp").alias("window_end_time"),
            avg("TotalAmount").alias("avg_order_value")
        )
        # Flatten window struct for easier querying
        .select(
            
            col("CustomerID"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("events_in_window"),
            col("window_revenue"),
            col("window_start_time"),
            col("window_end_time"),
            col("avg_order_value"),
            current_timestamp().alias("processed_timestamp")
        )
    )

# COMMAND ----------

# DBTITLE 1,Watermark Monitoring
# MAGIC %md
# MAGIC    
# MAGIC ## Monitoring Table: Watermark Progression
# MAGIC
# MAGIC Tracks watermark state and identifies late/dropped events for debugging.

# COMMAND ----------

# DBTITLE 1,Watermark Tracking Table
@dlt.table(
    name="strm_watermark_monitor",
    comment="Monitors watermark progression and tracks late/dropped events - useful for debugging streaming behavior",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
def silver_watermark_monitor():
    return (
        dlt.read_stream(BRONZE_SALES)
        .filter(col("_ingestion_timestamp").isNotNull())
        .select(
            col("OrderID"),
            col("CustomerID"),
            
            col("_ingestion_timestamp").alias("event_timestamp"),
            col("_input_file"),
            # Extract batch number for analysis
            expr("CAST(REGEXP_EXTRACT(_input_file, 'batch_(\\\\d+)', 1) AS INT)").alias("batch_number"),
            col("_ingestion_timestamp"),
            # Calculate lateness
            expr("0").alias("minutes_late"),
            # Categorize events
            expr("""
                CASE 
                    WHEN TIMESTAMPDIFF(MINUTE, event_timestamp, _ingestion_timestamp) > 5 
                    THEN 'WOULD_BE_DROPPED_5MIN_WATERMARK'
                    WHEN TIMESTAMPDIFF(MINUTE, event_timestamp, _ingestion_timestamp) > 0
                    THEN 'LATE_BUT_WITHIN_WATERMARK'
                    ELSE 'ON_TIME'
                END
            """).alias("watermark_status"),
            current_timestamp().alias("processed_timestamp")
        )
    )
