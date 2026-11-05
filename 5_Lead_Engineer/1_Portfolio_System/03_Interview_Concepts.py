# Databricks notebook source
# DBTITLE 1,Interview Prep - Conceptual Questions
# MAGIC %md
# MAGIC # Interview Questions - Conceptual Understanding
# MAGIC
# MAGIC ## Purpose
# MAGIC 30 conceptual questions covering:
# MAGIC * Medallion Architecture
# MAGIC * SCD Type 2
# MAGIC * Delta Lake internals
# MAGIC * Streaming vs Batch
# MAGIC * Data Quality
# MAGIC * Performance optimization
# MAGIC
# MAGIC ## How to Use
# MAGIC 1. Read question
# MAGIC 2. Formulate answer mentally
# MAGIC 3. Reveal answer in next cell
# MAGIC 4. Practice explaining out loud
# MAGIC
# MAGIC ## Approach
# MAGIC Use SkillAIPath as examples when answering:
# MAGIC * "In my SkillAIPath project, I implemented..."
# MAGIC * "For example, when handling supplier dimensions..."
# MAGIC * "I learned this when optimizing Gold layer queries..."

# COMMAND ----------

# DBTITLE 1,Q1: Medallion Architecture
# MAGIC %md
# MAGIC ## Question 1: Explain Medallion Architecture
# MAGIC
# MAGIC **Interviewer asks:** "What is Medallion architecture and why use it over a single-layer approach?"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Your Answer:**
# MAGIC
# MAGIC "Medallion architecture is a data lakehouse pattern with three layers:
# MAGIC
# MAGIC **Bronze (Raw):** 
# MAGIC * Immutable, append-only landing zone
# MAGIC * Preserves source data exactly as received
# MAGIC * Schema-on-read, minimal transformation
# MAGIC * Enables auditability and reprocessing
# MAGIC
# MAGIC **Silver (Curated):**
# MAGIC * Cleaned, validated, deduplicated
# MAGIC * Conformed dimension tables (SCD Type 2)
# MAGIC * Fact tables with referential integrity
# MAGIC * Business-ready for analytics
# MAGIC
# MAGIC **Gold (Aggregated):**
# MAGIC * Pre-computed business metrics
# MAGIC * Denormalized for dashboard performance
# MAGIC * Optimized with ZORDER, partitioning, statistics
# MAGIC
# MAGIC **Why not single-layer?**
# MAGIC * **Auditability:** Bronze preserves raw data for compliance
# MAGIC * **Reprocessability:** Can rebuild Silver without re-ingesting
# MAGIC * **Separation of concerns:** Bronze handles ingestion, Silver handles quality, Gold handles performance
# MAGIC * **Cost efficiency:** Expensive transformations run once, cached in Silver
# MAGIC
# MAGIC **SkillAIPath example:**
# MAGIC 'When our supplier CSV format changed, I only updated Bronze ingestion. Silver-to-Gold logic remained unchanged. This saved 3 days of rework.'"
# MAGIC
# MAGIC **Key phrases for impact:**
# MAGIC * "Immutable audit trail"
# MAGIC * "Reprocess without re-ingest"
# MAGIC * "Separation of concerns"

# COMMAND ----------

# DBTITLE 1,Q2: SCD Type 2
# MAGIC %md
# MAGIC ## Question 2: SCD Type 2 Implementation
# MAGIC
# MAGIC **Interviewer asks:** "Explain SCD Type 2. Why not just use Type 1 (overwrite)?"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Your Answer:**
# MAGIC
# MAGIC "SCD Type 2 preserves historical changes to dimension records. Each version of a record gets its own row with:
# MAGIC
# MAGIC **Key columns:**
# MAGIC * `surrogate_key` - Unique identifier for each version (e.g., customer_key)
# MAGIC * `natural_key` - Business identifier (e.g., customer_id)
# MAGIC * `effective_date` - When this version became active
# MAGIC * `end_date` - When superseded (9999-12-31 if current)
# MAGIC * `is_current` - Boolean flag for latest version
# MAGIC
# MAGIC **Example scenario:**
# MAGIC Customer 'C001' changes address:
# MAGIC
# MAGIC | customer_key | customer_id | address | effective_date | end_date | is_current |
# MAGIC |--------------|-------------|---------|----------------|----------|------------|
# MAGIC | 1 | C001 | Delhi | 2023-01-01 | 2024-05-15 | FALSE |
# MAGIC | 2 | C001 | Mumbai | 2024-05-16 | 9999-12-31 | TRUE |
# MAGIC
# MAGIC **Why not Type 1 (overwrite)?**
# MAGIC * **Lost history:** Can't answer 'What was customer's address last year?'
# MAGIC * **No trend analysis:** Can't track performance before/after changes
# MAGIC * **Audit requirements:** Financial/healthcare domains need full history
# MAGIC
# MAGIC **SkillAIPath example:**
# MAGIC 'We needed to analyze: Did supplier delivery performance drop after warehouse relocation? Type 1 would've overwritten the old address. Type 2 let us compare on-time rates before vs after the move.'
# MAGIC
# MAGIC **Trade-off awareness:**
# MAGIC 'Type 2 increases storage by ~30% but unlocks historical analytics. At SkillAIPath scale (8 dimensions, 50K records), that's 15MB - trivial compared to analytical value.'"
# MAGIC
# MAGIC **Key phrases:**
# MAGIC * "Preserves historical context"
# MAGIC * "Surrogate vs natural key"
# MAGIC * "Trade-off: storage for analytical flexibility"

# COMMAND ----------

# DBTITLE 1,Q3-10: More Conceptual Questions
# MAGIC %md
# MAGIC ## Question 3: Delta Lake ACID Guarantees
# MAGIC
# MAGIC **Q:** "What ACID guarantees does Delta Lake provide?"
# MAGIC
# MAGIC **A:** "Delta Lake provides full ACID transactions:
# MAGIC * **Atomicity:** Writes either fully complete or fully fail (no partial writes)
# MAGIC * **Consistency:** Schema enforcement + constraints prevent invalid data
# MAGIC * **Isolation:** MVCC (multi-version concurrency control) - readers see consistent snapshots
# MAGIC * **Durability:** Transaction log persists all operations
# MAGIC
# MAGIC SkillAIPath example: My CDC merge logic writes 50K fact_sales records + updates 5K existing. If process crashes mid-write, Delta Lake rolls back entirely. No orphaned or partially updated records."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Question 4: Auto Loader vs Spark Structured Streaming
# MAGIC
# MAGIC **Q:** "Why use Auto Loader instead of manual Spark Structured Streaming?"
# MAGIC
# MAGIC **A:** "Auto Loader is Databricks' enhanced file ingestion:
# MAGIC
# MAGIC **Advantages:**
# MAGIC * Schema inference handles evolving schemas automatically
# MAGIC * File notification (cloud events) vs expensive directory listing
# MAGIC * Checkpoint management built-in (exactly-once guarantee)
# MAGIC * Rescues data that doesn't match schema (quarantine bad records)
# MAGIC * 70% less boilerplate code
# MAGIC
# MAGIC **When to use manual streaming:**
# MAGIC * Kafka/Kinesis sources (not files)
# MAGIC * Custom deduplication logic
# MAGIC * Complex watermarking beyond Auto Loader's capabilities
# MAGIC
# MAGIC SkillAIPath example: Source team added 'loyalty_tier' column without notice. Auto Loader detected new column, merged schema, backfilled NULLs for historical records. Manual streaming would've crashed."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Question 5: OPTIMIZE vs ZORDER
# MAGIC
# MAGIC **Q:** "Explain OPTIMIZE and ZORDER. When to use each?"
# MAGIC
# MAGIC **A:** 
# MAGIC
# MAGIC **OPTIMIZE:**
# MAGIC * Compacts small files into larger ones
# MAGIC * Target file size ~1GB for optimal I/O
# MAGIC * Reclaims space from deleted/updated records
# MAGIC * Run after heavy writes
# MAGIC
# MAGIC **ZORDER:**
# MAGIC * Colocates related data within files
# MAGIC * Multi-dimensional clustering (not partitioning)
# MAGIC * Improves data skipping effectiveness
# MAGIC * Specify columns frequently used in filters/joins
# MAGIC
# MAGIC **Usage pattern:**
# MAGIC ```sql
# MAGIC OPTIMIZE table_name ZORDER BY (col1, col2)
# MAGIC ```
# MAGIC
# MAGIC **Column selection for ZORDER:**
# MAGIC * Primary/foreign keys (customer_key, product_key)
# MAGIC * Frequently filtered columns (date, status)
# MAGIC * High cardinality preferred
# MAGIC
# MAGIC **SkillAIPath results:**
# MAGIC * Applied to fact_sales: `ZORDER BY (customer_key, product_key, date_key)`
# MAGIC * File scan reduced from 500 files → 60 files
# MAGIC * Query time: 30s → 3s (90% improvement)
# MAGIC
# MAGIC Trade-off: OPTIMIZE takes time (5 min for 5M rows) but pays off in every subsequent query."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Question 6: Streaming vs Batch
# MAGIC
# MAGIC **Q:** "When should you use streaming vs batch processing?"
# MAGIC
# MAGIC **A:**
# MAGIC
# MAGIC **Use Streaming when:**
# MAGIC * Sub-minute latency required (fraud detection, monitoring)
# MAGIC * Data arrives continuously (IoT, clickstreams)
# MAGIC * Event-time processing matters (out-of-order events)
# MAGIC * Example: SkillAIPath could stream inventory updates for real-time stock levels
# MAGIC
# MAGIC **Use Batch when:**
# MAGIC * Hourly/daily latency acceptable (reporting dashboards)
# MAGIC * Data arrives in bulk (nightly database exports)
# MAGIC * Complex aggregations across large windows
# MAGIC * Example: SkillAIPath Gold layer runs daily - historical trends don't need real-time
# MAGIC
# MAGIC **Hybrid approach (SkillAIPath):**
# MAGIC * Bronze: Streaming with Auto Loader (incremental ingestion)
# MAGIC * Silver: Micro-batch every 30 min (CDC merge)
# MAGIC * Gold: Batch daily (full aggregation)
# MAGIC
# MAGIC **Key insight:** 'Streaming is a continuous batch' - same transformations, different trigger intervals."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Question 7: Data Quality Framework
# MAGIC
# MAGIC **Q:** "How do you ensure data quality in pipelines?"
# MAGIC
# MAGIC **A:** "Multi-layer validation:
# MAGIC
# MAGIC **Bronze Layer:**
# MAGIC * Schema validation (expected columns present)
# MAGIC * Record count checks (source vs ingested)
# MAGIC * Duplicate detection (log duplicates, don't block)
# MAGIC
# MAGIC **Silver Layer:**
# MAGIC * Referential integrity (foreign keys exist in dimensions)
# MAGIC * Null checks on critical columns (customer_id, order_amount)
# MAGIC * Range validation (dates not in future, amounts > 0)
# MAGIC * Uniqueness constraints (primary key uniqueness)
# MAGIC
# MAGIC **Gold Layer:**
# MAGIC * Business rule validation (total_sales = SUM(line_items))
# MAGIC * Completeness checks (all expected aggregates present)
# MAGIC * Outlier detection (flag values >3 stddev from mean)
# MAGIC
# MAGIC **SkillAIPath implementation:**
# MAGIC Built utils/data_quality_tests.ipynb with 15 checks. Failed checks:
# MAGIC * Block downstream (critical: null customer_id)
# MAGIC * Log and alert (warning: duplicate records)
# MAGIC
# MAGIC Result: Caught 3 logic bugs before production. Tests run automatically after each pipeline execution."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Question 8: Idempotency
# MAGIC
# MAGIC **Q:** "What is idempotency and why does it matter?"
# MAGIC
# MAGIC **A:** "Idempotency means: Running a pipeline multiple times produces the same result as running once.
# MAGIC
# MAGIC **Why it matters:**
# MAGIC * Pipelines fail mid-run (OOM, cluster termination)
# MAGIC * Need to reprocess without creating duplicates
# MAGIC * Enables safe retries
# MAGIC
# MAGIC **How to achieve:**
# MAGIC
# MAGIC **Method 1: Watermarks**
# MAGIC * Track last successful run timestamp
# MAGIC * Process only data after watermark
# MAGIC * Update watermark atomically after success
# MAGIC * SkillAIPath: watermark_manager.ipynb stores per-table timestamps
# MAGIC
# MAGIC **Method 2: MERGE (Upserts)**
# MAGIC * Use MERGE instead of INSERT
# MAGIC * ON primary_key match: UPDATE; no match: INSERT
# MAGIC * Rerunning updates existing records instead of duplicating
# MAGIC
# MAGIC **Method 3: Checkpoints (Auto Loader)**
# MAGIC * Tracks processed files
# MAGIC * Rerun skips already-processed files
# MAGIC
# MAGIC **SkillAIPath example:**
# MAGIC 'Bronze uses Auto Loader checkpoints. Silver uses watermarks + MERGE. If my nightly job fails at 2 AM, I can rerun at 3 AM. Only incremental data since last success gets reprocessed.'"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Question 9: Partitioning Strategy
# MAGIC
# MAGIC **Q:** "How do you decide partitioning strategy?"
# MAGIC
# MAGIC **A:**
# MAGIC
# MAGIC **Partition when:**
# MAGIC * Queries frequently filter on specific columns (date, region)
# MAGIC * Data volume >1TB per table
# MAGIC * Time-based retention (DROP partition vs DELETE rows)
# MAGIC
# MAGIC **Don't partition when:**
# MAGIC * Table <100GB (overhead > benefit)
# MAGIC * Filter columns change often
# MAGIC * Queries scan full table anyway
# MAGIC
# MAGIC **Partitioning guidelines:**
# MAGIC * **Cardinality:** 100-10,000 partitions ideal (not 10, not 1M)
# MAGIC * **Size:** 100MB-1GB per partition
# MAGIC * **Patterns:** Date is most common (YYYY-MM-DD)
# MAGIC
# MAGIC **SkillAIPath decision:**
# MAGIC * Bronze/Silver: No partitioning (<10GB tables)
# MAGIC * Gold fact_sales: NO partition, used ZORDER instead
# MAGIC * Why? Queries filter by date AND customer AND product. Partitioning by date would force full partition scan for customer queries.
# MAGIC
# MAGIC **Alternative: ZORDER > Partitioning**
# MAGIC When multi-dimensional filters (date + customer + product), ZORDER outperforms single-dimension partitioning.
# MAGIC
# MAGIC **Key phrase:** 'Partition on how you query, not how you ingest.'"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Question 10: Time Travel
# MAGIC
# MAGIC **Q:** "Explain Delta Lake Time Travel use cases."
# MAGIC
# MAGIC **A:** "Time Travel queries historical versions of Delta tables using:
# MAGIC
# MAGIC **Syntax:**
# MAGIC ```sql
# MAGIC -- By timestamp
# MAGIC SELECT * FROM table_name TIMESTAMP AS OF '2024-05-15 10:00:00'
# MAGIC
# MAGIC -- By version number
# MAGIC SELECT * FROM table_name VERSION AS OF 42
# MAGIC ```
# MAGIC
# MAGIC **Use cases:**
# MAGIC
# MAGIC **1. Audit/Compliance:**
# MAGIC 'Show me customer data as it existed on Dec 31 for year-end report'
# MAGIC
# MAGIC **2. Debugging:**
# MAGIC 'Dashboard shows wrong numbers. Let's compare today's data vs yesterday's.'
# MAGIC ```sql
# MAGIC SELECT * FROM sales TIMESTAMP AS OF CURRENT_DATE() - 1
# MAGIC ```
# MAGIC
# MAGIC **3. Recovery:**
# MAGIC 'Accidentally ran DELETE instead of UPDATE. Restore from 1 hour ago.'
# MAGIC ```sql
# MAGIC CREATE OR REPLACE TABLE sales AS 
# MAGIC SELECT * FROM sales TIMESTAMP AS OF '2024-05-24 14:00:00'
# MAGIC ```
# MAGIC
# MAGIC **4. A/B Testing:**
# MAGIC 'Compare model predictions on Version 10 vs Version 15 data.'
# MAGIC
# MAGIC **SkillAIPath implementation:**
# MAGIC Built utils/time_travel_recovery.ipynb:
# MAGIC * List recent versions: DESCRIBE HISTORY table_name
# MAGIC * Compare versions: Join current vs VERSION AS OF N
# MAGIC * Restore: Documented rollback procedures
# MAGIC
# MAGIC **Retention:** Delta log retained for 30 days by default (configurable)."
# MAGIC
# MAGIC **Key phrases:**
# MAGIC * "Audit without copying data"
# MAGIC * "Rollback without backups"
# MAGIC * "Compare versions for debugging"