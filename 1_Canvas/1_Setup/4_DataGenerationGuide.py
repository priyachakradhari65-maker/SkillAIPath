# Databricks notebook source
# DBTITLE 1,SkillAIPath Data Generation Guide
# MAGIC %md
# MAGIC #  SkillAIPath Data Generation Guide
# MAGIC **User-Friendly Guide to Bronze Layer Data Creation**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ##  What Does This Do?
# MAGIC
# MAGIC The `generate_batch` notebook creates **realistic B2B wholesale FMCG sales data** for SkillAIPath, a mid-sized Indian distributor. It simulates:
# MAGIC
# MAGIC * **1.1 million transactions** over 3 years (May 2023 - May 2026)
# MAGIC * **5,000+ B2B customers** placing bulk orders
# MAGIC * **44 products** across 8 categories (rice, wheat, dairy, etc.)
# MAGIC * **Daily batch processing** with realistic pricing, discounts, and GST
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ️ Two Operating Modes
# MAGIC
# MAGIC ### **Mode 1: Initial Historical Load (One-Time)**
# MAGIC **Purpose:** Generate 3 years of historical data to populate the data warehouse
# MAGIC
# MAGIC **What it does:**
# MAGIC * Creates 1.1M records spanning May 29, 2023 → May 29, 2026
# MAGIC * Writes to `{Config.SCHEMA_BRONZE}.bronze_stg_sales` table
# MAGIC * Takes ~25-30 minutes to complete
# MAGIC * Uses monthly batched generation (37 batches × ~30K records each)
# MAGIC
# MAGIC **When to use:** First time setting up SkillAIPath, or after truncating bronze table
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Mode 2: Daily Incremental Load (Ongoing)**
# MAGIC **Purpose:** Add 2,000 new records for a single day (simulates daily operations)
# MAGIC
# MAGIC **What it does:**
# MAGIC * Appends 2K records for a specific date (e.g., May 30, 2026)
# MAGIC * Takes ~2-3 minutes
# MAGIC * Preserves existing data (append only)
# MAGIC * Triggers downstream dimension/fact/gold refresh
# MAGIC
# MAGIC **When to use:** After initial load, to simulate daily batch processing
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Quick Start - Initial Load
# MAGIC %md
# MAGIC ## [QUICK] QUICK START: Initial 3-Year Load
# MAGIC
# MAGIC ### **Step 1: Navigate to generate_batch notebook**
# MAGIC ```
# MAGIC /Workspace../00_UTILS/generate_batch
# MAGIC ```
# MAGIC
# MAGIC ### **Step 2: Run Cell 3 (OVERRIDE: 3-Year Initial Load)**
# MAGIC **This cell hardcodes all parameters for you:**
# MAGIC * Batch Date: 2026-05-29 (yesterday)
# MAGIC * Total Records: 1,095,000 (1K per day × 1,095 days)
# MAGIC * Load Type: initial (overwrite)
# MAGIC * Date Range: May 29, 2023 → May 29, 2026
# MAGIC
# MAGIC **No widget changes needed!** Just run the cell.
# MAGIC
# MAGIC ### **Step 3: Run Cell 4 (Monthly Batch Plan)**
# MAGIC **This cell calculates:**
# MAGIC * 37 monthly batches
# MAGIC * ~30K records per batch
# MAGIC * Memory-optimized iteration plan
# MAGIC
# MAGIC **Output:** Shows batch plan with date ranges
# MAGIC
# MAGIC ### **Step 4: Run Cell 5 (EXECUTE Monthly Batches)**
# MAGIC **This cell generates all data:**
# MAGIC * Loops through 37 monthly batches
# MAGIC * Generates ~30K records per month using dbldatagen
# MAGIC * Writes immediately to Delta Lake (no memory accumulation)
# MAGIC * Shows progress: "BATCH 12/37: 2024-04-01 → 2024-04-30 (30 days)"
# MAGIC
# MAGIC **Duration:** ~25-30 minutes  
# MAGIC **Output:** 1.1M records in `{Config.SCHEMA_BRONZE}.bronze_stg_sales`
# MAGIC
# MAGIC ### **Step 5: Verify Data**
# MAGIC Run Cell 21 (Validation Query):
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   COUNT(*) as total_records,
# MAGIC   MIN(BatchDate) as first_batch,
# MAGIC   MAX(BatchDate) as last_batch,
# MAGIC   COUNT(DISTINCT BatchDate) as batch_count
# MAGIC FROM {Config.SCHEMA_BRONZE}.bronze_stg_sales
# MAGIC ```
# MAGIC
# MAGIC **Expected Output:**
# MAGIC * total_records: ~1,097,000
# MAGIC * first_batch: 2023-05-29
# MAGIC * last_batch: 2026-05-29
# MAGIC * batch_count: 37
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Quick Start - Daily Incremental
# MAGIC %md
# MAGIC ## [QUICK] QUICK START: Daily Incremental Load
# MAGIC
# MAGIC **Prerequisites:** Initial 3-year load already completed
# MAGIC
# MAGIC ### **Step 1: Open generate_batch notebook**
# MAGIC
# MAGIC ### **Step 2: Update Widgets (Top of Notebook)**
# MAGIC Change these widget values:
# MAGIC
# MAGIC | Widget | New Value | Purpose |
# MAGIC |--------|-----------|----------|
# MAGIC | `batch_date` | `2026-05-31` | Tomorrow's date |
# MAGIC | `row_count` | `2000` | Daily batch size |
# MAGIC | `load_type` | `incremental` | Append mode |
# MAGIC | `order_date_start` | `2026-05-31` | Same as batch_date |
# MAGIC | `order_date_end` | `2026-05-31` | Same as batch_date |
# MAGIC
# MAGIC **Leave other widgets unchanged** (MFD, EXP dates can stay as-is)
# MAGIC
# MAGIC ### **Step 3: Run Cell 2 (Parameters)**
# MAGIC Verify output shows:
# MAGIC ```
# MAGIC [PASS] INCREMENTAL MODE: Adding 2,000 records for 2026-05-31
# MAGIC ```
# MAGIC
# MAGIC ### **Step 4: Skip Cell 3!**
# MAGIC **DO NOT RUN Cell 3** (3-Year Override) - it will overwrite your data!
# MAGIC
# MAGIC ### **Step 5: Run Cells 9-12 (Data Generation)**
# MAGIC * Cell 9: Install libraries
# MAGIC * Cell 10: Import config
# MAGIC * Cell 11: Validate config
# MAGIC * Cell 12: Generate 2K records for 2026-05-31
# MAGIC
# MAGIC **Duration:** ~2-3 minutes  
# MAGIC **Output:** 2,000 new records appended
# MAGIC
# MAGIC ### **Step 6: Verify Incremental Load**
# MAGIC Run Cell 21 again - `total_records` should increase by 2,000:
# MAGIC ```
# MAGIC Before: 1,097,000
# MAGIC After: 1,099,000 (+2,000)
# MAGIC ```
# MAGIC
# MAGIC ### **Step 7: Refresh Downstream Tables**
# MAGIC After bronze update, re-run:
# MAGIC 1. Dimensions (01_DIMENSIONS folder)
# MAGIC 2. Facts (02_FACTS folder)
# MAGIC 3. Gold Marts (03_GOLD folder)
# MAGIC
# MAGIC Or use the master_pipeline orchestration notebook.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Widget Parameters Explained
# MAGIC %md
# MAGIC ## ️ Widget Parameters Explained
# MAGIC
# MAGIC ### **Core Parameters**
# MAGIC
# MAGIC #### `batch_date` (Text)
# MAGIC **Purpose:** The date for this batch (used for partitioning and audit trail)
# MAGIC
# MAGIC * **Initial Load:** Set to last day of historical range (e.g., `2026-05-29`)
# MAGIC * **Incremental:** Set to today's date (e.g., `2026-05-31`)
# MAGIC
# MAGIC **Example:** `2026-05-31`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### `row_count` (Text)
# MAGIC **Purpose:** Total number of records to generate
# MAGIC
# MAGIC * **Initial Load:** `1095000` (1K per day × 1,095 days)
# MAGIC * **Incremental:** `2000` (daily batch size)
# MAGIC
# MAGIC **Note:** For initial load, Cell 3 overrides this to 1,095,000 automatically.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### `load_type` (Dropdown)
# MAGIC **Purpose:** How to write data to the table
# MAGIC
# MAGIC * **`initial`:** Overwrites existing table (first-time load)
# MAGIC * **`incremental`:** Appends new records (daily batches)
# MAGIC
# MAGIC **Default:** `incremental`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### `scenario` (Dropdown)
# MAGIC **Purpose:** Data generation pattern (mostly informational)
# MAGIC
# MAGIC * **`incremental_batch`:** Daily batch loading (default)
# MAGIC * **`flat_dump`:** Single large file (Excel baseline)
# MAGIC * **`scd_type2`:** Customer history tracking
# MAGIC * **`factless_events`:** Event-based data
# MAGIC * **`cdc_stream`:** Change data capture
# MAGIC
# MAGIC **Default:** `incremental_batch`  
# MAGIC **Note:** Most scenarios use incremental_batch. Others are for advanced use cases.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Date Range Parameters**
# MAGIC
# MAGIC #### `order_date_start` / `order_date_end` (Text)
# MAGIC **Purpose:** Date range for OrderDate column in generated data
# MAGIC
# MAGIC * **Initial Load:** Full 3-year range (`2023-05-29` to `2026-05-29`)
# MAGIC * **Incremental:** Single day (`2026-05-31` to `2026-05-31`)
# MAGIC
# MAGIC **Example (Incremental):**
# MAGIC ```
# MAGIC order_date_start = "2026-05-31"
# MAGIC order_date_end = "2026-05-31"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### `mfd_date_start` / `mfd_date_end` (Text)
# MAGIC **Purpose:** Manufacture date range for products
# MAGIC
# MAGIC **Default:** `2023-05-29` to `2026-05-29` (3 years)  
# MAGIC **Note:** MFD dates are randomly assigned within this range. For incremental loads, keep these unchanged.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### `exp_date_start` / `exp_date_end` (Text)
# MAGIC **Purpose:** Expiry date range for products
# MAGIC
# MAGIC **Default:** `2026-05-29` to `2030-05-29` (4 years from MFD end)  
# MAGIC **Note:** EXP dates are calculated as MFD + 365 days. Keep these unchanged for incremental loads.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### `delivery_days_offset` (Text)
# MAGIC **Purpose:** Days between order and delivery
# MAGIC
# MAGIC **Default:** `4` (delivery = order date + 4 days)  
# MAGIC **Note:** Fixed offset, rarely needs changing.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Common Scenarios
# MAGIC %md
# MAGIC ##  Common Scenarios
# MAGIC
# MAGIC ### **Scenario 1: First-Time Setup**
# MAGIC **Goal:** Generate 3 years of historical data
# MAGIC
# MAGIC **Steps:**
# MAGIC 1. Open `generate_batch` notebook
# MAGIC 2. Run Cell 3 (hardcoded 3-year parameters)
# MAGIC 3. Run Cell 4 (monthly batch plan)
# MAGIC 4. Run Cell 5 (execute 37 monthly batches)
# MAGIC 5. Wait ~25-30 minutes
# MAGIC 6. Verify with Cell 21
# MAGIC
# MAGIC **Result:** 1.1M records in bronze_stg_sales
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Scenario 2: Daily Batch (Next Day)**
# MAGIC **Goal:** Add 2K records for May 31, 2026
# MAGIC
# MAGIC **Widget Changes:**
# MAGIC ```
# MAGIC batch_date = "2026-05-31"
# MAGIC row_count = "2000"
# MAGIC load_type = "incremental"
# MAGIC order_date_start = "2026-05-31"
# MAGIC order_date_end = "2026-05-31"
# MAGIC ```
# MAGIC
# MAGIC **Steps:**
# MAGIC 1. Update widgets (above)
# MAGIC 2. Run Cell 2 (verify incremental mode)
# MAGIC 3. Skip Cell 3!
# MAGIC 4. Run Cells 9-12 (generate 2K records)
# MAGIC 5. Verify with Cell 21 (should show +2K)
# MAGIC
# MAGIC **Result:** 1,099,000 total records
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Scenario 3: Backfill Missing Date**
# MAGIC **Goal:** Add data for a date we skipped (e.g., May 30, 2026)
# MAGIC
# MAGIC **Widget Changes:**
# MAGIC ```
# MAGIC batch_date = "2026-05-30"
# MAGIC row_count = "2000"
# MAGIC load_type = "incremental"
# MAGIC order_date_start = "2026-05-30"
# MAGIC order_date_end = "2026-05-30"
# MAGIC ```
# MAGIC
# MAGIC **Steps:** Same as Scenario 2
# MAGIC
# MAGIC **Note:** Idempotency check (Cell 13) prevents duplicates if you run twice for the same date.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Scenario 4: Regenerate Everything**
# MAGIC **Goal:** Start over from scratch
# MAGIC
# MAGIC **Steps:**
# MAGIC 1. Truncate bronze table:
# MAGIC    ```sql
# MAGIC    TRUNCATE TABLE {Config.SCHEMA_BRONZE}.bronze_stg_sales
# MAGIC    ```
# MAGIC 2. Follow Scenario 1 (first-time setup)
# MAGIC
# MAGIC **Warning:** This deletes all existing bronze data!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Scenario 5: Generate 1 Week of Data**
# MAGIC **Goal:** Add 7 days of incremental batches
# MAGIC
# MAGIC **Steps:**
# MAGIC 1. Set `batch_date = "2026-06-01"`, `row_count = "2000"`, `load_type = "incremental"`
# MAGIC 2. Set `order_date_start = order_date_end = "2026-06-01"`
# MAGIC 3. Run Cells 9-12
# MAGIC 4. Repeat for dates 2026-06-02, 2026-06-03, ..., 2026-06-07
# MAGIC
# MAGIC **Result:** +14,000 records (7 days × 2K)
# MAGIC
# MAGIC **Alternative:** Use orchestration notebook to automate this loop.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Troubleshooting
# MAGIC %md
# MAGIC ##  Troubleshooting
# MAGIC
# MAGIC ### **Issue 1: "Table already has data for this batch date"**
# MAGIC **Symptom:** Idempotency check (Cell 13) shows existing records for batch_date
# MAGIC
# MAGIC **Cause:** You already ran this batch, or bronze table has data for this date
# MAGIC
# MAGIC **Solutions:**
# MAGIC 1. **Skip and continue:** Data already loaded, move to next step
# MAGIC 2. **Overwrite batch:** Delete existing batch first:
# MAGIC    ```sql
# MAGIC    DELETE FROM {Config.SCHEMA_BRONZE}.bronze_stg_sales 
# MAGIC    WHERE BatchDate = '2026-05-31'
# MAGIC    ```
# MAGIC    Then re-run Cells 9-12
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Issue 2: "OOM (Out of Memory) Error"**
# MAGIC **Symptom:** Cell 5 fails with Java heap space or OOM error
# MAGIC
# MAGIC **Cause:** Trying to generate too many records at once
# MAGIC
# MAGIC **Solutions:**
# MAGIC 1. **Use Cell 5 (monthly batched generation):** Already memory-optimized for 1.1M records
# MAGIC 2. **Reduce batch size:** If still failing, edit Cell 5 to generate 15-day batches instead of monthly
# MAGIC 3. **Increase cluster size:** Switch to larger compute (not recommended for engineers)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Issue 3: "Cell 3 overwrites my incremental data!"**
# MAGIC **Symptom:** After running Cell 3, bronze table only has 1.1M records (historical load)
# MAGIC
# MAGIC **Cause:** Cell 3 uses `load_type = "initial"` which overwrites existing data
# MAGIC
# MAGIC **Solution:**
# MAGIC * **For incremental loads, SKIP Cell 3 entirely**
# MAGIC * Only run Cell 3 once during first-time setup
# MAGIC * For daily batches, run Cells 2, 9-12 only
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Issue 4: "Validation query shows wrong record count"**
# MAGIC **Symptom:** Cell 21 shows unexpected total_records
# MAGIC
# MAGIC **Expected Counts:**
# MAGIC * After initial load: ~1,097,000
# MAGIC * After 1 incremental batch: ~1,099,000
# MAGIC * After 5 incremental batches: ~1,107,000
# MAGIC
# MAGIC **Debugging:**
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   BatchDate,
# MAGIC   COUNT(*) as records_per_batch
# MAGIC FROM {Config.SCHEMA_BRONZE}.bronze_stg_sales
# MAGIC GROUP BY BatchDate
# MAGIC ORDER BY BatchDate DESC
# MAGIC LIMIT 10
# MAGIC ```
# MAGIC
# MAGIC Check if your incremental batch appears in the list.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Issue 5: "Widgets not updating"**
# MAGIC **Symptom:** Changed widget values but Cell 2 shows old values
# MAGIC
# MAGIC **Solution:**
# MAGIC 1. Change widget value in the widget panel (top of notebook)
# MAGIC 2. Re-run Cell 2 (Parameters) to reload widget values
# MAGIC 3. Verify output shows new values
# MAGIC
# MAGIC **Note:** Widget changes don't take effect until you re-run Cell 2.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Issue 6: "Bronze data exists but dimensions are empty"**
# MAGIC **Symptom:** bronze_stg_sales has 1.1M records, but dim_customer / dim_product are empty
# MAGIC
# MAGIC **Cause:** Dimension notebooks haven't been run yet
# MAGIC
# MAGIC **Solution:**
# MAGIC 1. Navigate to `/Workspace../01_DIMENSIONS/`
# MAGIC 2. Run all 8 dimension notebooks (or use master_pipeline orchestration)
# MAGIC 3. Then run facts (02_FACTS) and gold (03_GOLD)
# MAGIC
# MAGIC **Flow:** Bronze → Dimensions → Facts → Gold
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Issue 7: "dbldatagen library not found"**
# MAGIC **Symptom:** `ModuleNotFoundError: No module named 'dbldatagen'`
# MAGIC
# MAGIC **Solution:**
# MAGIC * Run Cell 9 (Install Libraries): `%pip install faker dbldatagen --quiet`
# MAGIC * Wait for installation to complete
# MAGIC * Then run Cells 10-12
# MAGIC
# MAGIC ---