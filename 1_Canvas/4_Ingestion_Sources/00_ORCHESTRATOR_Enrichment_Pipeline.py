# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Enrichment Pipeline Orchestrator - Parallel Execution
# MAGIC %md
# MAGIC # External Enrichment Pipeline - Complete Orchestrator (Parallel)
# MAGIC
# MAGIC **Purpose:** Orchestrate full Bronze → Silver → Gold enrichment pipeline with parallel execution
# MAGIC
# MAGIC **Bronze Layer (4 stages, parallel):**
# MAGIC 1. **Supplier Network** (100K+ suppliers, 50K+ parts, 500K+ relationships)
# MAGIC 2. **Weather Data** (55K+ daily weather records, 30 cities, 5 years)
# MAGIC 3. **Exchange Rates & Commodities** (73K+ FX/commodity prices, 5 years)
# MAGIC 4. **Logistics Deliveries** (500K+ delivery transactions, 2 years)
# MAGIC
# MAGIC **Silver Layer (2 stages, parallel):**
# MAGIC 5. **Supplier Cost Benchmark** - Compare actual costs vs market benchmarks
# MAGIC 6. **Weather Sales Correlation** - Join weather data with sales facts
# MAGIC
# MAGIC **Gold Layer (2 stages, parallel):**
# MAGIC 7. **Supplier Performance Analytics** - Scorecard + cost optimization marts
# MAGIC 8. **Weather Impact Analytics** - Weather-sensitive products + sales impact
# MAGIC
# MAGIC **Execution:** All stages within each layer run in parallel using ThreadPoolExecutor. Layers execute sequentially (Bronze → Silver → Gold).

# COMMAND ----------

# DBTITLE 1,Load Config
# MAGIC %run ../../1_Canvas/1_Setup/1_Config

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import functions as F
from datetime import datetime
import concurrent.futures
import time

print("="*80)
print("ENRICHMENT PIPELINE ORCHESTRATOR - PARALLEL EXECUTION")
print("="*80)
print(f"Target Catalog: {Config.CATALOG}")
print(f"Timestamp: {datetime.now()}")
print("="*80)

# COMMAND ----------

# DBTITLE 1,BRONZE LAYER - Parallel Ingestion (4 stages)
print("\n" + "="*80)
print("BRONZE LAYER - PARALLEL INGESTION (4 stages)")
print("="*80)
print("Running all 4 ingestion stages in parallel...")
print("="*80)

bronze_start = time.time()

def run_bronze_stage(notebook_name, stage_label):
    print(f"  [{stage_label}] Starting {notebook_name}...")
    dbutils.notebook.run(f"./{notebook_name}", timeout_seconds=1200)
    print(f"  [{stage_label}] Complete")
    return stage_label

bronze_stages = [
    ("01_Ingest_Tpch_Suppliers", "Suppliers"),
    ("02_Ingest_Weather_Data", "Weather"),
    ("07_Ingest_Exchange_Rates", "FX Rates"),
    ("08_Ingest_Logistics", "Logistics"),
]

with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
    futures = {
        executor.submit(run_bronze_stage, nb, label): label
        for nb, label in bronze_stages
    }
    for future in concurrent.futures.as_completed(futures):
        label = futures[future]
        try:
            future.result()
        except Exception as e:
            print(f"  [{label}] FAILED: {e}")
            raise

bronze_elapsed = time.time() - bronze_start
print(f"\n✅ BRONZE LAYER COMPLETE - All 4 stages finished in {bronze_elapsed:.1f}s")

# Count records
supplier_count = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_BRONZE}.ext_tpch_suppliers").count()
parts_count = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_BRONZE}.ext_tpch_parts").count()
partsupp_count = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_BRONZE}.ext_tpch_partsupp").count()
weather_count = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_BRONZE}.ext_weather_daily").count()
fx_count = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_BRONZE}.ext_exchange_rates").count()
logistics_count = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_BRONZE}.ext_logistics_deliveries").count()
print(f"  Suppliers: {supplier_count:,} | Parts: {parts_count:,} | PartSupp: {partsupp_count:,}")
print(f"  Weather: {weather_count:,} | FX: {fx_count:,} | Logistics: {logistics_count:,}")

# COMMAND ----------

# DBTITLE 1,Bronze Layer Summary
print("\n" + "="*80)
print("✅ BRONZE LAYER SUMMARY")
print("="*80)

total_supplier_network = supplier_count + parts_count + partsupp_count
total_all = total_supplier_network + weather_count + fx_count + logistics_count

print(f"\n📊 BRONZE INGESTION SUMMARY ({bronze_elapsed:.1f}s):")
print(f"  Suppliers+Parts+PartSupp: {total_supplier_network:>10,} records")
print(f"  Weather Data:            {weather_count:>10,} records")
print(f"  FX & Commodities:         {fx_count:>10,} records")
print(f"  Logistics:                {logistics_count:>10,} records")
print(f"  " + "-"*40)
print(f"  TOTAL BRONZE RECORDS:     {total_all:>10,}")
print(f"\n⏱️  Bronze layer completed in {bronze_elapsed:.1f}s (4 stages in parallel)")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Data Quality Validation
print("\n🔍 DATA QUALITY VALIDATION:")

# Check for nulls in key columns
print("\n1. Checking for null values in key fields...")
for table in ["ext_tpch_suppliers", "ext_weather_daily", "ext_exchange_rates", "ext_logistics_deliveries"]:
    df = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_BRONZE}.{table}")
    row_count = df.count()
    null_checks = []
    
    # Get first few columns to check
    cols_to_check = df.columns[:5]
    for col in cols_to_check:
        null_count = df.filter(F.col(col).isNull()).count()
        if null_count > 0:
            null_checks.append(f"{col}: {null_count}")
    
    if null_checks:
        print(f"  ⚠️  {table}: {', '.join(null_checks)}")
    else:
        print(f"  ✅ {table}: No nulls in key columns")

# Check date ranges
print("\n2. Validating date ranges...")
weather_dates = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_BRONZE}.ext_weather_daily") \
    .agg(F.min("weather_date").alias("min"), F.max("weather_date").alias("max")).collect()[0]
print(f"  Weather: {weather_dates['min']} to {weather_dates['max']}")

fx_dates = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_BRONZE}.ext_exchange_rates") \
    .agg(F.min("rate_date").alias("min"), F.max("rate_date").alias("max")).collect()[0]
print(f"  FX/Commodities: {fx_dates['min']} to {fx_dates['max']}")

logistics_dates = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_BRONZE}.ext_logistics_deliveries") \
    .agg(F.min("order_date").alias("min"), F.max("order_date").alias("max")).collect()[0]
print(f"  Logistics: {logistics_dates['min']} to {logistics_dates['max']}")

print("\n✅ DATA QUALITY VALIDATION COMPLETE")

# COMMAND ----------

# DBTITLE 1,SILVER LAYER - Parallel Execution (2 stages)
print("\n" + "="*80)
print("SILVER LAYER - PARALLEL EXECUTION (2 stages)")
print("="*80)
print("Running both silver notebooks in parallel...")
print("="*80)

silver_start = time.time()

def run_silver_stage(notebook_name, stage_label):
    print(f"  [{stage_label}] Starting {notebook_name}...")
    dbutils.notebook.run(f"./{notebook_name}", timeout_seconds=1200)
    print(f"  [{stage_label}] Complete")
    return stage_label

silver_stages = [
    ("03_Silver_Supplier_Benchmark", "Supplier Benchmark"),
    ("04_Silver_Weather_Sales", "Weather Sales"),
]

with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
    futures = {
        executor.submit(run_silver_stage, nb, label): label
        for nb, label in silver_stages
    }
    for future in concurrent.futures.as_completed(futures):
        label = futures[future]
        try:
            future.result()
        except Exception as e:
            print(f"  [{label}] FAILED: {e}")
            raise

silver_elapsed = time.time() - silver_start
print(f"\n✅ SILVER LAYER COMPLETE - Both stages finished in {silver_elapsed:.1f}s")

silver_benchmark_count = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.ext_supplier_cost_benchmark").count()
silver_alt_count = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.ext_product_sourcing_alternatives").count()
silver_weather_count = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.ext_weather_sales_correlation").count()
print(f"  Supplier Benchmarks: {silver_benchmark_count:,} | Alternatives: {silver_alt_count:,} | Weather Correlations: {silver_weather_count:,}")

# COMMAND ----------

# DBTITLE 1,GOLD LAYER - Parallel Execution (2 stages)
print("\n" + "="*80)
print("GOLD LAYER - PARALLEL EXECUTION (2 stages)")
print("="*80)
print("Running both gold notebooks in parallel...")
print("="*80)

gold_start = time.time()

def run_gold_stage(notebook_name, stage_label):
    print(f"  [{stage_label}] Starting {notebook_name}...")
    dbutils.notebook.run(f"./{notebook_name}", timeout_seconds=1200)
    print(f"  [{stage_label}] Complete")
    return stage_label

gold_stages = [
    ("05_Gold_Supplier_Analytics", "Supplier Analytics"),
    ("06_Gold_Weather_Analytics", "Weather Analytics"),
]

with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
    futures = {
        executor.submit(run_gold_stage, nb, label): label
        for nb, label in gold_stages
    }
    for future in concurrent.futures.as_completed(futures):
        label = futures[future]
        try:
            future.result()
        except Exception as e:
            print(f"  [{label}] FAILED: {e}")
            raise

gold_elapsed = time.time() - gold_start
print(f"\n✅ GOLD LAYER COMPLETE - Both stages finished in {gold_elapsed:.1f}s")

gold_perf_count = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_supplier_performance").count()
gold_opt_count = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_cost_optimization_opportunities").count()
gold_impact_count = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_weather_sales_impact").count()
gold_sensitive_count = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_weather_sensitive_products").count()
print(f"  Supplier Performance: {gold_perf_count:,} | Cost Opt: {gold_opt_count:,}")
print(f"  Weather Impact: {gold_impact_count:,} | Sensitive Products: {gold_sensitive_count:,}")

# COMMAND ----------

# DBTITLE 1,Full Pipeline Summary
print("\n" + "="*80)
print("✅ FULL ENRICHMENT PIPELINE COMPLETE - ALL 8 STAGES (PARALLEL)")
print("="*80)
print(f"\n📊 FULL PIPELINE SUMMARY:")
print(f"  Bronze: {total_all:,} records ({bronze_elapsed:.1f}s - 4 parallel)")
print(f"  Silver: {silver_benchmark_count:,} benchmarks + {silver_alt_count:,} alternatives + {silver_weather_count:,} weather correlations ({silver_elapsed:.1f}s - 2 parallel)")
print(f"  Gold:   {gold_perf_count:,} supplier scores + {gold_opt_count:,} cost opportunities + {gold_impact_count:,} weather impacts + {gold_sensitive_count:,} sensitive products ({gold_elapsed:.1f}s - 2 parallel)")
total_elapsed = bronze_elapsed + silver_elapsed + gold_elapsed
print(f"\n  Total pipeline time: {total_elapsed:.1f}s")
print(f"  Completed at: {datetime.now()}")
print("\n" + "="*80)