# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Supplier Benchmark Analysis - Silver Layer
# MAGIC %md
# MAGIC # Supplier Benchmark Intelligence - Silver Layer
# MAGIC
# MAGIC **Business Value:** Compare your supplier costs against industry benchmarks
# MAGIC
# MAGIC **Integration Logic:**
# MAGIC - Join your purchase data with TPC-H supplier benchmark data
# MAGIC - Calculate cost variance vs industry standard
# MAGIC - Identify overpriced suppliers and sourcing opportunities
# MAGIC
# MAGIC **Input Tables:**
# MAGIC - `sap_dev.silver.fact_purchases` - Your actual purchase transactions
# MAGIC - `sap_dev.bronze.ext_tpch_suppliers` - TPC-H supplier benchmarks
# MAGIC - `sap_dev.bronze.ext_tpch_partsupp` - Benchmark supplier costs by part
# MAGIC
# MAGIC **Output Tables:**
# MAGIC - `sap_dev.silver.ext_supplier_cost_benchmark` - Supplier cost comparison
# MAGIC - `sap_dev.silver.ext_product_sourcing_alternatives` - Alternative supplier options
# MAGIC
# MAGIC **Key Metrics:**
# MAGIC - Cost variance % vs TPC-H benchmark
# MAGIC - Number of alternative suppliers available
# MAGIC - Potential cost savings per product
# MAGIC - Supplier concentration risk score

# COMMAND ----------

# DBTITLE 1,Load Config
# MAGIC %run ../../1_Canvas/1_Setup/1_Config

# COMMAND ----------

# DBTITLE 1,Create Supplier Cost Benchmark Table
spark.sql(f"""
-- Create supplier cost benchmark analysis
CREATE OR REPLACE TABLE {Config.CATALOG}.{Config.SCHEMA_SILVER}.ext_supplier_cost_benchmark AS
SELECT 
    -- Your actual purchase data
    p.WholesalerSK,
    p.ProductSK,
    dp.ProductName,
    COUNT(*) as purchase_count,
    AVG(p.UnitCost) as avg_your_cost,
    SUM(p.Quantity) as total_quantity_purchased,
    
    -- TPC-H benchmark data (industry standard costs)
    AVG(ps.supplier_cost_inr) as avg_benchmark_cost,
    COUNT(DISTINCT ps.supplier_key) as alternative_suppliers_available,
    
    -- Cost comparison metrics
    ROUND(
        (AVG(p.UnitCost) - AVG(ps.supplier_cost_inr)) / AVG(ps.supplier_cost_inr) * 100, 
        2
    ) as cost_variance_pct,
    
    ROUND(
        (AVG(p.UnitCost) - AVG(ps.supplier_cost_inr)) * SUM(p.Quantity),
        2
    ) as potential_savings,
    
    -- Risk indicators
    CASE 
        WHEN COUNT(DISTINCT ps.supplier_key) = 1 THEN 'HIGH_RISK'
        WHEN COUNT(DISTINCT ps.supplier_key) <= 3 THEN 'MEDIUM_RISK'
        ELSE 'LOW_RISK'
    END as supplier_concentration_risk,
    
    current_timestamp() as processed_at
    
FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.fact_purchase p
JOIN {Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_product dp
    ON p.ProductSK = dp.ProductSK
LEFT JOIN {Config.CATALOG}.{Config.SCHEMA_BRONZE}.ext_tpch_partsupp ps
    ON p.ProductSK = ps.part_key
GROUP BY 
    p.WholesalerSK,
    p.ProductSK,
    dp.ProductName
""")
print("✅ ext_supplier_cost_benchmark created")

# COMMAND ----------

# DBTITLE 1,Create Product Sourcing Alternatives Table
spark.sql(f"""
-- Identify alternative suppliers with better pricing
CREATE OR REPLACE TABLE {Config.CATALOG}.{Config.SCHEMA_SILVER}.ext_product_sourcing_alternatives AS
WITH your_costs AS (
    SELECT 
        p.ProductSK,
        dp.ProductName,
        AVG(p.UnitCost) as current_avg_cost
    FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.fact_purchase p
    JOIN {Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_product dp
        ON p.ProductSK = dp.ProductSK
    GROUP BY p.ProductSK, dp.ProductName
),
benchmark_alternatives AS (
    SELECT 
        ps.part_key as benchmark_part_id,
        ps.supplier_key as benchmark_supplier_id,
        s.supplier_name as benchmark_supplier_name,
        s.supplier_address as supplier_address,
        s.phone as supplier_phone,
        ps.supplier_cost_inr as benchmark_cost,
        ps.available_qty as available_quantity
    FROM {Config.CATALOG}.{Config.SCHEMA_BRONZE}.ext_tpch_partsupp ps
    JOIN {Config.CATALOG}.{Config.SCHEMA_BRONZE}.ext_tpch_suppliers s
        ON ps.supplier_key = s.supplier_key
)
SELECT 
    yc.ProductSK,
    yc.ProductName,
    yc.current_avg_cost,
    ba.benchmark_supplier_id,
    ba.benchmark_supplier_name,
    ba.supplier_address,
    ba.supplier_phone,
    ba.benchmark_cost,
    ba.available_quantity,
    ROUND(
        (yc.current_avg_cost - ba.benchmark_cost) / yc.current_avg_cost * 100,
        2
    ) as potential_savings_pct,
    CASE 
        WHEN ba.benchmark_cost < yc.current_avg_cost * 0.8 THEN 'HIGH_SAVINGS'
        WHEN ba.benchmark_cost < yc.current_avg_cost * 0.95 THEN 'MEDIUM_SAVINGS'
        ELSE 'LOW_SAVINGS'
    END as savings_opportunity,
    current_timestamp() as processed_at
FROM your_costs yc
JOIN benchmark_alternatives ba
    ON yc.ProductSK = ba.benchmark_part_id
WHERE ba.benchmark_cost < yc.current_avg_cost  -- Only show cheaper alternatives
ORDER BY potential_savings_pct DESC
""")
print("✅ ext_product_sourcing_alternatives created")

# COMMAND ----------

# DBTITLE 1,Validation Summary
print("="*80)
print("✅ SUPPLIER BENCHMARK SILVER LAYER COMPLETE")
print("="*80)

# Validate tables created
benchmark_count = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.ext_supplier_cost_benchmark").count()
alternatives_count = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.ext_product_sourcing_alternatives").count()

print(f"\nSupplier cost benchmarks: {benchmark_count:,} records")
print(f"Alternative sourcing options: {alternatives_count:,} records")

# Show top 5 cost-saving opportunities
print("\nTop 5 Cost-Saving Opportunities:")
spark.sql(f"""
    SELECT 
        ProductName,
        benchmark_supplier_name,
        current_avg_cost,
        benchmark_cost,
        potential_savings_pct,
        savings_opportunity
    FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.ext_product_sourcing_alternatives
    ORDER BY potential_savings_pct DESC
    LIMIT 5
""").show(truncate=False)

print(f"\nSilver tables created:")
print(f"  - {Config.CATALOG}.{Config.SCHEMA_SILVER}.ext_supplier_cost_benchmark")
print(f"  - {Config.CATALOG}.{Config.SCHEMA_SILVER}.ext_product_sourcing_alternatives")

# COMMAND ----------

# DBTITLE 1,Data Quality Validation
print("="*80)
print("SILVER LAYER DATA QUALITY VALIDATION")
print("="*80)

# 1. Row counts
print("\n1. Row counts:")
for tbl in ["ext_supplier_cost_benchmark", "ext_product_sourcing_alternatives"]:
    cnt = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.{tbl}").count()
    print(f"  {tbl}: {cnt:,} records")

# 2. Null checks
print("\n2. Null checks (key columns):")
null_results = spark.sql(f"""
    SELECT 'benchmark.WholesalerSK' as col, COUNT(*) as nulls FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.ext_supplier_cost_benchmark WHERE WholesalerSK IS NULL
    UNION ALL
    SELECT 'benchmark.ProductSK', COUNT(*) FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.ext_supplier_cost_benchmark WHERE ProductSK IS NULL
    UNION ALL
    SELECT 'benchmark.ProductName', COUNT(*) FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.ext_supplier_cost_benchmark WHERE ProductName IS NULL
    UNION ALL
    SELECT 'benchmark.avg_your_cost', COUNT(*) FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.ext_supplier_cost_benchmark WHERE avg_your_cost IS NULL
    UNION ALL
    SELECT 'benchmark.avg_benchmark_cost', COUNT(*) FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.ext_supplier_cost_benchmark WHERE avg_benchmark_cost IS NULL
    UNION ALL
    SELECT 'alternatives.ProductSK', COUNT(*) FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.ext_product_sourcing_alternatives WHERE ProductSK IS NULL
    UNION ALL
    SELECT 'alternatives.ProductName', COUNT(*) FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.ext_product_sourcing_alternatives WHERE ProductName IS NULL
    UNION ALL
    SELECT 'alternatives.current_avg_cost', COUNT(*) FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.ext_product_sourcing_alternatives WHERE current_avg_cost IS NULL
    UNION ALL
    SELECT 'alternatives.benchmark_cost', COUNT(*) FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.ext_product_sourcing_alternatives WHERE benchmark_cost IS NULL
""")
for row in null_results.collect():
    status = "OK" if row['nulls'] == 0 else "WARNING"
    print(f"  [{status}] {row['col']}: {row['nulls']} nulls")

# 3. Cost variance distribution
print("\n3. Cost variance distribution:")
spark.sql(f"""
    SELECT 
        ROUND(AVG(cost_variance_pct), 2) as avg_variance_pct,
        ROUND(MIN(cost_variance_pct), 2) as min_variance_pct,
        ROUND(MAX(cost_variance_pct), 2) as max_variance_pct,
        ROUND(AVG(potential_savings), 2) as avg_potential_savings
    FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.ext_supplier_cost_benchmark
""").show()

# 4. Supplier concentration risk
print("4. Supplier concentration risk:")
spark.sql(f"""
    SELECT supplier_concentration_risk, COUNT(*) as cnt
    FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.ext_supplier_cost_benchmark
    GROUP BY supplier_concentration_risk
    ORDER BY cnt DESC
""").show()

# 5. Savings opportunity distribution
print("5. Savings opportunity distribution:")
spark.sql(f"""
    SELECT savings_opportunity, COUNT(*) as cnt
    FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.ext_product_sourcing_alternatives
    GROUP BY savings_opportunity
    ORDER BY cnt DESC
""").show()

# 6. Duplicate check
print("6. Duplicate check (benchmark table):")
dup_cnt = spark.sql(f"""
    SELECT COUNT(*) as dup_count FROM (
        SELECT WholesalerSK, ProductSK, COUNT(*) as c
        FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.ext_supplier_cost_benchmark
        GROUP BY WholesalerSK, ProductSK
        HAVING COUNT(*) > 1
    )
""").collect()[0]['dup_count']
print(f"  Duplicate (WholesalerSK, ProductSK) combos: {dup_cnt}")

print("\nSILVER LAYER DATA QUALITY VALIDATION COMPLETE")

# COMMAND ----------

