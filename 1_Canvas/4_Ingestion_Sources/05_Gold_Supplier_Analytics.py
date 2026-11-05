# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Supplier Performance Analytics - Gold Layer
# MAGIC %md
# MAGIC # Supplier Performance & Cost Optimization - Gold Analytics
# MAGIC
# MAGIC **Business Value:** Executive-ready supplier intelligence and cost optimization recommendations
# MAGIC
# MAGIC **Analytics Objectives:**
# MAGIC - Identify overpriced suppliers and quantify savings opportunities
# MAGIC - Rank suppliers by cost competitiveness vs industry benchmarks
# MAGIC - Recommend supplier diversification to reduce concentration risk
# MAGIC - Generate actionable procurement insights for negotiations
# MAGIC
# MAGIC **Input Tables:**
# MAGIC - `sap_dev.silver.ext_supplier_cost_benchmark` - Cost comparison analysis
# MAGIC - `sap_dev.silver.ext_product_sourcing_alternatives` - Alternative supplier options
# MAGIC - `sap_dev.silver.fact_purchases` - Purchase history
# MAGIC
# MAGIC **Output Marts:**
# MAGIC - `sap_dev.gold.mart_supplier_performance` - Supplier scorecard with KPIs
# MAGIC - `sap_dev.gold.mart_cost_optimization_opportunities` - Top savings opportunities

# COMMAND ----------

# DBTITLE 1,Load Config
# MAGIC %run ../../1_Canvas/1_Setup/1_Config

# COMMAND ----------

# DBTITLE 1,Create Supplier Performance Mart
spark.sql(f"""
-- Build comprehensive supplier performance scorecard
CREATE OR REPLACE TABLE {Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_supplier_performance AS
WITH supplier_stats AS (
    SELECT 
        WholesalerSK as supplier_id,
        COUNT(DISTINCT ProductSK) as products_supplied,
        COUNT(*) as purchase_transactions,
        SUM(total_quantity_purchased) as total_units_purchased,
        AVG(avg_your_cost) as avg_cost_per_unit,
        SUM(avg_your_cost * total_quantity_purchased) as total_spend,
        AVG(cost_variance_pct) as avg_cost_variance_pct,
        SUM(potential_savings) as total_potential_savings,
        MAX(supplier_concentration_risk) as risk_level,
        AVG(alternative_suppliers_available) as avg_alternatives_per_product
    FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.ext_supplier_cost_benchmark
    GROUP BY WholesalerSK
)
SELECT 
    supplier_id,
    products_supplied,
    purchase_transactions,
    total_units_purchased,
    ROUND(avg_cost_per_unit, 2) as avg_cost_per_unit,
    ROUND(total_spend, 2) as total_spend,
    ROUND(avg_cost_variance_pct, 2) as cost_variance_pct,
    ROUND(total_potential_savings, 2) as potential_annual_savings,
    risk_level,
    ROUND(avg_alternatives_per_product, 1) as avg_alternatives_available,
    
    -- Performance grade
    CASE 
        WHEN avg_cost_variance_pct <= 0 THEN 'A - COMPETITIVE'
        WHEN avg_cost_variance_pct <= 10 THEN 'B - FAIR'
        WHEN avg_cost_variance_pct <= 25 THEN 'C - EXPENSIVE'
        ELSE 'D - OVERPRICED'
    END as performance_grade,
    
    -- Action recommendation
    CASE 
        WHEN avg_cost_variance_pct > 25 AND avg_alternatives_per_product >= 3 THEN 'RENEGOTIATE_OR_SWITCH'
        WHEN avg_cost_variance_pct > 15 THEN 'NEGOTIATE_PRICING'
        WHEN risk_level = 'HIGH_RISK' THEN 'DIVERSIFY_SUPPLIERS'
        WHEN avg_cost_variance_pct <= 0 THEN 'MAINTAIN_RELATIONSHIP'
        ELSE 'MONITOR'
    END as recommended_action,
    
    current_timestamp() as last_updated
    
FROM supplier_stats
ORDER BY total_potential_savings DESC
""")
print("✅ mart_supplier_performance created")

# COMMAND ----------

# DBTITLE 1,Create Cost Optimization Opportunities Mart
spark.sql(f"""
-- Prioritized list of cost-saving opportunities
CREATE OR REPLACE TABLE {Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_cost_optimization_opportunities AS
WITH ranked_opportunities AS (
    SELECT 
        ProductSK,
        ProductName,
        current_avg_cost,
        COUNT(DISTINCT benchmark_supplier_id) as alternative_count,
        MIN(benchmark_cost) as lowest_benchmark_cost,
        MAX(potential_savings_pct) as max_savings_pct,
        AVG(potential_savings_pct) as avg_savings_pct
    FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.ext_product_sourcing_alternatives
    WHERE savings_opportunity IN ('HIGH_SAVINGS', 'MEDIUM_SAVINGS')
    GROUP BY ProductSK, ProductName, current_avg_cost
),
purchase_volume AS (
    SELECT 
        ProductSK,
        SUM(Quantity) as annual_volume,
        COUNT(DISTINCT WholesalerSK) as current_supplier_count
    FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.fact_purchase
    GROUP BY ProductSK
)
SELECT 
    r.ProductSK,
    r.ProductName,
    r.current_avg_cost,
    r.lowest_benchmark_cost,
    ROUND(r.max_savings_pct, 2) as max_savings_pct,
    r.alternative_count,
    pv.annual_volume,
    pv.current_supplier_count,
    
    -- Calculate annual savings potential
    ROUND(
        (r.current_avg_cost - r.lowest_benchmark_cost) * pv.annual_volume,
        2
    ) as estimated_annual_savings,
    
    -- Prioritization score (0-100)
    ROUND(
        (r.max_savings_pct * 0.6) + 
        (LEAST(r.alternative_count / 10.0, 1.0) * 20) +
        (CASE WHEN pv.current_supplier_count = 1 THEN 20 ELSE 0 END),
        1
    ) as optimization_priority_score,
    
    -- Action priority
    CASE 
        WHEN r.max_savings_pct > 30 AND r.alternative_count >= 5 THEN 'CRITICAL'
        WHEN r.max_savings_pct > 20 THEN 'HIGH'
        WHEN r.max_savings_pct > 10 THEN 'MEDIUM'
        ELSE 'LOW'
    END as action_priority,
    
    current_timestamp() as last_updated
    
FROM ranked_opportunities r
JOIN purchase_volume pv ON r.ProductSK = pv.ProductSK
ORDER BY optimization_priority_score DESC, estimated_annual_savings DESC
LIMIT 100
""")
print("✅ mart_cost_optimization_opportunities created")

# COMMAND ----------

# DBTITLE 1,Executive Summary & Insights
print("="*80)
print("✅ SUPPLIER PERFORMANCE GOLD ANALYTICS COMPLETE")
print("="*80)

# Total potential savings
total_savings = spark.sql(f"""
    SELECT SUM(potential_annual_savings) as total_savings
    FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_supplier_performance
""").collect()[0]['total_savings']

print(f"\n💰 TOTAL ANNUAL SAVINGS POTENTIAL: ${total_savings:,.2f}")

# Supplier performance distribution
print("\n=== Supplier Performance Distribution ===")
spark.sql(f"""
    SELECT 
        performance_grade,
        COUNT(*) as supplier_count,
        ROUND(SUM(total_spend), 2) as total_spend,
        ROUND(SUM(potential_annual_savings), 2) as potential_savings
    FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_supplier_performance
    GROUP BY performance_grade
    ORDER BY performance_grade
""").show(truncate=False)

# Top 10 cost optimization opportunities
print("\n=== Top 10 Cost Optimization Opportunities ===")
spark.sql(f"""
    SELECT 
        ProductName,
        current_avg_cost,
        lowest_benchmark_cost,
        max_savings_pct,
        estimated_annual_savings,
        action_priority
    FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_cost_optimization_opportunities
    LIMIT 10
""").show(truncate=False)

# Recommended actions summary
print("\n=== Recommended Actions Summary ===")
spark.sql(f"""
    SELECT 
        recommended_action,
        COUNT(*) as supplier_count,
        ROUND(SUM(potential_annual_savings), 2) as total_savings_opportunity
    FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_supplier_performance
    GROUP BY recommended_action
    ORDER BY total_savings_opportunity DESC
""").show(truncate=False)

print("\n" + "="*80)
print("Gold Marts Created:")
print(f"  - {Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_supplier_performance")
print(f"  - {Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_cost_optimization_opportunities")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Data Quality Validation
print("="*80)
print("GOLD LAYER DATA QUALITY VALIDATION")
print("="*80)

# 1. Row counts
print("\n1. Row counts:")
for tbl in ["mart_supplier_performance", "mart_cost_optimization_opportunities"]:
    cnt = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_GOLD}.{tbl}").count()
    print(f"  {tbl}: {cnt:,} records")

# 2. Null checks
print("\n2. Null checks (key columns):")
nulls = spark.sql(f"""
    SELECT 'supplier_perf.supplier_id' as col, COUNT(*) as nulls FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_supplier_performance WHERE supplier_id IS NULL
    UNION ALL
    SELECT 'supplier_perf.total_spend', COUNT(*) FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_supplier_performance WHERE total_spend IS NULL
    UNION ALL
    SELECT 'supplier_perf.potential_annual_savings', COUNT(*) FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_supplier_performance WHERE potential_annual_savings IS NULL
    UNION ALL
    SELECT 'supplier_perf.performance_grade', COUNT(*) FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_supplier_performance WHERE performance_grade IS NULL
    UNION ALL
    SELECT 'supplier_perf.recommended_action', COUNT(*) FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_supplier_performance WHERE recommended_action IS NULL
    UNION ALL
    SELECT 'cost_opt.ProductSK', COUNT(*) FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_cost_optimization_opportunities WHERE ProductSK IS NULL
    UNION ALL
    SELECT 'cost_opt.ProductName', COUNT(*) FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_cost_optimization_opportunities WHERE ProductName IS NULL
    UNION ALL
    SELECT 'cost_opt.estimated_annual_savings', COUNT(*) FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_cost_optimization_opportunities WHERE estimated_annual_savings IS NULL
    UNION ALL
    SELECT 'cost_opt.action_priority', COUNT(*) FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_cost_optimization_opportunities WHERE action_priority IS NULL
""").collect()
for row in nulls:
    status = "OK" if row['nulls'] == 0 else "WARNING"
    print(f"  [{status}] {row['col']}: {row['nulls']} nulls")

# 3. Performance grade distribution
print("\n3. Supplier performance grade distribution:")
spark.sql(f"""
    SELECT performance_grade, COUNT(*) as cnt, ROUND(SUM(total_spend), 2) as spend, ROUND(SUM(potential_annual_savings), 2) as savings
    FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_supplier_performance
    GROUP BY performance_grade
    ORDER BY performance_grade
""").show()

# 4. Action priority distribution
print("4. Cost optimization action priority:")
spark.sql(f"""
    SELECT action_priority, COUNT(*) as cnt, ROUND(SUM(estimated_annual_savings), 2) as total_savings
    FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_cost_optimization_opportunities
    GROUP BY action_priority
    ORDER BY total_savings DESC
""").show()

# 5. Duplicate check
print("5. Duplicate check:")
dup_suppliers = spark.sql(f"""
    SELECT COUNT(*) as dup FROM (
        SELECT supplier_id, COUNT(*) as c FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_supplier_performance GROUP BY supplier_id HAVING c > 1
    )
""").collect()[0]['dup']
print(f"  Duplicate supplier_ids: {dup_suppliers}")

dup_products = spark.sql(f"""
    SELECT COUNT(*) as dup FROM (
        SELECT ProductSK, COUNT(*) as c FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_cost_optimization_opportunities GROUP BY ProductSK HAVING c > 1
    )
""").collect()[0]['dup']
print(f"  Duplicate ProductSKs in cost_opt: {dup_products}")

# 6. Savings sanity check
print("\n6. Savings sanity check:")
spark.sql(f"""
    SELECT 
        ROUND(MIN(estimated_annual_savings), 2) as min_savings,
        ROUND(MAX(estimated_annual_savings), 2) as max_savings,
        ROUND(AVG(estimated_annual_savings), 2) as avg_savings,
        ROUND(SUM(estimated_annual_savings), 2) as total_savings
    FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_cost_optimization_opportunities
""").show()

print("GOLD LAYER DATA QUALITY VALIDATION COMPLETE")

# COMMAND ----------

# DBTITLE 1,Data Quality Validation
