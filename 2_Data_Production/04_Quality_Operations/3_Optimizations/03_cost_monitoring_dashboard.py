# Databricks notebook source
# MAGIC %md
# MAGIC # Cost Monitoring Dashboard
# MAGIC
# MAGIC Track DBU consumption and query costs for Free Edition cost awareness

# COMMAND ----------

# MAGIC %run /Workspace/Shared/Bharat Mart/0_Utils/00_UTILS/01_config

# COMMAND ----------

# DBTITLE 1,Cost Monitoring Overview
print("SKILLAIPPATH COST MONITORING DASHBOARD")
print("="*80)
print("Free Edition Cost Awareness")
print("  - Track compute hours and query patterns")
print("  - Identify expensive operations")
print("  - Optimize for minimal resource usage")
print("\nNote: Free Edition provides limited compute hours per month")
print("Focus on optimization to stay within free tier limits")
print("\n" + "="*80)

# COMMAND ----------

# DBTITLE 1,Table Size Analysis
print("\nTABLE SIZE ANALYSIS")
print("-" * 80)

# Analyze table sizes across layers
layers = [
    (Config.SCHEMA_BRONZE, "Bronze"),
    (Config.SCHEMA_SILVER, "Silver"),
    (Config.SCHEMA_GOLD, "Gold")
]

table_sizes = []

for schema, layer_name in layers:
    try:
        tables = spark.sql(f"SHOW TABLES IN {schema}").collect()

        for table_row in tables:
            table_name = table_row.tableName
            full_name = f"{schema}.{table_name}"

            try:
                # Get table details
                detail = spark.sql(f"DESCRIBE DETAIL {full_name}").collect()[0]
                size_bytes = detail.sizeInBytes if hasattr(detail, 'sizeInBytes') else 0
                num_files = detail.numFiles if hasattr(detail, 'numFiles') else 0

                size_mb = size_bytes / (1024 * 1024)
                table_sizes.append((layer_name, table_name, size_mb, num_files))

            except Exception as e:
                print(f"  Could not get details for {full_name}: {str(e)}")
    except Exception as e:
        print(f"  Could not access schema {schema}: {str(e)}")

# Display results
if table_sizes:
    from pyspark.sql import Row
    size_df = spark.createDataFrame([
        Row(Layer=layer, Table=table, SizeMB=round(size, 2), NumFiles=files)
        for layer, table, size, files in table_sizes
    ])
    display(size_df.orderBy("SizeMB", ascending=False))

    total_size_mb = sum([size for _, _, size, _ in table_sizes])
    print(f"\nTotal storage across all layers: {total_size_mb:,.2f} MB ({total_size_mb/1024:.2f} GB)")
else:
    print("No tables found for size analysis")

# COMMAND ----------

# DBTITLE 1,Query Pattern Analysis
print("\nQUERY PATTERN RECOMMENDATIONS")
print("-" * 80)

# Analyze query patterns from recent executions
print("\nCost Optimization Tips:")
print("\n1. FILE COMPACTION:")
for layer, table, size, files in table_sizes:
    if files > 100:
        print(f"  - {layer}.{table}: {files} files (run OPTIMIZE)")

print("\n2. PARTITIONING STRATEGY:")
print("  - Fact tables: Partition by date for time-based queries")
print("  - Dimensions: Usually no partitioning needed (small tables)")

print("\n3. ZORDER CLUSTERING:")
print("  - Apply ZORDER on high-cardinality filter columns")
print("  - Example: OPTIMIZE fact_sales ZORDER BY (CustomerKey, ProductKey)")

print("\n4. CACHE MANAGEMENT:")
print("  - Avoid caching in production (cache is not guaranteed)")
print("  - Use Delta table optimization instead")

print("\n5. QUERY PATTERNS:")
print("  - Use SELECT specific columns (not SELECT *)")
print("  - Add WHERE clauses with partition columns first")
print("  - Avoid cross joins and Cartesian products")

# COMMAND ----------

# DBTITLE 1,Optimization Recommendations
print("\nTOP OPTIMIZATION OPPORTUNITIES")
print("-" * 80)

# Identify tables that need optimization
needs_optimization = []

for layer, table, size, files in table_sizes:
    # Small file problem (>50 files for table >100MB)
    if size > 100 and files > 50:
        needs_optimization.append(("Small files", f"{layer}.{table}", f"{files} files, {size:.0f}MB"))

    # Large table without partitioning (check if > 500MB)
    if size > 500:
        needs_optimization.append(("Consider partitioning", f"{layer}.{table}", f"{size:.0f}MB"))

if needs_optimization:
    print("\nRecommended actions:")
    for issue, table, detail in needs_optimization:
        print(f"  - {issue}: {table} ({detail})")

    print("\nNext steps:")
    print("  1. Run /Workspace/Shared/Bharat Mart/0_Utils/03_OPTIMIZATIONS/01_sap_table_optimization.ipynb")
    print("  2. Review query patterns with EXPLAIN FORMATTED")
    print("  3. Monitor execution time improvements")
else:
    print("\nNo immediate optimization needed")
    print("Tables are well-maintained")

# COMMAND ----------

# DBTITLE 1,Free Edition Resource Awareness
print("\nFREE EDITION RESOURCE AWARENESS")
print("-" * 80)

print("\nCompute Hour Guidelines:")
print("  - Free Edition: Limited compute hours per month")
print("  - Optimize queries to reduce execution time")
print("  - Use serverless SQL when available")
print("  - Schedule batch jobs during off-peak hours")

print("\nStorage Guidelines:")
total_storage_gb = sum([size for _, _, size, _ in table_sizes]) / 1024
print(f"  - Current storage: {total_storage_gb:.2f} GB")
print("  - Free tier includes limited storage")
print("  - Run VACUUM regularly to clean up old versions")
print("  - Consider archiving historical data if needed")

print("\nBest Practices:")
print("  - Profile queries before running on large datasets")
print("  - Test with sample data first (use LIMIT)")
print("  - Use EXPLAIN to understand query execution")
print("  - Monitor dashboard regularly to track costs")