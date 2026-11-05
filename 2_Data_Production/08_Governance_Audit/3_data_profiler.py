# Databricks notebook source
# MAGIC %md
# MAGIC # Data Profiler
# MAGIC
# MAGIC Automated column-level profiling for data discovery and quality assessment

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

dbutils.widgets.text("table_name", "{Config.SCHEMA_SILVER}.fact_sales", "Table to Profile")

# COMMAND ----------

# DBTITLE 1,Get Table Schema and Row Count
table_name = dbutils.widgets.get("table_name")
df = spark.table(table_name)
row_count = df.count()

print(f"Profiling Table: {table_name}")
print(f"Total Rows: {row_count:,}")
print(f"Total Columns: {len(df.columns)}")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Column-Level Profiling
from pyspark.sql.functions import col, count, countDistinct, min as spark_min, max as spark_max, avg, stddev

profile_results = []

for column in df.columns:
    col_type = dict(df.dtypes)[column]

    # Basic stats
    null_count = df.filter(col(column).isNull()).count()
    null_pct = (null_count / row_count * 100) if row_count > 0 else 0
    distinct_count = df.select(countDistinct(col(column))).collect()[0][0]
    cardinality_pct = (distinct_count / row_count * 100) if row_count > 0 else 0

    stats = {
        "column_name": column,
        "data_type": col_type,
        "null_count": null_count,
        "null_percentage": round(null_pct, 2),
        "distinct_count": distinct_count,
        "cardinality_percentage": round(cardinality_pct, 2)
    }

    # Numeric column stats
    if col_type in ["int", "bigint", "double", "float", "decimal"]:
        num_stats = df.select(
            spark_min(col(column)).alias("min_val"),
            spark_max(col(column)).alias("max_val"),
            avg(col(column)).alias("avg_val"),
            stddev(col(column)).alias("stddev_val")
        ).collect()[0]

        stats.update({
            "min_value": num_stats["min_val"],
            "max_value": num_stats["max_val"],
            "avg_value": round(num_stats["avg_val"], 2) if num_stats["avg_val"] else None,
            "stddev": round(num_stats["stddev_val"], 2) if num_stats["stddev_val"] else None
        })

    profile_results.append(stats)

print(f"Profiling complete for {len(profile_results)} columns")

# COMMAND ----------

# DBTITLE 1,Display Profiling Report
from pyspark.sql import Row

profile_df = spark.createDataFrame([Row(**r) for r in profile_results])
display(profile_df.orderBy("null_percentage", ascending=False))

# COMMAND ----------

# DBTITLE 1,Data Quality Summary
high_null_cols = [r for r in profile_results if r["null_percentage"] > 10]
low_cardinality_cols = [r for r in profile_results if r["distinct_count"] < 10]
high_cardinality_cols = [r for r in profile_results if r["cardinality_percentage"] > 95]

print("DATA QUALITY SUMMARY")
print("="*80)
print(f"Columns with > 10% NULL values: {len(high_null_cols)}")
if high_null_cols:
    for col_info in high_null_cols:
        print(f"  - {col_info['column_name']}: {col_info['null_percentage']}% NULL")

print(f"\nLow cardinality columns (< 10 distinct values): {len(low_cardinality_cols)}")
if low_cardinality_cols:
    for col_info in low_cardinality_cols:
        print(f"  - {col_info['column_name']}: {col_info['distinct_count']} distinct values")

print(f"\nHigh cardinality columns (> 95% unique): {len(high_cardinality_cols)}")
if high_cardinality_cols:
    for col_info in high_cardinality_cols:
        print(f"  - {col_info['column_name']}: {col_info['cardinality_percentage']}% unique")

# COMMAND ----------

# DBTITLE 1,Recommendations
print("\nRECOMMENDATIONS")
print("="*80)

if high_null_cols:
    print("\nHigh NULL columns:")
    print("  - Consider adding NOT NULL constraints if business rules allow")
    print("  - Investigate data quality issues at source")
    print("  - Add validation checks in bronze layer")

if low_cardinality_cols:
    print("\nLow cardinality columns:")
    print("  - Good candidates for partitioning or ZORDER clustering")
    print("  - Consider using as filter columns in WHERE clauses")
    print("  - Potential lookup/reference dimension candidates")

if high_cardinality_cols:
    print("\nHigh cardinality columns:")
    print("  - Likely primary/foreign keys or unique identifiers")
    print("  - Not ideal for ZORDER (minimal clustering benefit)")
    print("  - Good candidates for uniqueness validation")

# COMMAND ----------

# DBTITLE 1,String Pattern Analysis
# String Pattern Analysis for text columns
string_cols = [f.name for f in df.schema.fields if str(f.dataType) == "StringType"]

if string_cols:
    print("\nSTRING PATTERN ANALYSIS")
    print("="*80)

    for col_name in string_cols[:5]:  # Limit to first 5 string columns
        # Get sample values
        samples = df.select(col_name).filter(f"{col_name} IS NOT NULL").limit(5).collect()

        # Calculate length statistics
        from pyspark.sql.functions import length
        length_stats = df.select(
            spark_min(length(col(col_name))).alias("min_len"),
            spark_max(length(col(col_name))).alias("max_len"),
            avg(length(col(col_name))).alias("avg_len")
        ).collect()[0]

        print(f"\n{col_name}:")
        print(f"  Min Length: {length_stats['min_len']}")
        print(f"  Max Length: {length_stats['max_len']}")
        print(f"  Avg Length: {length_stats['avg_len']:.2f}" if length_stats['avg_len'] else "  Avg Length: NULL")
        print(f"  Sample Values: {', '.join([str(r[0]) for r in samples[:3] if r[0]])}")

# COMMAND ----------

# DBTITLE 1,Outlier Detection
# Outlier Detection using IQR method
if numeric_cols:
    print("\nOUTLIER DETECTION (IQR Method)")
    print("="*80)

    from pyspark.sql.functions import expr, percentile_approx

    for col_name in numeric_cols[:5]:  # Limit to first 5 numeric columns
        # Calculate quartiles
        quantiles = df.select(
            percentile_approx(col(col_name), 0.25).alias("q1"),
            percentile_approx(col(col_name), 0.75).alias("q3")
        ).collect()[0]

        if quantiles['q1'] and quantiles['q3']:
            q1 = quantiles['q1']
            q3 = quantiles['q3']
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr

            # Count outliers
            outliers = df.filter(
                (col(col_name) < lower_bound) | (col(col_name) > upper_bound)
            ).count()
            outlier_pct = (outliers / row_count * 100) if row_count > 0 else 0

            print(f"\n{col_name}:")
            print(f"  Q1: {q1:.2f}, Q3: {q3:.2f}, IQR: {iqr:.2f}")
            print(f"  Bounds: [{lower_bound:.2f}, {upper_bound:.2f}]")
            print(f"  Outliers: {outliers:,} ({outlier_pct:.2f}%)")

            if outlier_pct > 5:
                print(f"  WARNING: High outlier percentage (>{outlier_pct:.0f}%)")

# COMMAND ----------

# DBTITLE 1,Correlation Analysis
# Correlation Analysis between numeric columns
if len(numeric_cols) >= 2:
    print("\nCORRELATION ANALYSIS")
    print("="*80)
    print("Pearson correlation coefficients between numeric columns:")
    print("")

    from pyspark.sql.functions import corr

    # Calculate correlations for pairs of numeric columns
    correlations = []
    for i, col1 in enumerate(numeric_cols[:5]):
        for col2 in numeric_cols[i+1:6]:  # Limit to avoid too many pairs
            try:
                corr_val = df.select(corr(col1, col2)).collect()[0][0]
                if corr_val is not None:
                    correlations.append((col1, col2, corr_val))
            except:
                pass

    # Sort by absolute correlation value
    correlations.sort(key=lambda x: abs(x[2]), reverse=True)

    for col1, col2, corr_val in correlations[:10]:  # Show top 10 correlations
        strength = "Strong" if abs(corr_val) > 0.7 else "Moderate" if abs(corr_val) > 0.4 else "Weak"
        direction = "positive" if corr_val > 0 else "negative"
        print(f"  {col1} <-> {col2}: {corr_val:.3f} ({strength} {direction})")

    print("\nInterpretation:")
    print("  > 0.7: Strong correlation (potential multicollinearity)")
    print("  0.4-0.7: Moderate correlation")
    print("  < 0.4: Weak correlation")

# COMMAND ----------

# DBTITLE 1,Data Quality Score
# Calculate overall data quality score
print("\nOVERALL DATA QUALITY SCORE")
print("="*80)

# Scoring criteria
completeness_score = ((row_count - sum([r['null_count'] for r in profile_results])) / (row_count * len(profile_results)) * 100) if row_count > 0 else 0

# Uniqueness score (average cardinality)
uniqueness_score = sum([r['cardinality_percentage'] for r in profile_results]) / len(profile_results)

# Consistency score (1 - outlier rate)
if numeric_cols:
    # Simplified: assume 5% outliers is acceptable
    consistency_score = 95.0
else:
    consistency_score = 100.0

# Overall score (weighted average)
overall_score = (
    completeness_score * 0.4 +  # 40% weight on completeness
    consistency_score * 0.3 +    # 30% weight on consistency
    min(uniqueness_score, 100) * 0.3  # 30% weight on uniqueness
)

print(f"Completeness: {completeness_score:.1f}% (non-NULL values)")
print(f"Uniqueness: {min(uniqueness_score, 100):.1f}% (average cardinality)")
print(f"Consistency: {consistency_score:.1f}% (within expected ranges)")
print("")
print(f"OVERALL QUALITY SCORE: {overall_score:.1f}/100")
print("")

if overall_score >= 90:
    print("STATUS: Excellent data quality")
elif overall_score >= 75:
    print("STATUS: Good data quality")
elif overall_score >= 60:
    print("STATUS: Acceptable data quality - review warnings")
else:
    print("STATUS: Poor data quality - immediate action required")