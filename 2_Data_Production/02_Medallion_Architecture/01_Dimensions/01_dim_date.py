# Databricks notebook source
# DBTITLE 1,Dimension: Date (SCD Type 0)
# MAGIC %md
# MAGIC # DimDate: Date Dimension (Type 0 - Static)
# MAGIC **SkillAIPath Data Warehouse - Silver Layer**
# MAGIC
# MAGIC ## Module Overview
# MAGIC **Purpose:** Calendar attributes for time-based analysis
# MAGIC
# MAGIC **Pattern:** SCD Type 0 (Pre-populated, never changes)
# MAGIC
# MAGIC **Source:** Generated programmatically (2023-2027)
# MAGIC
# MAGIC **Target:** `sap_{env}.silver.dim_date`
# MAGIC
# MAGIC **Key Concepts:**
# MAGIC - Role-playing dimensions (OrderDate, DeliveryDate, MFDDate all join to DimDate)
# MAGIC - Denormalized calendar attributes (Year, Quarter, Month, Day, DayOfWeek)
# MAGIC - Type 0 dimension (loaded once, never updated)

# COMMAND ----------

# DBTITLE 1,Load Config
# MAGIC %run ../../../1_Canvas/1_Setup/1_Config

# COMMAND ----------

# DBTITLE 1,Configuration and Dependencies
# Configuration and Dependencies
from datetime import datetime, timedelta
from pyspark.sql.functions import col, lit, when, concat, date_format, year, month, quarter, dayofmonth, dayofweek, weekofyear

print("=" * 70)
print("DIM_DATE - SCD TYPE 0 (STATIC DIMENSION)")
print("=" * 70)
print(f"Environment: {Config.ENVIRONMENT}")
print(f"Catalog: {Config.CATALOG}")
print(f"Target: {Config.TBL_DIM_DATE}")
print("Range: 2023-01-01 to 2027-12-30 (5 years - current data)")
print("Type: 0 (Loaded once, never updated)")
print("=" * 70)

# COMMAND ----------

# DBTITLE 1,Generate Date Range (2023-2028)
# Generate continuous date range: 2023-01-01 to 2027-06-30 (current date)
start_date = datetime(2023, 1, 1)
end_date = datetime(2027, 12, 30)
date_list = []

current_date = start_date
while current_date <= end_date:
    date_list.append((current_date,))
    current_date += timedelta(days=1)

print(f"Generated {len(date_list):,} dates (2023 to january 2027 - current data capacity)")

# Create DataFrame
df_dates = spark.createDataFrame(date_list, ["Date"])

print(f"NOTE: Date dimension is SMALL (4 years = 1,461 rows) - can fit in memory/cache")

# COMMAND ----------

# DBTITLE 1,Add Calendar Attributes
# Add calendar attributes (denormalized for query performance)
df_date_dim = df_dates \
    .withColumn("DateID", date_format(col("Date"), "yyyyMMdd").cast("int")) \
    .withColumn("Year", year(col("Date"))) \
    .withColumn("Quarter", concat(lit("Q"), quarter(col("Date")))) \
    .withColumn("MonthName", date_format(col("Date"), "MMMM")) \
    .withColumn("MonthNumber", month(col("Date"))) \
    .withColumn("DayOfMonth", dayofmonth(col("Date"))) \
    .withColumn("DayOfWeek", date_format(col("Date"), "EEEE")) \
    .withColumn("DayOfWeekNumber", dayofweek(col("Date"))) \
    .withColumn("IsWeekend", when(dayofweek(col("Date")).isin(1, 7), 1).otherwise(0)) \
    .withColumn("WeekOfYear", weekofyear(col("Date"))) \
    .withColumn("FiscalYear", 
                when(month(col("Date")) >= 4, year(col("Date")) + 1)
                .otherwise(year(col("Date")))) \
    .withColumn("FiscalQuarter",
                concat(
                    lit("Q"),
                    when(month(col("Date")).between(4, 6), 1)
                    .when(month(col("Date")).between(7, 9), 2)
                    .when(month(col("Date")).between(10, 12), 3)
                    .otherwise(4),
                    lit("-FY"),
                    when(month(col("Date")) >= 4, year(col("Date")) + 1)
                    .otherwise(year(col("Date")))
                )) \
    .select(
        "DateID",
        "Date",
        "Year",
        "Quarter",
        "MonthName",
        "MonthNumber",
        "DayOfMonth",
        "DayOfWeek",
        "DayOfWeekNumber",
        "IsWeekend",
        "WeekOfYear",
        "FiscalYear",
        "FiscalQuarter"
    )

print("Calendar attributes computed")
print("\nNOTE: Fiscal year = Apr-Mar (India standard). US uses Oct-Sep, UK uses Apr-Mar.")
print("NOTE: Denormalization: Year/Quarter/Month redundant but improves query performance (no JOINs)")

# COMMAND ----------

# DBTITLE 1,Write to Dimension Table (One-Time Load)
# Create table (Type 0 - never updated)
# Cast Date from timestamp to date to match table schema
df_date_dim_typed = df_date_dim.withColumn("Date", col("Date").cast("date"))

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {Config.TBL_DIM_DATE} (
        DateID INT,
        Date DATE,
        Year INT,
        Quarter STRING,
        MonthName STRING,
        MonthNumber INT,
        DayOfMonth INT,
        DayOfWeek STRING,
        DayOfWeekNumber INT,
        IsWeekend INT,
        WeekOfYear INT,
        FiscalYear INT,
        FiscalQuarter STRING
    )
    USING DELTA
""")

# Check if already loaded
existing_count = spark.read.table(Config.TBL_DIM_DATE).count()

if existing_count == 0:
    # Initial load
    df_date_dim_typed.write.mode("overwrite").saveAsTable(Config.TBL_DIM_DATE)
    print(f"DimDate loaded: {df_date_dim_typed.count():,} rows")
    print("NOTE: Type 0 dimension - this table will NEVER be updated (only extended for future years)")
else:
    print(f"WARNING: DimDate already exists ({existing_count:,} rows) - skipping load")
    print(f"NOTE: To reload: DROP TABLE {Config.TBL_DIM_DATE}; then rerun this notebook")

# COMMAND ----------

# DBTITLE 1,Validation Query - Sample Dates
# Verify date dimension populated correctly
spark.sql(f"""
    SELECT 
        DateID, Date, Year, Quarter, MonthName,
        DayOfWeek, IsWeekend, FiscalYear, FiscalQuarter
    FROM {Config.TBL_DIM_DATE}
    WHERE year = 2023 AND MonthNumber = 1
    ORDER BY Date
    LIMIT 10
""").display()

# COMMAND ----------

# DBTITLE 1,Validation Query - Date Range Coverage
# Check date range and coverage
spark.sql(f"""
    SELECT 
        MIN(Date) AS start_date,
        MAX(Date) AS end_date,
        COUNT(*) AS total_days,
        COUNT(DISTINCT Year) AS years_covered,
        SUM(IsWeekend) AS weekend_days,
        COUNT(*) - SUM(IsWeekend) AS weekdays
    FROM {Config.TBL_DIM_DATE}
""").display()

# COMMAND ----------

# DBTITLE 1,Return Summary
# Return processing summary
final_count = spark.read.table(Config.TBL_DIM_DATE).count()
date_range = spark.sql(f"SELECT MIN(Date) as min_date, MAX(Date) as max_date FROM {Config.TBL_DIM_DATE}").collect()[0]

summary = {
    "status": "success",
    "environment": Config.ENVIRONMENT,
    "catalog": Config.CATALOG,
    "dimension": "DimDate",
    "scd_type": "Type 0 (Static)",
    "total_dates": final_count,
    "date_range": f"{date_range['min_date']} to {date_range['max_date']}",
    "table": Config.TBL_DIM_DATE
}

print("\n" + "="*70)
print("DIM_DATE (Type 0) LOADED")
print("="*70)
for key, value in summary.items():
    print(f"  {key}: {value}")
print("="*70)
print("\nNOTE: DimDate enables role-playing dimensions (FactSales.OrderDateID, DeliveryDateID, MFDDateID all join to same DimDate)")

dbutils.notebook.exit(str(summary))