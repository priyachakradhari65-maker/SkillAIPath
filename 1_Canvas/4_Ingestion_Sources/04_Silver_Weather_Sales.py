# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Weather-Sales Correlation - Silver Layer
# MAGIC %md
# MAGIC # Weather-Sales Correlation Analysis - Silver Layer
# MAGIC
# MAGIC **Business Value:** Understand weather impact on sales and optimize for seasonal patterns
# MAGIC
# MAGIC **Integration Logic:**
# MAGIC - Join daily sales data with weather conditions
# MAGIC - Calculate weather-driven sales patterns
# MAGIC - Identify seasonal product opportunities
# MAGIC
# MAGIC **Input Tables:**
# MAGIC - `sap_dev.silver.fact_sales` - Daily sales transactions
# MAGIC - `sap_dev.bronze.ext_weather_daily` - Historical weather data
# MAGIC - `sap_dev.silver.dim_product` - Product attributes
# MAGIC - `sap_dev.silver.dim_date` - Date dimension
# MAGIC
# MAGIC **Output Table:**
# MAGIC - `sap_dev.silver.ext_weather_sales_correlation` - Sales enriched with weather context
# MAGIC
# MAGIC **Key Metrics:**
# MAGIC - Sales volume by temperature range
# MAGIC - Revenue correlation with precipitation
# MAGIC - Product performance by weather condition
# MAGIC - Seasonal sales patterns with weather overlay

# COMMAND ----------

# DBTITLE 1,Load Config
# MAGIC %run ../../1_Canvas/1_Setup/1_Config

# COMMAND ----------

# DBTITLE 1,Create Weather-Sales Correlation Table
spark.sql(f"""
-- Join sales data with weather conditions (weather aggregated by date across cities)
CREATE OR REPLACE TABLE {Config.CATALOG}.{Config.SCHEMA_SILVER}.ext_weather_sales_correlation AS
WITH weather_by_date AS (
    SELECT 
        weather_date,
        AVG(temp_avg_c) as temp_avg_c,
        AVG(temp_max_c) as temp_max_c,
        AVG(temp_min_c) as temp_min_c,
        SUM(precipitation_mm) as precipitation_mm,
        MAX(condition_text) as condition_text,
        AVG(humidity_avg_pct) as humidity_avg_pct,
        AVG(wind_speed_avg_kph) as wind_speed_avg_kph
    FROM {Config.CATALOG}.{Config.SCHEMA_BRONZE}.ext_weather_daily
    WHERE YEAR(weather_date) = 2024  -- Most recent weather year; sales are 2026
    GROUP BY weather_date
)
SELECT 
    -- Sales data
    s.OrderID,
    s.OrderDateID,
    s.ProductSK,
    s.CustomerSK,
    s.ProductQuantity as quantity,
    s.finalprice as revenue,
    
    -- Date context
    d.Date as sale_date,
    d.MonthName,
    d.Quarter,
    d.Year,
    d.DayOfWeek,
    d.IsWeekend,
    
    -- Product context
    p.ProductName,
    pc.ProductCategory as category,
    
    -- Weather conditions on sale date (aggregated across cities)
    w.temp_avg_c,
    w.temp_max_c,
    w.temp_min_c,
    w.precipitation_mm,
    w.condition_text as weather_condition,
    w.humidity_avg_pct,
    w.wind_speed_avg_kph,
    
    -- Weather categorization for analysis
    CASE 
        WHEN w.temp_avg_c < 10 THEN 'COLD'
        WHEN w.temp_avg_c < 20 THEN 'MILD'
        WHEN w.temp_avg_c < 30 THEN 'WARM'
        ELSE 'HOT'
    END as temp_category,
    
    CASE 
        WHEN w.precipitation_mm = 0 THEN 'DRY'
        WHEN w.precipitation_mm < 5 THEN 'LIGHT_RAIN'
        WHEN w.precipitation_mm < 20 THEN 'MODERATE_RAIN'
        ELSE 'HEAVY_RAIN'
    END as rain_category,
    
    -- Seasonal indicator
    CASE 
        WHEN d.MonthName IN ('December', 'January', 'February') THEN 'WINTER'
        WHEN d.MonthName IN ('March', 'April', 'May') THEN 'SPRING'
        WHEN d.MonthName IN ('June', 'July', 'August') THEN 'SUMMER'
        ELSE 'FALL'
    END as season,
    
    current_timestamp() as processed_at
    
FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.fact_sales s
JOIN {Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_date d
    ON s.OrderDateID = d.DateID
JOIN {Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_product p
    ON s.ProductSK = p.ProductSK
LEFT JOIN {Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_product_category pc
    ON s.ProductCategorySK = pc.ProductCategorySK
LEFT JOIN weather_by_date w
    ON MONTH(d.Date) = MONTH(w.weather_date)
    AND DAY(d.Date) = DAY(w.weather_date)
WHERE w.weather_date IS NOT NULL  -- Only include dates with weather data available
""")
print("✅ ext_weather_sales_correlation created")

# COMMAND ----------

# DBTITLE 1,Validation & Sample Insights
print("="*80)
print("✅ WEATHER-SALES CORRELATION SILVER LAYER COMPLETE")
print("="*80)

# Validate table created
corr_count = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.ext_weather_sales_correlation").count()
print(f"\nWeather-sales correlation records: {corr_count:,}")

# Show sample insights: Revenue by temperature category
print("\n=== Revenue by Temperature Category ===")
spark.sql(f"""
    SELECT 
        temp_category,
        COUNT(*) as transactions,
        ROUND(SUM(revenue), 2) as total_revenue,
        ROUND(AVG(revenue), 2) as avg_transaction_value,
        ROUND(SUM(quantity), 0) as total_units_sold
    FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.ext_weather_sales_correlation
    GROUP BY temp_category
    ORDER BY total_revenue DESC
""").show()

# Show insights: Revenue by rain category
print("\n=== Revenue by Precipitation Category ===")
spark.sql(f"""
    SELECT 
        rain_category,
        COUNT(*) as transactions,
        ROUND(SUM(revenue), 2) as total_revenue,
        ROUND(AVG(revenue), 2) as avg_transaction_value
    FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.ext_weather_sales_correlation
    GROUP BY rain_category
    ORDER BY total_revenue DESC
""").show()

# Show top products by season
print("\n=== Top Products by Season ===")
spark.sql(f"""
    WITH season_product_stats AS (
        SELECT 
            season,
            ProductName,
            ROUND(SUM(revenue), 2) as total_revenue,
            SUM(quantity) as total_units
        FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.ext_weather_sales_correlation
        GROUP BY season, ProductName
    )
    SELECT 
        season,
        ProductName,
        total_revenue,
        total_units
    FROM season_product_stats
    QUALIFY ROW_NUMBER() OVER (PARTITION BY season ORDER BY total_revenue DESC) <= 3
    ORDER BY season, total_revenue DESC
""").show(truncate=False)

print(f"\nSilver table created:")
print(f"  - {Config.CATALOG}.{Config.SCHEMA_SILVER}.ext_weather_sales_correlation")
print(f"\nReady for Gold layer weather impact analytics!")

# COMMAND ----------

