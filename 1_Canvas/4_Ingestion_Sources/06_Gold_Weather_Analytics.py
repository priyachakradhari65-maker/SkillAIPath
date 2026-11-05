# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Weather Impact Analytics - Gold Layer
# MAGIC %md
# MAGIC # Weather Impact Analytics - Gold Layer
# MAGIC
# MAGIC **Business Value:** Weather-driven sales intelligence and seasonal optimization
# MAGIC
# MAGIC **Analytics Objectives:**
# MAGIC - Quantify weather impact on sales performance
# MAGIC - Identify weather-sensitive products for inventory planning
# MAGIC - Optimize marketing campaigns around weather forecasts
# MAGIC - Build seasonal forecasting models with weather variables
# MAGIC
# MAGIC **Input Tables:**
# MAGIC - `sap_dev.silver.ext_weather_sales_correlation` - Sales + weather data
# MAGIC
# MAGIC **Output Marts:**
# MAGIC - `sap_dev.gold.mart_weather_sales_impact` - Weather impact KPIs
# MAGIC - `sap_dev.gold.mart_weather_sensitive_products` - Product weather sensitivity

# COMMAND ----------

# DBTITLE 1,Load Config
# MAGIC %run ../../1_Canvas/1_Setup/1_Config

# COMMAND ----------

# DBTITLE 1,Create Weather Sales Impact Mart
spark.sql(f"""
-- Aggregate weather impact on sales performance
CREATE OR REPLACE TABLE {Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_weather_sales_impact AS
SELECT 
    -- Weather conditions
    temp_category,
    rain_category,
    season,
    
    -- Sales metrics
    COUNT(*) as total_transactions,
    COUNT(DISTINCT CustomerSK) as unique_customers,
    SUM(quantity) as total_units_sold,
    ROUND(SUM(revenue), 2) as total_revenue,
    ROUND(AVG(revenue), 2) as avg_transaction_value,
    ROUND(AVG(quantity), 2) as avg_units_per_transaction,
    
    -- Weather-specific insights
    ROUND(AVG(temp_avg_c), 1) as avg_temperature_c,
    ROUND(AVG(precipitation_mm), 2) as avg_precipitation_mm,
    ROUND(AVG(humidity_avg_pct), 1) as avg_humidity_pct,
    
    -- Performance indicators
    ROUND(
        SUM(revenue) / COUNT(*),
        2
    ) as revenue_per_transaction,
    
    ROUND(
        SUM(revenue) / COUNT(DISTINCT sale_date),
        2
    ) as avg_daily_revenue,
    
    -- Comparison to baseline (overall average)
    ROUND(
        (SUM(revenue) / COUNT(*)) / 
        AVG(SUM(revenue) / COUNT(*)) OVER (),
        2
    ) as revenue_index,
    
    current_timestamp() as last_updated
    
FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.ext_weather_sales_correlation
GROUP BY 
    temp_category,
    rain_category,
    season
ORDER BY total_revenue DESC
""")
print("✅ mart_weather_sales_impact created")

# COMMAND ----------

# DBTITLE 1,Create Weather-Sensitive Products Mart
spark.sql(f"""
-- Identify products with high weather sensitivity
CREATE OR REPLACE TABLE {Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_weather_sensitive_products AS
WITH product_weather_stats AS (
    SELECT 
        ProductSK,
        ProductName,
        category,
        temp_category,
        rain_category,
        SUM(quantity) as units_sold,
        ROUND(SUM(revenue), 2) as revenue
    FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.ext_weather_sales_correlation
    GROUP BY 
        ProductSK,
        ProductName,
        category,
        temp_category,
        rain_category
),
product_totals AS (
    SELECT 
        ProductSK,
        ProductName,
        category,
        SUM(units_sold) as total_units,
        SUM(revenue) as total_revenue
    FROM product_weather_stats
    GROUP BY ProductSK, ProductName, category
),
weather_variance AS (
    SELECT 
        ps.ProductSK,
        ps.ProductName,
        ps.category,
        pt.total_revenue,
        
        -- Temperature sensitivity
        MAX(CASE WHEN ps.temp_category = 'HOT' THEN ps.revenue ELSE 0 END) as revenue_hot,
        MAX(CASE WHEN ps.temp_category = 'COLD' THEN ps.revenue ELSE 0 END) as revenue_cold,
        
        -- Rain sensitivity
        MAX(CASE WHEN ps.rain_category = 'DRY' THEN ps.revenue ELSE 0 END) as revenue_dry,
        MAX(CASE WHEN ps.rain_category IN ('MODERATE_RAIN', 'HEAVY_RAIN') THEN ps.revenue ELSE 0 END) as revenue_rain,
        
        -- Calculate variance as indicator of weather sensitivity
        STDDEV(ps.revenue) as revenue_stddev
        
    FROM product_weather_stats ps
    JOIN product_totals pt ON ps.ProductSK = pt.ProductSK
    GROUP BY 
        ps.ProductSK,
        ps.ProductName,
        ps.category,
        pt.total_revenue
)
SELECT 
    ProductSK,
    ProductName,
    category,
    ROUND(total_revenue, 2) as total_revenue,
    ROUND(revenue_stddev, 2) as weather_variance,
    
    -- Weather sensitivity scores (0-100)
    ROUND(
        LEAST((revenue_stddev / NULLIF(total_revenue, 0)) * 100, 100),
        1
    ) as weather_sensitivity_score,
    
    -- Preferred conditions
    CASE 
        WHEN revenue_hot > revenue_cold * 1.5 THEN 'HOT_WEATHER'
        WHEN revenue_cold > revenue_hot * 1.5 THEN 'COLD_WEATHER'
        ELSE 'NEUTRAL'
    END as temperature_preference,
    
    CASE 
        WHEN revenue_rain > revenue_dry * 1.2 THEN 'RAINY_WEATHER'
        WHEN revenue_dry > revenue_rain * 1.2 THEN 'DRY_WEATHER'
        ELSE 'NEUTRAL'
    END as precipitation_preference,
    
    -- Action recommendation
    CASE 
        WHEN revenue_stddev / NULLIF(total_revenue, 0) > 0.3 THEN 'HIGH_SENSITIVITY_FORECAST_CRITICAL'
        WHEN revenue_stddev / NULLIF(total_revenue, 0) > 0.15 THEN 'MODERATE_SENSITIVITY_MONITOR'
        ELSE 'LOW_SENSITIVITY_STANDARD_PLANNING'
    END as inventory_recommendation,
    
    current_timestamp() as last_updated
    
FROM weather_variance
WHERE total_revenue > 0
ORDER BY weather_sensitivity_score DESC
LIMIT 500
""")
print("✅ mart_weather_sensitive_products created")

# COMMAND ----------

# DBTITLE 1,Weather Impact Insights
print("="*80)
print("✅ WEATHER IMPACT GOLD ANALYTICS COMPLETE")
print("="*80)

# Show weather impact summary
print("\n=== Revenue Performance by Weather Conditions ===")
spark.sql(f"""
    SELECT 
        temp_category,
        rain_category,
        total_transactions,
        total_revenue,
        avg_transaction_value,
        revenue_index
    FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_weather_sales_impact
    ORDER BY total_revenue DESC
    LIMIT 10
""").show(truncate=False)

# Show seasonal performance
print("\n=== Revenue by Season ===")
spark.sql(f"""
    SELECT 
        season,
        SUM(total_transactions) as transactions,
        ROUND(SUM(total_revenue), 2) as revenue,
        ROUND(AVG(avg_transaction_value), 2) as avg_transaction
    FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_weather_sales_impact
    GROUP BY season
    ORDER BY revenue DESC
""").show(truncate=False)

# Show top weather-sensitive products
print("\n=== Top 10 Weather-Sensitive Products ===")
spark.sql(f"""
    SELECT 
        ProductName,
        category,
        weather_sensitivity_score,
        temperature_preference,
        precipitation_preference,
        inventory_recommendation
    FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_weather_sensitive_products
    ORDER BY weather_sensitivity_score DESC
    LIMIT 10
""").show(truncate=False)

# Count high-sensitivity products
high_sensitivity = spark.sql(f"""
    SELECT COUNT(*) as count
    FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_weather_sensitive_products
    WHERE weather_sensitivity_score > 30
""").collect()[0]['count']

print(f"\n⛈️  Weather-Sensitive Products: {high_sensitivity} products require forecast-driven inventory planning")

print("\n" + "="*80)
print("Gold Marts Created:")
print(f"  - {Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_weather_sales_impact")
print(f"  - {Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_weather_sensitive_products")
print("="*80)

# COMMAND ----------

