# Databricks notebook source
# DBTITLE 1,Exchange Rates & Commodity Prices - Bronze Ingestion
# MAGIC %md
# MAGIC # Exchange Rates & Commodity Prices - Bronze Layer (PRODUCTION-GRADE)
# MAGIC
# MAGIC **Business Value:** Multi-currency financial analysis and commodity cost tracking
# MAGIC
# MAGIC **Data Generation:** dbldatagen - 50,000+ realistic financial records
# MAGIC
# MAGIC **Coverage:**
# MAGIC - **30 Major Currencies** (USD, EUR, GBP, JPY, CNY, AUD, etc.)
# MAGIC - **10 Key Commodities** (Gold, Silver, Crude Oil, Copper, etc.)
# MAGIC - **5 Years Daily Data** (2020-2025)
# MAGIC - **Realistic FX Volatility** and commodity price movements
# MAGIC
# MAGIC **Output Table:**
# MAGIC - `sap_dev.bronze.ext_exchange_rates` - 50,000+ daily FX & commodity prices
# MAGIC
# MAGIC **Use Cases:**
# MAGIC 1. Multi-currency revenue reporting (convert international sales to INR)
# MAGIC 2. Commodity cost analysis (raw material pricing trends)
# MAGIC 3. Currency hedging strategy (identify FX risk exposure)
# MAGIC 4. Procurement planning (optimal timing for commodity purchases)
# MAGIC 5. Financial forecasting (adjust for FX and commodity volatility)

# COMMAND ----------

# DBTITLE 1,Load Config
# MAGIC %run ../../1_Canvas/1_Setup/1_Config

# COMMAND ----------

# DBTITLE 1,Install Dependencies
# MAGIC %pip install dbldatagen --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Reload Config After Restart
# MAGIC %run ../../1_Canvas/1_Setup/1_Config

# COMMAND ----------

# DBTITLE 1,Import Libraries
import dbldatagen as dg
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, date
import math

print("="*80)
print("EXCHANGE RATES & COMMODITY PRICES - RICH GENERATION")
print("="*80)
print(f"Target: {Config.CATALOG}.{Config.SCHEMA_BRONZE}.ext_exchange_rates")
print(f"Coverage: 30 currencies + 10 commodities × 5 years × daily")
print(f"Period: 2020-01-01 to 2024-12-31")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Define Currencies and Commodities
# Major currencies with base exchange rates to INR (approximate 2024 values)
CURRENCIES = [
    ("USD", "US Dollar", 82.5),
    ("EUR", "Euro", 90.0),
    ("GBP", "British Pound", 105.0),
    ("JPY", "Japanese Yen", 0.56),
    ("CNY", "Chinese Yuan", 11.5),
    ("AUD", "Australian Dollar", 54.0),
    ("CAD", "Canadian Dollar", 61.0),
    ("CHF", "Swiss Franc", 93.0),
    ("SGD", "Singapore Dollar", 61.5),
    ("HKD", "Hong Kong Dollar", 10.5),
    ("NZD", "New Zealand Dollar", 50.0),
    ("SEK", "Swedish Krona", 7.8),
    ("NOK", "Norwegian Krone", 7.7),
    ("DKK", "Danish Krone", 12.0),
    ("ZAR", "South African Rand", 4.4),
    ("MXN", "Mexican Peso", 4.8),
    ("BRL", "Brazilian Real", 16.5),
    ("RUB", "Russian Ruble", 0.88),
    ("KRW", "South Korean Won", 0.062),
    ("THB", "Thai Baht", 2.3),
    ("MYR", "Malaysian Ringgit", 17.5),
    ("IDR", "Indonesian Rupiah", 0.0053),
    ("PHP", "Philippine Peso", 1.47),
    ("AED", "UAE Dirham", 22.5),
    ("SAR", "Saudi Riyal", 22.0),
    ("ILS", "Israeli Shekel", 22.2),
    ("TRY", "Turkish Lira", 2.8),
    ("PLN", "Polish Zloty", 20.5),
    ("CZK", "Czech Koruna", 3.6),
    ("HUF", "Hungarian Forint", 0.23)
]

# Key commodities with base prices
COMMODITIES = [
    ("GOLD", "Gold (per 10g)", "INR", 62000.0, 2000.0),      # Base price, volatility
    ("SILVER", "Silver (per kg)", "INR", 72000.0, 5000.0),
    ("CRUDE_OIL", "Crude Oil (per barrel)", "USD", 75.0, 15.0),
    ("COPPER", "Copper (per kg)", "USD", 8.5, 1.5),
    ("ALUMINUM", "Aluminum (per kg)", "USD", 2.3, 0.4),
    ("ZINC", "Zinc (per kg)", "USD", 2.8, 0.5),
    ("WHEAT", "Wheat (per quintal)", "INR", 2500.0, 300.0),
    ("RICE", "Rice (per quintal)", "INR", 3500.0, 400.0),
    ("COTTON", "Cotton (per quintal)", "INR", 7500.0, 800.0),
    ("RUBBER", "Rubber (per kg)", "INR", 150.0, 20.0)
]

print(f"\n💱 CURRENCIES COVERED: {len(CURRENCIES)}")
for code, name, _ in CURRENCIES[:5]:
    print(f"   - {code}: {name}")
print(f"   ... and {len(CURRENCIES) - 5} more currencies")

print(f"\n📊 COMMODITIES COVERED: {len(COMMODITIES)}")
for code, name, _, base_price, _ in COMMODITIES:
    print(f"   - {code}: {name}")

# COMMAND ----------

# DBTITLE 1,Generate 5 Years of Daily FX Rates
print("\n💱 GENERATING EXCHANGE RATES (5 years × 30 currencies)...")

# Date range
start_date = date(2020, 1, 1)
end_date = date(2024, 12, 31)
date_range_days = (end_date - start_date).days + 1

print(f"Date Range: {start_date} to {end_date} ({date_range_days} days)")
print(f"Expected FX Records: {date_range_days} days × {len(CURRENCIES)} currencies = {date_range_days * len(CURRENCIES):,}")

# Generate base FX data
fx_spec = (
    dg.DataGenerator(spark, name="fx_rates", rows=date_range_days * len(CURRENCIES), partitions=8, randomSeed=42)
    .withIdOutput()
    .withColumn("date_offset", "integer", minValue=0, maxValue=date_range_days-1, random=True)
    .withColumn("currency_index", "integer", minValue=0, maxValue=len(CURRENCIES)-1, random=True)
    .withColumn("random_factor", "decimal(10,6)", minValue=-0.05, maxValue=0.05, random=True)  # ±5% daily volatility
)

fx_df = fx_spec.build()

# Add actual dates
fx_df = fx_df.withColumn(
    "rate_date",
    F.expr(f"date_add('{start_date}', date_offset)")
)

# Create currency lookup DataFrame (Spark Connect compatible — no spark.sparkContext.broadcast)
currency_columns = ["currency_index", "currency_code", "currency_name", "base_rate"]
currency_rows = [(i, c[0], c[1], c[2]) for i, c in enumerate(CURRENCIES)]
currencies_df = spark.createDataFrame(currency_rows, currency_columns)

# Join FX data with currency lookup
fx_df = fx_df.join(currencies_df, on="currency_index", how="left")

# Select final columns
fx_df = fx_df.select(
    "rate_date",
    "currency_code", "currency_name", "base_rate",
    "random_factor"
)

# Calculate realistic exchange rate with trend and volatility
fx_df = fx_df.withColumn(
    "days_since_start",
    F.datediff(F.col("rate_date"), F.lit(start_date))
).withColumn(
    # Add long-term trend (slight appreciation/depreciation over 5 years)
    "trend_factor",
    1.0 + (F.col("days_since_start") / (date_range_days * 20.0)) * F.when(F.rand() > 0.5, 1).otherwise(-1)
).withColumn(
    # Calculate final exchange rate
    "exchange_rate_inr",
    F.round(
        F.col("base_rate") * F.col("trend_factor") * (1.0 + F.col("random_factor")),
        4
    )
).withColumn(
    "rate_type", F.lit("FX")
).withColumn(
    "unit", F.lit("INR")
)

fx_final = fx_df.select(
    "rate_date", "rate_type", "currency_code", "currency_name",
    "exchange_rate_inr", "unit"
)

print(f"✅ FX rates generated: {fx_final.count():,} records")

# COMMAND ----------

# DBTITLE 1,Generate 5 Years of Daily Commodity Prices
print("\n📊 GENERATING COMMODITY PRICES (5 years × 10 commodities)...")

print(f"Expected Commodity Records: {date_range_days} days × {len(COMMODITIES)} commodities = {date_range_days * len(COMMODITIES):,}")

# Generate base commodity data
commodity_spec = (
    dg.DataGenerator(spark, name="commodities", rows=date_range_days * len(COMMODITIES), partitions=4, randomSeed=42)
    .withIdOutput()
    .withColumn("date_offset", "integer", minValue=0, maxValue=date_range_days-1, random=True)
    .withColumn("commodity_index", "integer", minValue=0, maxValue=len(COMMODITIES)-1, random=True)
    .withColumn("random_factor", "decimal(10,6)", minValue=-0.10, maxValue=0.10, random=True)  # ±10% daily volatility
)

commodity_df = commodity_spec.build()

# Add actual dates
commodity_df = commodity_df.withColumn(
    "rate_date",
    F.expr(f"date_add('{start_date}', date_offset)")
)

# Create commodity lookup DataFrame (Spark Connect compatible — no spark.sparkContext.broadcast)
commodity_columns = ["commodity_index", "currency_code", "currency_name", "unit", "base_price", "volatility"]
commodity_rows = [(i, c[0], c[1], c[2], c[3], c[4]) for i, c in enumerate(COMMODITIES)]
commodities_df = spark.createDataFrame(commodity_rows, commodity_columns)

# Join commodity data with lookup
commodity_df = commodity_df.join(commodities_df, on="commodity_index", how="left")

# Select final columns
commodity_df = commodity_df.select(
    "rate_date",
    "currency_code", "currency_name", "base_price",
    "volatility", "unit", "random_factor"
)

# Calculate realistic commodity prices with trend and volatility
commodity_df = commodity_df.withColumn(
    "days_since_start",
    F.datediff(F.col("rate_date"), F.lit(start_date))
).withColumn(
    # Add seasonal trend for agricultural commodities
    "seasonal_factor",
    F.when(
        F.col("currency_code").isin("WHEAT", "RICE", "COTTON"),
        1.0 + 0.1 * F.sin(2 * 3.14159 * F.col("days_since_start") / 365.0)  # Annual seasonality
    ).otherwise(1.0)
).withColumn(
    # Calculate final commodity price
    "exchange_rate_inr",
    F.round(
        F.col("base_price") * F.col("seasonal_factor") * (1.0 + F.col("random_factor")),
        2
    )
).withColumn(
    "rate_type", F.lit("COMMODITY")
)

commodity_final = commodity_df.select(
    "rate_date", "rate_type", "currency_code", "currency_name",
    "exchange_rate_inr", "unit"
)

print(f"✅ Commodity prices generated: {commodity_final.count():,} records")

# COMMAND ----------

# DBTITLE 1,Combine FX and Commodities, Write to Bronze
print("\n💾 COMBINING AND WRITING TO BRONZE LAYER...")

# Union FX and commodity data
combined_df = fx_final.union(commodity_final)

# Add metadata
final_df = combined_df.withColumn(
    "_ingested_at", F.current_timestamp()
).withColumn(
    "_source", F.lit("dbldatagen_fx_commodities")
).withColumn(
    "location_key", F.hash(F.col("currency_code"))  # For joins
)

# Write to bronze
final_df.write.mode("overwrite").saveAsTable(
    f"{Config.CATALOG}.{Config.SCHEMA_BRONZE}.ext_exchange_rates"
)

total_count = final_df.count()

print(f"✅ Exchange rates and commodity prices written: {total_count:,} records")
print(f"   Table: {Config.CATALOG}.{Config.SCHEMA_BRONZE}.ext_exchange_rates")

# COMMAND ----------

# DBTITLE 1,Summary Statistics
print("\n" + "="*80)
print("✅ EXCHANGE RATES & COMMODITY PRICES INGESTION COMPLETE")
print("="*80)
print(f"Total Records: {total_count:,}")
print(f"  - FX Rates: ~{date_range_days * len(CURRENCIES):,}")
print(f"  - Commodities: ~{date_range_days * len(COMMODITIES):,}")
print(f"\nCurrencies: {len(CURRENCIES)} (USD, EUR, GBP, JPY, CNY, etc.)")
print(f"Commodities: {len(COMMODITIES)} (Gold, Silver, Crude Oil, Copper, etc.)")
print(f"Date Range: 2020-01-01 to 2024-12-31 (5 years)")
print(f"\nTable: {Config.CATALOG}.{Config.SCHEMA_BRONZE}.ext_exchange_rates")
print(f"\nColumns:")
print(f"  - rate_date, rate_type (FX/COMMODITY)")
print(f"  - currency_code, currency_name")
print(f"  - exchange_rate_inr (price in INR or original currency)")
print(f"  - unit (INR, USD, etc.)")
print("="*80)

# Display samples
print("\n📊 SAMPLE FX RATES:")
spark.table(f"{Config.CATALOG}.{Config.SCHEMA_BRONZE}.ext_exchange_rates") \
    .filter(F.col("rate_type") == "FX") \
    .filter(F.col("currency_code").isin("USD", "EUR", "GBP")) \
    .orderBy("rate_date", "currency_code") \
    .limit(10).display()

print("\n📊 SAMPLE COMMODITY PRICES:")
spark.table(f"{Config.CATALOG}.{Config.SCHEMA_BRONZE}.ext_exchange_rates") \
    .filter(F.col("rate_type") == "COMMODITY") \
    .filter(F.col("currency_code").isin("GOLD", "SILVER", "CRUDE_OIL")) \
    .orderBy("rate_date", "currency_code") \
    .limit(10).display()

# COMMAND ----------

