# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Indian Logistics & Delivery Data - Bronze Ingestion
# MAGIC %md
# MAGIC # Indian Logistics & Delivery Network - Bronze Layer (PRODUCTION-GRADE)
# MAGIC
# MAGIC **Business Value:** Last-mile delivery performance and logistics optimization
# MAGIC
# MAGIC **Data Generation:** dbldatagen + Faker(en_IN) - 500,000+ realistic delivery records
# MAGIC
# MAGIC **Coverage:**
# MAGIC - **500,000+ Deliveries** across India (2 years of data)
# MAGIC - **30 Major Cities** with realistic PIN codes
# MAGIC - **10 Courier Partners** (Delhivery, BlueDart, DTDC, etc.)
# MAGIC - **Realistic Delivery Times**: Same-day, Next-day, 2-7 days
# MAGIC - **Delivery Status**: Delivered, In-Transit, Failed, RTO
# MAGIC - **Distance and Cost Metrics**
# MAGIC
# MAGIC **Output Table:**
# MAGIC - `sap_dev.bronze.ext_logistics_deliveries` - 500,000+ delivery transactions
# MAGIC
# MAGIC **Use Cases:**
# MAGIC 1. Courier partner performance benchmarking (on-time %, cost efficiency)
# MAGIC 2. Delivery time prediction models (by distance, city, partner)
# MAGIC 3. Regional logistics optimization (identify high-cost/slow zones)
# MAGIC 4. Return-to-Origin (RTO) analysis and reduction strategies
# MAGIC 5. Customer satisfaction correlation with delivery experience

# COMMAND ----------

# DBTITLE 1,Load Config
# MAGIC %run ../../1_Canvas/1_Setup/1_Config

# COMMAND ----------

# DBTITLE 1,Install Dependencies
# MAGIC %pip install dbldatagen faker --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Reload Config After Restart
# MAGIC %run ../../1_Canvas/1_Setup/1_Config

# COMMAND ----------

# DBTITLE 1,Import Libraries
import dbldatagen as dg
from faker import Faker
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, date, timedelta
import random

fake = Faker('en_IN')
Faker.seed(42)

print("="*80)
print("INDIAN LOGISTICS & DELIVERY DATA - RICH GENERATION")
print("="*80)
print(f"Target: {Config.CATALOG}.{Config.SCHEMA_BRONZE}.ext_logistics_deliveries")
print(f"Volume: 500,000+ delivery transactions (2 years)")
print(f"Period: 2023-01-01 to 2024-12-31")
print(f"Coverage: 30 cities, 10 courier partners")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Define Courier Partners and Cities
# Major Indian courier partners
COURIER_PARTNERS = [
    ("DELHIVERY", "Delhivery Express", 0.25),  # Market share weights
    ("BLUEDART", "Blue Dart Express", 0.20),
    ("DTDC", "DTDC Courier", 0.15),
    ("ECOM_EXPRESS", "Ecom Express", 0.12),
    ("XPRESSBEES", "XpressBees", 0.10),
    ("EKART", "Ekart Logistics", 0.08),
    ("SHADOWFAX", "Shadowfax", 0.05),
    ("FEDEX", "FedEx India", 0.03),
    ("DHL", "DHL Express", 0.02)
]

# Major Indian cities with tier classification
INDIAN_CITIES_LOGISTICS = [
    # (city, state, tier, avg_distance_km, pin_base)
    ("Mumbai", "Maharashtra", "Tier-1", 15, 400001),
    ("Delhi", "Delhi", "Tier-1", 20, 110001),
    ("Bangalore", "Karnataka", "Tier-1", 18, 560001),
    ("Hyderabad", "Telangana", "Tier-1", 17, 500001),
    ("Chennai", "Tamil Nadu", "Tier-1", 16, 600001),
    ("Kolkata", "West Bengal", "Tier-1", 19, 700001),
    ("Pune", "Maharashtra", "Tier-1", 14, 411001),
    ("Ahmedabad", "Gujarat", "Tier-1", 15, 380001),
    ("Surat", "Gujarat", "Tier-2", 12, 395001),
    ("Jaipur", "Rajasthan", "Tier-2", 13, 302001),
    ("Lucknow", "Uttar Pradesh", "Tier-2", 14, 226001),
    ("Kanpur", "Uttar Pradesh", "Tier-2", 15, 208001),
    ("Nagpur", "Maharashtra", "Tier-2", 13, 440001),
    ("Indore", "Madhya Pradesh", "Tier-2", 12, 452001),
    ("Thane", "Maharashtra", "Tier-2", 11, 400601),
    ("Bhopal", "Madhya Pradesh", "Tier-2", 12, 462001),
    ("Visakhapatnam", "Andhra Pradesh", "Tier-2", 14, 530001),
    ("Patna", "Bihar", "Tier-2", 13, 800001),
    ("Vadodara", "Gujarat", "Tier-2", 11, 390001),
    ("Ghaziabad", "Uttar Pradesh", "Tier-2", 10, 201001),
    ("Ludhiana", "Punjab", "Tier-2", 12, 141001),
    ("Agra", "Uttar Pradesh", "Tier-3", 11, 282001),
    ("Nashik", "Maharashtra", "Tier-3", 10, 422001),
    ("Faridabad", "Haryana", "Tier-3", 12, 121001),
    ("Meerut", "Uttar Pradesh", "Tier-3", 11, 250001),
    ("Rajkot", "Gujarat", "Tier-3", 10, 360001),
    ("Varanasi", "Uttar Pradesh", "Tier-3", 12, 221001),
    ("Amritsar", "Punjab", "Tier-3", 11, 143001),
    ("Coimbatore", "Tamil Nadu", "Tier-3", 10, 641001),
    ("Kochi", "Kerala", "Tier-2", 13, 682001),
]

print(f"\n🚚 COURIER PARTNERS: {len(COURIER_PARTNERS)}")
for code, name, share in COURIER_PARTNERS[:5]:
    print(f"   - {code}: {name} ({share*100:.0f}% market share)")
print(f"   ... and {len(COURIER_PARTNERS) - 5} more partners")

print(f"\n📍 CITIES COVERED: {len(INDIAN_CITIES_LOGISTICS)}")
for city, state, tier, _, _ in INDIAN_CITIES_LOGISTICS[:5]:
    print(f"   - {city}, {state} ({tier})")
print(f"   ... and {len(INDIAN_CITIES_LOGISTICS) - 5} more cities")

# COMMAND ----------

# DBTITLE 1,Generate 500K Delivery Transactions
print("\n📦 GENERATING DELIVERY TRANSACTIONS (500,000 records)...")

# Date range: 2 years
start_date = date(2023, 1, 1)
end_date = date(2024, 12, 31)
date_range_days = (end_date - start_date).days + 1

print(f"Date Range: {start_date} to {end_date} ({date_range_days} days)")

# Generate base delivery data
delivery_spec = (
    dg.DataGenerator(spark, name="deliveries", rows=500000, partitions=16, randomSeed=42)
    .withIdOutput()  # delivery_id
    .withColumn("tracking_number", "string", template=r"TRK\n{12}", random=True, uniqueValues=500000)
    .withColumn("order_date_offset", "integer", minValue=0, maxValue=date_range_days-1, random=True)
    .withColumn("city_index", "integer", minValue=0, maxValue=len(INDIAN_CITIES_LOGISTICS)-1, random=True)
    .withColumn("courier_index", "integer", minValue=0, maxValue=len(COURIER_PARTNERS)-1, random=True)
    .withColumn("package_weight_kg", "decimal(8,2)", minValue=0.1, maxValue=50.0, random=True)
    .withColumn("distance_km", "integer", minValue=5, maxValue=2500, random=True)
    .withColumn("shipping_cost_inr", "decimal(10,2)", minValue=30.0, maxValue=2500.0, random=True)
)

delivery_df = delivery_spec.build()

print(f"✅ Base delivery framework generated: 500,000 records")

# COMMAND ----------

# DBTITLE 1,Add City and Courier Information
print("\n📍 ADDING CITY AND COURIER DETAILS...")

# Create city lookup DataFrame (Spark Connect compatible — no spark.sparkContext.broadcast)
city_columns = ["city_index", "destination_city", "destination_state", "city_tier", "avg_distance", "pin_base"]
city_rows = [(i, c[0], c[1], c[2], c[3], c[4]) for i, c in enumerate(INDIAN_CITIES_LOGISTICS)]
cities_df = spark.createDataFrame(city_rows, city_columns)

# Create courier lookup DataFrame
courier_columns = ["courier_index", "courier_code", "courier_name", "market_share"]
courier_rows = [(i, c[0], c[1], c[2]) for i, c in enumerate(COURIER_PARTNERS)]
couriers_df = spark.createDataFrame(courier_rows, courier_columns)

# Join delivery data with city and courier lookups
delivery_df = delivery_df.join(F.broadcast(cities_df), on="city_index", how="left")
delivery_df = delivery_df.join(F.broadcast(couriers_df), on="courier_index", how="left")

# Select final columns
delivery_df = delivery_df.select(
    "id", "tracking_number", "order_date_offset",
    "destination_city", "destination_state", "city_tier", "pin_base",
    "courier_code", "courier_name",
    "package_weight_kg", "distance_km", "shipping_cost_inr"
)

# Add realistic PIN code
delivery_df = delivery_df.withColumn(
    "destination_pincode",
    F.concat(
        F.col("pin_base").cast("string"),
        F.lpad((F.rand() * 999).cast("int").cast("string"), 3, "0")
    )
)

print("✅ City and courier details added")

# COMMAND ----------

# DBTITLE 1,Calculate Delivery Dates and Status
print("\n📅 CALCULATING DELIVERY TIMELINE AND STATUS...")

# Add order date
delivery_df = delivery_df.withColumn(
    "order_date",
    F.expr(f"date_add('{start_date}', order_date_offset)")
)

# Calculate expected delivery days based on distance and city tier
delivery_df = delivery_df.withColumn(
    "expected_delivery_days",
    F.when(F.col("distance_km") < 50, 1)  # Same city: 1 day
    .when((F.col("distance_km") < 300) & (F.col("city_tier") == "Tier-1"), 2)  # Nearby Tier-1: 2 days
    .when((F.col("distance_km") < 500) & (F.col("city_tier").isin("Tier-1", "Tier-2")), 3)  # Inter-city: 3 days
    .when(F.col("distance_km") < 1000, 4)  # Long distance: 4 days
    .otherwise(5 + (F.rand() * 3).cast("int"))  # Very long distance: 5-7 days
)

# Add delivery delay factor (some deliveries are delayed)
delivery_df = delivery_df.withColumn(
    "actual_delivery_days",
    F.when(F.rand() < 0.70, F.col("expected_delivery_days"))  # 70% on-time
    .when(F.rand() < 0.85, F.col("expected_delivery_days") + 1)  # 15% 1-day delay
    .when(F.rand() < 0.95, F.col("expected_delivery_days") + (F.rand() * 3).cast("int"))  # 10% 2-3 day delay
    .otherwise(F.col("expected_delivery_days") + (F.rand() * 7).cast("int"))  # 5% major delay
)

# Calculate actual delivery date
delivery_df = delivery_df.withColumn(
    "delivered_date",
    F.expr("date_add(order_date, actual_delivery_days)")
)

# Delivery status (90% delivered, 5% in-transit, 3% failed, 2% RTO)
delivery_df = delivery_df.withColumn(
    "delivery_status",
    F.when(F.rand() < 0.90, "Delivered")
    .when(F.rand() < 0.95, "In-Transit")
    .when(F.rand() < 0.98, "Failed")
    .otherwise("RTO")  # Return to Origin
)

# For non-delivered, set delivered_date to null
delivery_df = delivery_df.withColumn(
    "delivered_date",
    F.when(F.col("delivery_status") == "Delivered", F.col("delivered_date"))
    .otherwise(F.lit(None).cast("date"))
)

print("✅ Delivery timeline and status calculated")

# COMMAND ----------

# DBTITLE 1,Add Performance Metrics
print("\n⚡ ADDING PERFORMANCE METRICS...")

# On-time delivery flag
delivery_df = delivery_df.withColumn(
    "on_time_delivery",
    F.when(F.col("delivery_status") == "Delivered",
           F.col("actual_delivery_days") <= F.col("expected_delivery_days"))
    .otherwise(F.lit(None).cast("boolean"))
)

# Customer rating (1-5 stars, correlated with on-time delivery)
delivery_df = delivery_df.withColumn(
    "customer_rating",
    F.when(F.col("delivery_status") == "Delivered",
           F.when(F.col("on_time_delivery") == True, 4 + (F.rand() * 1).cast("int"))  # 4-5 stars
           .when(F.col("actual_delivery_days") <= F.col("expected_delivery_days") + 1, 3 + (F.rand() * 1).cast("int"))  # 3-4 stars
           .otherwise(2 + (F.rand() * 2).cast("int")))  # 2-3 stars
    .otherwise(F.lit(None).cast("int"))
)

# Delivery attempt count
delivery_df = delivery_df.withColumn(
    "delivery_attempts",
    F.when(F.col("delivery_status") == "Delivered", 1 + (F.rand() * 2).cast("int"))  # 1-3 attempts
    .when(F.col("delivery_status") == "Failed", 2 + (F.rand() * 2).cast("int"))  # 2-4 attempts
    .when(F.col("delivery_status") == "RTO", 3 + (F.rand() * 1).cast("int"))  # 3-4 attempts
    .otherwise(1)  # In-transit: 1 attempt so far
)

# Return reason for failed/RTO
delivery_df = delivery_df.withColumn(
    "return_reason",
    F.when(F.col("delivery_status").isin("Failed", "RTO"),
           F.array(
               F.lit("Customer unavailable"), F.lit("Incorrect address"), 
               F.lit("Customer refused"), F.lit("Damaged package"),
               F.lit("Payment issue"), F.lit("Delayed beyond acceptable time")
           ).getItem((F.rand() * 6).cast("int")))
    .otherwise(F.lit(None).cast("string"))
)

print("✅ Performance metrics added")

# COMMAND ----------

# DBTITLE 1,Add Metadata and Write to Bronze
print("\n💾 WRITING TO BRONZE LAYER...")

# Select final columns
delivery_final = delivery_df.select(
    F.col("id").alias("delivery_id"),
    "tracking_number",
    "order_date",
    "delivered_date",
    "destination_city",
    "destination_state",
    "destination_pincode",
    "city_tier",
    "courier_code",
    "courier_name",
    "package_weight_kg",
    "distance_km",
    "shipping_cost_inr",
    "expected_delivery_days",
    "actual_delivery_days",
    "delivery_status",
    "on_time_delivery",
    "customer_rating",
    "delivery_attempts",
    "return_reason"
).withColumn(
    "_ingested_at", F.current_timestamp()
).withColumn(
    "_source", F.lit("dbldatagen_indian_logistics")
)

# Write to bronze — count from written table (avoids double computation)
delivery_final.write.mode("overwrite").saveAsTable(
    f"{Config.CATALOG}.{Config.SCHEMA_BRONZE}.ext_logistics_deliveries"
)

# Get count from the written Delta table (uses metadata, no recomputation)
delivery_count = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_BRONZE}.ext_logistics_deliveries").count()

print(f"✅ Delivery data written: {delivery_count:,} records")
print(f"   Table: {Config.CATALOG}.{Config.SCHEMA_BRONZE}.ext_logistics_deliveries")

# COMMAND ----------

# DBTITLE 1,Summary Statistics and Validation
print("\n" + "="*80)
print("✅ INDIAN LOGISTICS DATA INGESTION COMPLETE (PRODUCTION-GRADE)")
print("="*80)
print(f"Total Records: {delivery_count:,}")
print(f"Date Range: {start_date} to {end_date} (2 years)")
print(f"Cities: {len(INDIAN_CITIES_LOGISTICS)} (Tier-1, Tier-2, Tier-3)")
print(f"Courier Partners: {len(COURIER_PARTNERS)}")
print(f"\nDelivery Statistics:")

stats_df = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_BRONZE}.ext_logistics_deliveries")

# Overall status distribution
print("\nDelivery Status Distribution:")
stats_df.groupBy("delivery_status").count() \
    .withColumn("percentage", F.round(F.col("count") / delivery_count * 100, 1)) \
    .orderBy(F.desc("count")).show(10, False)

# On-time delivery rate by courier
print("\nOn-Time Delivery Rate by Courier:")
stats_df.filter(F.col("delivery_status") == "Delivered") \
    .groupBy("courier_name").agg(
        F.count("*").alias("deliveries"),
        F.round(F.avg(F.when(F.col("on_time_delivery"), 1).otherwise(0)) * 100, 1).alias("on_time_pct"),
        F.round(F.avg("customer_rating"), 2).alias("avg_rating")
    ).orderBy(F.desc("on_time_pct")).show(10, False)

# Delivery performance by city tier
print("\nDelivery Performance by City Tier:")
stats_df.filter(F.col("delivery_status") == "Delivered") \
    .groupBy("city_tier").agg(
        F.count("*").alias("deliveries"),
        F.round(F.avg("actual_delivery_days"), 1).alias("avg_days"),
        F.round(F.avg(F.when(F.col("on_time_delivery"), 1).otherwise(0)) * 100, 1).alias("on_time_pct")
    ).orderBy("city_tier").show()

print(f"\nTable: {Config.CATALOG}.{Config.SCHEMA_BRONZE}.ext_logistics_deliveries")
print(f"\nColumns Available:")
print(f"  - delivery_id, tracking_number")
print(f"  - order_date, delivered_date")
print(f"  - destination_city, destination_state, destination_pincode, city_tier")
print(f"  - courier_code, courier_name")
print(f"  - package_weight_kg, distance_km, shipping_cost_inr")
print(f"  - expected_delivery_days, actual_delivery_days")
print(f"  - delivery_status, on_time_delivery, customer_rating")
print(f"  - delivery_attempts, return_reason")
print("="*80)

# Display sample
print("\n📦 SAMPLE DELIVERY RECORDS:")
stats_df.orderBy("order_date").limit(10).display()

# COMMAND ----------

