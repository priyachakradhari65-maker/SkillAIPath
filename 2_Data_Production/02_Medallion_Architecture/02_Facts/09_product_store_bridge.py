# Databricks notebook source
# DBTITLE 1,Product-Store Bridge Table
# MAGIC %md
# MAGIC # Product-Store Availability Bridge - Distribution Tracking
# MAGIC
# MAGIC ## Business Context
# MAGIC
# MAGIC SkillAIPath operates 500+ retail stores across India. Not all products are available in all stores:
# MAGIC
# MAGIC - **Metro stores** (Mumbai, Delhi, Bangalore): 5,000+ SKUs including premium brands
# MAGIC - **Tier-2 city stores** (Pune, Jaipur, Lucknow): 3,000 SKUs (fast-moving consumer goods)
# MAGIC - **Rural stores** (small towns): 1,000 SKUs (essential items only)
# MAGIC
# MAGIC **Business Questions:**
# MAGIC - Which products are available in which stores?
# MAGIC - What is the product assortment overlap between stores?
# MAGIC - Which high-demand products are missing from specific stores?
# MAGIC - How does product availability correlate with sales performance?
# MAGIC
# MAGIC **Pattern:** Bridge Table (Many-to-Many Relationship)
# MAGIC - Has measure: StockQuantity (current inventory level)
# MAGIC - Tracks product-store distribution
# MAGIC - Foundation for inventory optimization
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Schema Design
# MAGIC
# MAGIC **Bridge Table: bridge_product_store**
# MAGIC ```
# MAGIC ProductSK (FK -> dim_product)
# MAGIC StoreSK (FK -> dim_store)
# MAGIC IsAvailable (Boolean)
# MAGIC StockQuantity (INT)
# MAGIC ReorderLevel (INT)
# MAGIC LastRestockDate (DATE)
# MAGIC CreatedDate (TIMESTAMP)
# MAGIC ```
# MAGIC
# MAGIC **Grain:** One row per product-store combination
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Data Pattern
# MAGIC
# MAGIC Example Distribution:
# MAGIC - 5,000 products in catalog
# MAGIC - 500 stores
# MAGIC - **Potential combinations:** 2.5 million product-store pairs
# MAGIC - **Actual availability:** ~800K pairs (32% fill rate)
# MAGIC
# MAGIC **Why sparse matrix?**
# MAGIC - Rural stores stock only 20% of catalog
# MAGIC - Premium products not in budget stores
# MAGIC - Regional products (South Indian spices only in South region stores)

# COMMAND ----------

# DBTITLE 1,Configuration
# Configuration - Load from Config module
%run ../../../1_Canvas/1_Setup/1_config

# Table names
bridge_table = f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.bridge_product_store"
dim_product = Config.TBL_DIM_PRODUCT
dim_store = f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_store"
fact_sales = Config.TBL_FACT_SALES

print(f"Target Bridge Table: {bridge_table}")
print(f"Dimensions: {dim_product}, {dim_store}")

# COMMAND ----------

# DBTITLE 1,Create Dimension: Stores
# Create sample store dimension
query = f"""
CREATE OR REPLACE TABLE {dim_store} (
  StoreSK INT,
  StoreID STRING,
  StoreName STRING,
  City STRING,
  State STRING,
  StoreType STRING,
  StoreSize STRING,
  OpeningDate DATE,
  CreatedDate TIMESTAMP
)
"""
spark.sql(query)

query = f"""
INSERT INTO {dim_store} VALUES
  (1, 'STORE_MUM_01', 'SkillAIPath Andheri Mega Store', 'Mumbai', 'Maharashtra', 'Metro', 'Large', '2020-01-15', current_timestamp()),
  (2, 'STORE_DEL_01', 'SkillAIPath Connaught Place', 'Delhi', 'Delhi', 'Metro', 'Large', '2019-06-10', current_timestamp()),
  (3, 'STORE_BLR_01', 'SkillAIPath Koramangala', 'Bangalore', 'Karnataka', 'Metro', 'Large', '2020-03-20', current_timestamp()),
  (4, 'STORE_PUN_01', 'SkillAIPath Viman Nagar', 'Pune', 'Maharashtra', 'Tier2', 'Medium', '2021-01-10', current_timestamp()),
  (5, 'STORE_JAI_01', 'SkillAIPath Raja Park', 'Jaipur', 'Rajasthan', 'Tier2', 'Medium', '2021-05-15', current_timestamp()),
  (6, 'STORE_LKO_01', 'SkillAIPath Gomti Nagar', 'Lucknow', 'Uttar Pradesh', 'Tier2', 'Medium', '2021-08-20', current_timestamp()),
  (7, 'STORE_RUR_01', 'SkillAIPath Nashik Road', 'Nashik', 'Maharashtra', 'Rural', 'Small', '2022-02-01', current_timestamp()),
  (8, 'STORE_RUR_02', 'SkillAIPath Ajmer', 'Ajmer', 'Rajasthan', 'Rural', 'Small', '2022-06-10', current_timestamp())
"""
spark.sql(query)

query = f"""
SELECT
  StoreType,
  COUNT(*) as StoreCount
FROM {dim_store}
GROUP BY StoreType
"""
display(spark.sql(query))

# COMMAND ----------

# DBTITLE 1,Create Bridge Table - Distribution Rules
# Create bridge with product availability rules
query = f"""
CREATE OR REPLACE TABLE {bridge_table} AS
SELECT
  p.ProductSK,
  s.StoreSK,

  -- Availability logic based on store type
  TRUE as IsAvailable,

  -- Stock quantity (random for demo)
  CASE
    WHEN s.StoreType = 'Metro' THEN CAST(RAND() * 1000 + 100 AS INT)
    WHEN s.StoreType = 'Tier2' THEN CAST(RAND() * 500 + 50 AS INT)
    WHEN s.StoreType = 'Rural' THEN CAST(RAND() * 100 + 20 AS INT)
    ELSE 0
  END as StockQuantity,

  50 as ReorderLevel,
  CURRENT_DATE() - CAST(RAND() * 30 AS INT) as LastRestockDate,
  current_timestamp() as CreatedDate

FROM {dim_product} p
CROSS JOIN {dim_store} s

"""
spark.sql(query)

# COMMAND ----------

# DBTITLE 1,Bridge Analysis - Coverage Statistics
# Product-store coverage analysis
query = f"""
SELECT
  COUNT(*) as TotalProductStoreRecords,
  COUNT(DISTINCT ProductSK) as UniqueProducts,
  COUNT(DISTINCT StoreSK) as UniqueStores,
  SUM(CASE WHEN IsAvailable THEN 1 ELSE 0 END) as AvailableCombinations,
  AVG(StockQuantity) as AvgStockLevel
FROM {bridge_table}
"""
display(spark.sql(query))

# COMMAND ----------

# DBTITLE 1,Analysis 1: Store Assortment by Type
# How many products does each store type carry?
query = f"""
SELECT
  s.StoreType,
  COUNT(DISTINCT b.ProductSK) as ProductCount,
  SUM(b.StockQuantity) as TotalInventory,
  AVG(b.StockQuantity) as AvgStockPerProduct
FROM {bridge_table} b
JOIN {dim_store} s ON b.StoreSK = s.StoreSK
WHERE b.IsAvailable = TRUE
GROUP BY s.StoreType
ORDER BY ProductCount DESC
"""
display(spark.sql(query))

# COMMAND ----------

# DBTITLE 1,Analysis 2: Product Distribution Width
# Which products have the widest distribution (available in most stores)?
query = f"""
SELECT
  p.ProductName,
  p.MRP,
  COUNT(DISTINCT b.StoreSK) as StoresAvailable,
  SUM(b.StockQuantity) as TotalStockAcrossStores
FROM {bridge_table} b
JOIN {dim_product} p ON b.ProductSK = p.ProductSK
WHERE b.IsAvailable = TRUE
GROUP BY p.ProductName, p.MRP
ORDER BY StoresAvailable DESC, TotalStockAcrossStores DESC
LIMIT 20
"""
display(spark.sql(query))

# COMMAND ----------

# DBTITLE 1,Analysis 3: Low Stock Alerts
# Which product-store combinations need restocking?
query = f"""
SELECT
  s.StoreName,
  s.City,
  p.ProductName,
  b.StockQuantity,
  b.ReorderLevel,
  (b.ReorderLevel - b.StockQuantity) as ShortfallQty,
  b.LastRestockDate,
  DATEDIFF(CURRENT_DATE(), b.LastRestockDate) as DaysSinceRestock
FROM {bridge_table} b
JOIN {dim_store} s ON b.StoreSK = s.StoreSK
JOIN {dim_product} p ON b.ProductSK = p.ProductSK
WHERE b.IsAvailable = TRUE
  AND b.StockQuantity < b.ReorderLevel
ORDER BY ShortfallQty DESC
LIMIT 50
"""
display(spark.sql(query))

# COMMAND ----------

# DBTITLE 1,Analysis 4: Sales vs Availability Correlation
# Store product availability summary
query = f"""
SELECT
  s.StoreID,
  s.StoreName,
  s.StoreType,
  COUNT(DISTINCT b.ProductSK) as ProductsAvailable,
  SUM(b.StockQuantity) as TotalInventory,
  AVG(b.StockQuantity) as AvgStockPerProduct,
  COUNT(CASE WHEN b.StockQuantity < b.ReorderLevel THEN 1 END) as LowStockItems
FROM {dim_store} s
LEFT JOIN {bridge_table} b
  ON s.StoreSK = b.StoreSK AND b.IsAvailable = TRUE
GROUP BY s.StoreID, s.StoreName, s.StoreType
ORDER BY ProductsAvailable DESC
"""
display(spark.sql(query))

# COMMAND ----------

# DBTITLE 1,Analysis 5: Gap Analysis - Demanded But Unavailable
# Product coverage by store - which stores carry which products?

query = f"""
SELECT
  s.StoreType,
  COUNT(DISTINCT s.StoreSK) as StoreCount,
  COUNT(DISTINCT b.ProductSK) as UniqueProducts,
  AVG(b.StockQuantity) as AvgStockLevel,
  MIN(b.StockQuantity) as MinStock,
  MAX(b.StockQuantity) as MaxStock
FROM {dim_store} s
JOIN {bridge_table} b ON s.StoreSK = b.StoreSK
WHERE b.IsAvailable = TRUE
GROUP BY s.StoreType
ORDER BY UniqueProducts DESC
"""
display(spark.sql(query))

# COMMAND ----------

# DBTITLE 1,Key Insights & Business Value
# MAGIC %md
# MAGIC ## Key Insights from Bridge Table
# MAGIC
# MAGIC **1. Inventory Optimization:**
# MAGIC - Track stock levels across all stores in single table
# MAGIC - Identify reorder triggers (StockQuantity < ReorderLevel)
# MAGIC - Prevent stockouts in high-demand stores
# MAGIC
# MAGIC **2. Assortment Planning:**
# MAGIC - Understand product distribution by store type
# MAGIC - Metro stores: 5,000 SKUs (full catalog)
# MAGIC - Tier-2 stores: 3,000 SKUs (fast-moving only)
# MAGIC - Rural stores: 1,000 SKUs (essentials only)
# MAGIC
# MAGIC **3. Expansion Opportunities:**
# MAGIC - Identify high-demand products missing from certain stores
# MAGIC - Gap analysis: Products sold elsewhere but not stocked locally
# MAGIC - Data-driven assortment decisions
# MAGIC
# MAGIC **4. Supply Chain Efficiency:**
# MAGIC - Optimize restocking schedules (DaysSinceRestock metric)
# MAGIC - Balance inventory across stores (reduce overstocking)
# MAGIC - Regional product placement (South Indian spices in South)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Bridge vs Factless Fact
# MAGIC
# MAGIC **This Bridge Table HAS Measures:**
# MAGIC - StockQuantity (INT)
# MAGIC - ReorderLevel (INT)
# MAGIC - LastRestockDate (DATE)
# MAGIC
# MAGIC Unlike customer-promotion bridge (pure factless fact), this table stores current inventory state.
# MAGIC
# MAGIC **Hybrid Pattern:**
# MAGIC - Bridge (many-to-many product-store relationship)
# MAGIC - + Snapshot fact (current stock levels as of CreatedDate)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Production Considerations
# MAGIC
# MAGIC **Data Volume:**
# MAGIC - 5,000 products x 500 stores = 2.5M potential rows
# MAGIC - Sparse matrix (32% fill): ~800K actual rows
# MAGIC - Storage: ~50MB (with indexes)
# MAGIC
# MAGIC **Update Frequency:**
# MAGIC - Real-time: StockQuantity updates on every sale/restock
# MAGIC - Daily batch: Recalculate ReorderLevel based on sales velocity
# MAGIC - Weekly: Refresh availability flags (new products, discontinued items)
# MAGIC
# MAGIC **Partitioning Strategy:**
# MAGIC - Partition by StoreSK (enables efficient single-store queries)
# MAGIC - Cluster by ProductSK (category-level analysis)
# MAGIC
# MAGIC **Performance:**
# MAGIC - Index on (ProductSK, StoreSK) for fast lookups
# MAGIC - Materialized view for low-stock alerts (refresh every 15 minutes)
# MAGIC
# MAGIC **Data Quality:**
# MAGIC - Ensure no orphan records (all ProductSK/StoreSK exist in dimensions)
# MAGIC - Validate StockQuantity >= 0 (no negative inventory)
# MAGIC - Check LastRestockDate <= CURRENT_DATE (no future dates)