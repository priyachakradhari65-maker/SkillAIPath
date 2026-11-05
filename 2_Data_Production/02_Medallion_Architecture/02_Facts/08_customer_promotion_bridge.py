# Databricks notebook source
# DBTITLE 1,Customer-Promotion Bridge Table
# MAGIC %md
# MAGIC # Customer-Promotion Bridge Table - Factless Fact Pattern
# MAGIC
# MAGIC ## Business Context
# MAGIC
# MAGIC SkillAIPath runs targeted promotional campaigns (Diwali Sale, Summer Clearance, Premium Member Offers). Not all customers are exposed to all promotions - eligibility depends on:
# MAGIC
# MAGIC - Customer segment (Premium, Regular, New)
# MAGIC - Purchase history
# MAGIC - Geographic location
# MAGIC - Campaign timing
# MAGIC
# MAGIC **Business Questions:**
# MAGIC - Which customers were eligible for which promotions?
# MAGIC - What was the promotion exposure rate by segment?
# MAGIC - Which promotions had the highest customer reach?
# MAGIC - Did exposed customers actually make purchases?
# MAGIC
# MAGIC **Pattern:** Factless Fact (Many-to-Many Relationship)
# MAGIC - No measures - records occurrence only
# MAGIC - Tracks eligibility/exposure event
# MAGIC - Bridge between Customer and Promotion dimensions
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Schema Design
# MAGIC
# MAGIC **Bridge Table: bridge_customer_promotion**
# MAGIC ```
# MAGIC CustomerSK (FK -> dim_customer)
# MAGIC PromotionSK (FK -> dim_promotion)
# MAGIC ExposureDate
# MAGIC ExposureChannel (Email, SMS, App Push, In-Store)
# MAGIC IsEligible (Boolean)
# MAGIC DidPurchase (Boolean - derived from fact_sales)
# MAGIC CreatedDate
# MAGIC ```
# MAGIC
# MAGIC **Grain:** One row per customer-promotion exposure
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Data Pattern
# MAGIC
# MAGIC Example: Diwali Sale 2024
# MAGIC - 10,000 customers in database
# MAGIC - 3,000 Premium customers (100% exposed via all channels)
# MAGIC - 5,000 Regular customers (50% exposed via email only)
# MAGIC - 2,000 New customers (excluded - not eligible)
# MAGIC
# MAGIC **Bridge Records:** ~5,500 customer-promotion pairs

# COMMAND ----------

# DBTITLE 1,Configuration
# Configuration - Load from Config module
%run ../../../1_Canvas/1_Setup/1_config

# Table names
bridge_table = f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.bridge_customer_promotion"
dim_customer = Config.TBL_DIM_CUSTOMER
dim_promotion = f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_promotion"
fact_sales = Config.TBL_FACT_SALES

print(f"Target Bridge Table: {bridge_table}")
print(f"Dimensions: {dim_customer}, {dim_promotion}")

# COMMAND ----------

# DBTITLE 1,Create Dimension: Promotions
# Create sample promotion dimension
query = f"""
CREATE OR REPLACE TABLE {dim_promotion} (
  PromotionSK INT,
  PromotionID STRING,
  PromotionName STRING,
  PromotionType STRING,
  StartDate DATE,
  EndDate DATE,
  DiscountPercent DECIMAL(5,2),
  TargetSegment STRING,
  CreatedDate TIMESTAMP
)
"""
spark.sql(query)

query = f"""
INSERT INTO {dim_promotion} VALUES
  (1, 'PROMO2024DIWALI', 'Diwali Mega Sale 2024', 'Seasonal', '2024-10-20', '2024-11-05', 25.00, 'All', current_timestamp()),
  (2, 'PROMO2024SUMMER', 'Summer Clearance 2024', 'Seasonal', '2024-05-01', '2024-05-31', 30.00, 'Regular,Premium', current_timestamp()),
  (3, 'PROMO2024PREMIUM', 'Premium Member Exclusive', 'Loyalty', '2024-01-01', '2024-12-31', 15.00, 'Premium', current_timestamp()),
  (4, 'PROMO2024FLASH', 'Flash Weekend Sale', 'Flash', '2024-06-01', '2024-06-02', 40.00, 'All', current_timestamp()),
  (5, 'PROMO2024NEWJOIN', 'New Customer Welcome Offer', 'Acquisition', '2024-01-01', '2024-12-31', 10.00, 'New', current_timestamp())
"""
spark.sql(query)

display(spark.sql(f"SELECT * FROM {dim_promotion}"))

# COMMAND ----------

# DBTITLE 1,Sample Customer Segments
# Verify customer data in dimension
query = f"""
SELECT
  CustomerName,
  CustomerGender,
  COUNT(*) as CustomerCount
FROM {dim_customer}
WHERE IsCurrent = 1
GROUP BY CustomerName, CustomerGender
ORDER BY CustomerCount DESC
LIMIT 10
"""
display(spark.sql(query))

# COMMAND ----------

# DBTITLE 1,Create Bridge Table - Eligibility Rules
# Create bridge table with eligibility logic
query = f"""
CREATE OR REPLACE TABLE {bridge_table} AS
WITH customer_segments AS (
  SELECT
    CustomerSK,
    CASE
      WHEN CustomerAge >= 50 THEN 'Premium'
      WHEN CustomerAge >= 30 THEN 'Regular'
      ELSE 'New'
    END as CustomerSegment
  FROM {dim_customer}
  WHERE IsCurrent = 1
)
SELECT
  c.CustomerSK,
  p.PromotionSK,
  p.StartDate as ExposureDate,

  -- Exposure channel logic
  CASE
    WHEN cs.CustomerSegment = 'Premium' THEN 'Multi-Channel'
    WHEN cs.CustomerSegment = 'Regular' THEN 'Email'
    WHEN cs.CustomerSegment = 'New' AND p.TargetSegment = 'New' THEN 'App Push'
    ELSE 'None'
  END as ExposureChannel,

  -- Eligibility logic
  CASE
    WHEN p.TargetSegment = 'All' THEN TRUE
    WHEN p.TargetSegment LIKE '%' || cs.CustomerSegment || '%' THEN TRUE
    ELSE FALSE
  END as IsEligible,

  FALSE as DidPurchase,
  current_timestamp() as CreatedDate

FROM {dim_customer} c
JOIN customer_segments cs ON c.CustomerSK = cs.CustomerSK
CROSS JOIN {dim_promotion} p
WHERE c.IsCurrent = 1
  AND (
    p.TargetSegment = 'All'
    OR p.TargetSegment LIKE '%' || cs.CustomerSegment || '%'
  )
"""
spark.sql(query)

# COMMAND ----------

# DBTITLE 1,Bridge Table Analysis - Row Count
# Verify bridge table creation
query = f"""
SELECT
  COUNT(*) as TotalExposures,
  COUNT(DISTINCT CustomerSK) as UniqueCustomers,
  COUNT(DISTINCT PromotionSK) as UniquePromotions,
  SUM(CASE WHEN IsEligible THEN 1 ELSE 0 END) as EligibleExposures
FROM {bridge_table}
"""
display(spark.sql(query))

# COMMAND ----------

# DBTITLE 1,Analysis 1: Promotion Reach by Segment
# Which customer segments were exposed to which promotions?
query = f"""
WITH customer_segments AS (
  SELECT
    CustomerSK,
    CASE
      WHEN CustomerAge >= 50 THEN 'Premium'
      WHEN CustomerAge >= 30 THEN 'Regular'
      ELSE 'New'
    END as CustomerSegment
  FROM {dim_customer}
  WHERE IsCurrent = 1
)
SELECT
  p.PromotionName,
  cs.CustomerSegment,
  COUNT(DISTINCT b.CustomerSK) as ExposedCustomers,
  b.ExposureChannel,
  SUM(CASE WHEN b.IsEligible THEN 1 ELSE 0 END) as EligibleCustomers
FROM {bridge_table} b
JOIN {dim_customer} c ON b.CustomerSK = c.CustomerSK AND c.IsCurrent = 1
JOIN customer_segments cs ON c.CustomerSK = cs.CustomerSK
JOIN {dim_promotion} p ON b.PromotionSK = p.PromotionSK
GROUP BY p.PromotionName, cs.CustomerSegment, b.ExposureChannel
ORDER BY p.PromotionName, ExposedCustomers DESC
"""
display(spark.sql(query))

# COMMAND ----------

# DBTITLE 1,Analysis 2: Customer Promotion History
# How many promotions was each customer exposed to?
query = f"""
WITH customer_segments AS (
  SELECT
    CustomerSK,
    CustomerID,
    CustomerName,
    CASE
      WHEN CustomerAge >= 50 THEN 'Premium'
      WHEN CustomerAge >= 30 THEN 'Regular'
      ELSE 'New'
    END as CustomerSegment
  FROM {dim_customer}
  WHERE IsCurrent = 1
)
SELECT
  cs.CustomerID,
  cs.CustomerName,
  cs.CustomerSegment,
  COUNT(DISTINCT b.PromotionSK) as PromotionsExposed,
  CONCAT_WS(', ', COLLECT_LIST(p.PromotionName)) as PromotionList
FROM customer_segments cs
JOIN {bridge_table} b ON cs.CustomerSK = b.CustomerSK
JOIN {dim_promotion} p ON b.PromotionSK = p.PromotionSK
GROUP BY cs.CustomerID, cs.CustomerName, cs.CustomerSegment
ORDER BY PromotionsExposed DESC
LIMIT 20
"""
display(spark.sql(query))

# COMMAND ----------

# DBTITLE 1,Analysis 3: Promotion Effectiveness (Join to Fact Sales)
# Update DidPurchase flag based on fact_sales
# Which customers exposed to promotions actually made purchases during campaign period?

query = f"""
MERGE INTO {bridge_table} AS bridge
USING (
  SELECT DISTINCT
    b.CustomerSK,
    b.PromotionSK
  FROM {bridge_table} b
  JOIN {dim_promotion} p ON b.PromotionSK = p.PromotionSK
  JOIN {fact_sales} f
    ON b.CustomerSK = f.CustomerSK
    AND f.BatchDate BETWEEN p.StartDate AND p.EndDate
) AS purchases
ON bridge.CustomerSK = purchases.CustomerSK
   AND bridge.PromotionSK = purchases.PromotionSK
WHEN MATCHED THEN
  UPDATE SET bridge.DidPurchase = TRUE
"""
spark.sql(query)

# Analysis: Promotion conversion rate
query = f"""
SELECT
  p.PromotionName,
  COUNT(DISTINCT b.CustomerSK) as ExposedCustomers,
  SUM(CASE WHEN b.DidPurchase THEN 1 ELSE 0 END) as ConvertedCustomers,
  ROUND(SUM(CASE WHEN b.DidPurchase THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT b.CustomerSK), 2) as ConversionRate
FROM {bridge_table} b
JOIN {dim_promotion} p ON b.PromotionSK = p.PromotionSK
GROUP BY p.PromotionName
ORDER BY ConversionRate DESC
"""
display(spark.sql(query))

# COMMAND ----------

# DBTITLE 1,Analysis 4: Channel Effectiveness
# Which exposure channels have the highest conversion rates?
query = f"""
SELECT
  ExposureChannel,
  COUNT(DISTINCT CustomerSK) as ExposedCustomers,
  SUM(CASE WHEN DidPurchase THEN 1 ELSE 0 END) as ConvertedCustomers,
  ROUND(SUM(CASE WHEN DidPurchase THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT CustomerSK), 2) as ConversionRate
FROM {bridge_table}
GROUP BY ExposureChannel
ORDER BY ConversionRate DESC
"""
display(spark.sql(query))

# COMMAND ----------

# DBTITLE 1,Key Insights & Business Value
# MAGIC %md
# MAGIC ## Key Insights from Bridge Table
# MAGIC
# MAGIC **1. Targeted Marketing:**
# MAGIC - Track which customers were exposed to which campaigns
# MAGIC - Measure reach by segment (Premium vs Regular vs New)
# MAGIC - Optimize channel selection (Multi-Channel vs Email vs App Push)
# MAGIC
# MAGIC **2. Campaign Effectiveness:**
# MAGIC - Calculate conversion rates (exposed customers who purchased)
# MAGIC - Compare channel effectiveness (which channel converts best)
# MAGIC - Identify under-performing campaigns
# MAGIC
# MAGIC **3. Customer Journey Analysis:**
# MAGIC - Understand promotion fatigue (customers exposed to too many campaigns)
# MAGIC - Identify high-value customers who respond to promotions
# MAGIC - Segment customers by promotion responsiveness
# MAGIC
# MAGIC **4. Budget Allocation:**
# MAGIC - Eliminate wasteful exposures (customers who never convert)
# MAGIC - Focus spend on high-converting segments
# MAGIC - Test new channels with low-risk segments
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Why Factless Fact?
# MAGIC
# MAGIC **No Measures:**
# MAGIC - Table records occurrence of exposure event
# MAGIC - No numeric values to aggregate (no revenue, quantity, amount)
# MAGIC - Pure relationship tracking
# MAGIC
# MAGIC **Benefits:**
# MAGIC - Answers "who was exposed to what" questions
# MAGIC - Enables many-to-many analysis (customer-promotion pairs)
# MAGIC - Foundation for behavioral segmentation
# MAGIC
# MAGIC **Compared to Traditional Fact:**
# MAGIC - fact_sales has measures (Quantity, TotalAmount)
# MAGIC - bridge_customer_promotion has no measures (just flags: IsEligible, DidPurchase)
# MAGIC - Bridge enables analysis of non-purchasing behavior
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Production Considerations
# MAGIC
# MAGIC **Data Volume:**
# MAGIC - 10K customers x 5 promotions = 50K potential rows
# MAGIC - Filter to eligible combinations only (saves 30-40% storage)
# MAGIC - Partition by PromotionSK for efficient queries
# MAGIC
# MAGIC **Update Frequency:**
# MAGIC - Daily batch: Add new customer-promotion pairs
# MAGIC - Hourly/Real-time: Update DidPurchase flag from fact_sales
# MAGIC
# MAGIC **Data Retention:**
# MAGIC - Keep historical exposures for 2 years
# MAGIC - Archive after promotion end date + 90 days
# MAGIC
# MAGIC **Indexes:**
# MAGIC - CustomerSK (common filter in customer journey queries)
# MAGIC - PromotionSK (common filter in campaign analysis)
# MAGIC - Composite index on (CustomerSK, PromotionSK) for uniqueness