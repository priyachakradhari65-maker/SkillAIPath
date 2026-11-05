# Databricks notebook source
# DBTITLE 1,5-Minute Project Demo Script
# MAGIC %md
# MAGIC # BharatMartDBX Project Demo - 5 Minutes
# MAGIC
# MAGIC ## Opening (30 seconds)
# MAGIC
# MAGIC **Slide 1: Title**
# MAGIC "Hi, I'm [Name]. I built BharatMartDBX - a complete data warehouse for e-commerce analytics using Databricks, Delta Lake, and PySpark."
# MAGIC
# MAGIC ## Architecture Overview (60 seconds)
# MAGIC
# MAGIC **Slide 2: Medallion Architecture**
# MAGIC
# MAGIC "The pipeline follows 3 layers:
# MAGIC 1. **Bronze:** Raw CSV files from operational systems (orders, customers, inventory)
# MAGIC 2. **Silver:** Clean dimensions (SCD Type 2 for customer history) and fact tables (500K sales transactions)
# MAGIC 3. **Gold:** Pre-aggregated marts for dashboards (customer 360, product performance)
# MAGIC
# MAGIC All data stored in Delta Lake format for ACID transactions and time travel."

# COMMAND ----------

# DBTITLE 1,Live Demo - RFM Analysis (90 seconds)
# MAGIC %md
# MAGIC ## Demo Script: Customer Segmentation
# MAGIC
# MAGIC **Open Notebook:** /1_Execution_Core/Canvas/4_Orchestration/06_gold_part1_orchestrator.py (RFM analysis mart)
# MAGIC
# MAGIC **Narration:**
# MAGIC
# MAGIC "Let me show you RFM analysis - segmenting customers by Recency, Frequency, and Monetary value.
# MAGIC
# MAGIC [Run Cell 3: Calculate RFM]
# MAGIC This query analyzes 500K transactions to compute:
# MAGIC * Recency: Days since last purchase
# MAGIC * Frequency: Number of orders
# MAGIC * Monetary: Total spent
# MAGIC
# MAGIC [Run Cell 4: Assign Scores]
# MAGIC Using window functions, I score each dimension 1-5, then create segments:
# MAGIC * Champions: High R, F, M (VIP customers)
# MAGIC * At Risk: High past spend, but haven't purchased recently
# MAGIC * Hibernating: Low engagement
# MAGIC
# MAGIC [Show Results]
# MAGIC Out of 4,234 customers:
# MAGIC * 12% Champions (₹5+ Lakhs lifetime value)
# MAGIC * 18% At Risk (need urgent attention)
# MAGIC * 25% Hibernating (re-engagement candidates)
# MAGIC
# MAGIC Marketing can now target each segment with appropriate campaigns."

# COMMAND ----------

# DBTITLE 1,Results & Impact (60 seconds)
# MAGIC %md
# MAGIC ## Demo Script: Business Impact
# MAGIC
# MAGIC **Slide 3: Key Outcomes**
# MAGIC
# MAGIC "This project demonstrates:
# MAGIC
# MAGIC 1. **Architecture:** Medallion pattern (Bronze/Silver/Gold), 11 dimensions, 3 fact tables, 4 gold marts
# MAGIC
# MAGIC 2. **Performance:** Optimized fact_sales with ZORDER - query time dropped from 45 seconds to 5 seconds (9x improvement)
# MAGIC
# MAGIC 3. **Data Quality:** Built orphan detection, implemented 20+ validation queries, documented 3 debugging case studies
# MAGIC
# MAGIC 4. **Production Skills:** Incremental loading with watermarks, SCD Type 2, workflow orchestration, cost optimization
# MAGIC
# MAGIC 5. **Tools:** Databricks, PySpark, Delta Lake, SQL, dbt
# MAGIC
# MAGIC All code is on my GitHub with detailed README and architecture diagrams. Happy to answer any questions!"