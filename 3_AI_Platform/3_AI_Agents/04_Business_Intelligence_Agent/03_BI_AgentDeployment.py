# Databricks notebook source
# DBTITLE 1,Business Intelligence Genie Space Setup
# MAGIC %md
# MAGIC # Business Intelligence - Genie Space Setup
# MAGIC
# MAGIC ## Purpose
# MAGIC Creates or updates the **Business Intelligence Q&A** Genie Space with:
# MAGIC - 9 data source tables (4 Gold/Mart + 4 ML predictions + 1 Silver enriched sales)
# MAGIC - 11 UC functions as trusted SQL functions
# MAGIC - Business analytics instructions covering 6 BI domains and example SQL queries
# MAGIC
# MAGIC ## Architecture
# MAGIC ```
# MAGIC Gold/Mart + ML + Silver Tables → Genie Space → User asks NL questions → Genie returns answers
# MAGIC ```
# MAGIC
# MAGIC **No Agent Bricks dependency.** The Genie Space is the final user-facing layer.
# MAGIC
# MAGIC **Idempotent:** First run creates the space. Subsequent runs update it.

# COMMAND ----------

# DBTITLE 1,Configuration & Setup
# Business Intelligence - Genie Space Setup
# Creates or updates ONE Genie Space for BI Q&A

from databricks.sdk import WorkspaceClient
import json
import random
import datetime

# ============================================================
# CONFIGURATION
# ============================================================

WAREHOUSE_ID = "dbf439d4924bae6d"  # Serverless Starter Warehouse
SPACE_TITLE = "Business Intelligence Q&A"
SPACE_DESCRIPTION = "Natural language Q&A over business performance, customer analytics, ML predictions, and product insights"

CATALOG = "sap_dev"
SCHEMA_GOLD = "gold"

# Data source tables (4 Mart + 4 ML predictions)
MART_TABLES = [
    f"{CATALOG}.{SCHEMA_GOLD}.mart_customer_360",
    f"{CATALOG}.{SCHEMA_GOLD}.mart_rfm_analysis",
    f"{CATALOG}.{SCHEMA_GOLD}.mart_product_performance",
    f"{CATALOG}.{SCHEMA_GOLD}.mart_inventory_health",
]
ML_TABLES = [
    f"{CATALOG}.{SCHEMA_GOLD}.churn_predictions",
    f"{CATALOG}.{SCHEMA_GOLD}.demand_forecast",
    f"{CATALOG}.{SCHEMA_GOLD}.next_purchase_predictions",
    f"{CATALOG}.{SCHEMA_GOLD}.product_recommendations",
]
SILVER_TABLES = [
    f"{CATALOG}.silver.int_sales_enriched",
]
ALL_TABLES = MART_TABLES + ML_TABLES + SILVER_TABLES

# UC Functions to attach as trusted SQL functions
UC_FUNCTIONS = [
    f"{CATALOG}.{SCHEMA_GOLD}.get_rfm_segment_summary",
    f"{CATALOG}.{SCHEMA_GOLD}.get_churn_risk_customers",
    f"{CATALOG}.{SCHEMA_GOLD}.get_top_products",
    f"{CATALOG}.{SCHEMA_GOLD}.get_demand_forecast",
    f"{CATALOG}.{SCHEMA_GOLD}.get_customer_360_view",
    f"{CATALOG}.{SCHEMA_GOLD}.get_revenue_summary",
    f"{CATALOG}.{SCHEMA_GOLD}.get_product_recommendations_for_customer",
    f"{CATALOG}.{SCHEMA_GOLD}.get_inventory_health_summary",
    f"{CATALOG}.{SCHEMA_GOLD}.get_sales_profitability_summary",
    f"{CATALOG}.{SCHEMA_GOLD}.get_category_performance",
    f"{CATALOG}.{SCHEMA_GOLD}.get_next_purchase_predictions",
]

print("=" * 80)
print("BUSINESS INTELLIGENCE GENIE SPACE SETUP")
print("=" * 80)
print(f"  Warehouse ID: {WAREHOUSE_ID}")
print(f"  Space Title: {SPACE_TITLE}")
print(f"  Tables: {len(ALL_TABLES)} (4 Mart + 4 ML + 1 Silver)")
print(f"  UC Functions: {len(UC_FUNCTIONS)}")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Build Genie Space Configuration
# ============================================================
# UTILITY: Generate Genie-compatible IDs
# ============================================================

def generate_genie_id():
    """Generate a 32-char lowercase hex ID for Genie config elements."""
    t = int((datetime.datetime.now() - datetime.datetime(1582, 10, 15)).total_seconds() * 1e7)
    part1 = ((t & 0xFFFFFFFFFFFF0000) | (1 << 12) | ((t & 0xFFFF) >> 4))
    part2 = random.getrandbits(62) | 0x8000000000000000
    return f"{part1:016x}{part2:016x}"

# ============================================================
# BUILD SERIALIZED SPACE CONFIG
# ============================================================

table_descriptions = {
    f"{CATALOG}.{SCHEMA_GOLD}.mart_customer_360": "Unified customer profile with purchase history, spend, order value, and activity status. Contains CustomerKey, CustomerID, CustomerName, Email, TotalPurchases, TotalSpend, AvgOrderValue, FirstPurchaseDate, LastPurchaseDate, DaysSinceLastPurchase, CustomerSegment, ActivityStatus.",
    f"{CATALOG}.{SCHEMA_GOLD}.mart_rfm_analysis": "RFM (Recency, Frequency, Monetary) segmentation of customers. Contains CustomerSK, CustomerName, Recency, Frequency, Monetary_Lakhs, R_Score, F_Score, M_Score, RFM_Score, Segment (9 segments: Champions, Loyal Customers, Potential Loyalist, Recent Customers, Promising, About to Sleep, Needs Attention, At Risk, Cant Lose Them).",
    f"{CATALOG}.{SCHEMA_GOLD}.mart_product_performance": "Product performance summary with total orders, quantity sold, revenue, and performance category. Contains ProductSK, ProductID, ProductName, ProductCategory, TotalOrders, TotalQuantitySold, TotalRevenue, AvgSaleValue, AvgStockLevel, PerformanceCategory (Best Seller, Popular).",
    f"{CATALOG}.{SCHEMA_GOLD}.mart_inventory_health": "Inventory health metrics by product and warehouse. Contains ProductSK, ProductName, WarehouseSK, WarehouseName, CurrentStock, CurrentStockValue, StockStatus, DaysInStock, QuantitySoldLast90Days, OrderCountLast90Days, TurnoverRatio, HealthStatus.",
    f"{CATALOG}.{SCHEMA_GOLD}.churn_predictions": "Customer churn predictions with churn probability and risk segment. Contains customer_id, recency_days, total_purchases, lifetime_value_lakhs, churn_probability, is_churned_predicted, prediction_date, model_version, churn_threshold_days, risk_segment (Low Risk, High Risk). Filter by prediction_date = CURRENT_DATE() for latest.",
    f"{CATALOG}.{SCHEMA_GOLD}.demand_forecast": "Product demand forecasts with confidence bounds. Contains product_id, product_name, category, forecast_date, predicted_demand, lower_bound, upper_bound, prediction_date, model_type, forecast_horizon_days. Filter by prediction_date = CURRENT_DATE() for latest.",
    f"{CATALOG}.{SCHEMA_GOLD}.next_purchase_predictions": "Next purchase predictions for customers. Contains customer_id, recommended_product_id, recommended_product_name, recommendation_score, recommendation_rank, prediction_date. Filter by prediction_date = CURRENT_DATE() for latest.",
    f"{CATALOG}.{SCHEMA_GOLD}.product_recommendations": "Product recommendations for customers. Contains customer_id, recommended_product_id, recommended_product_name, recommendation_score, recommendation_rank, prediction_date. Filter by prediction_date = CURRENT_DATE() for latest.",
    f"{CATALOG}.silver.int_sales_enriched": "Enriched sales transactions with pre-computed margins and customer context. Contains OrderID, CustomerName, customer_segment, activity_status, customer_lifetime_value, ProductName, product_avg_margin, product_popularity, avg_stock_level, WareHouseName, RetailerName, OrderDate, ProductQuantity, SellingPrice, CostPrice, FinalPrice, TotalAmount, transaction_margin, margin_pct. Use this table for cost, profit, and margin analysis.",
}

tables_config = sorted(
    [{"identifier": tbl, "description": [table_descriptions.get(tbl, "")]} for tbl in ALL_TABLES],
    key=lambda x: x["identifier"],
)

bi_instructions = (
    "You are a Business Intelligence assistant for SkillAIPath's retail analytics platform.\n\n"
    "## Your Role\n"
    "Answer questions about business performance, customer analytics, product insights, inventory health, financial KPIs, and ML-driven predictions.\n\n"
    "## Capabilities\n"
    "You cover six BI domains:\n"
    "1. **Customer Intelligence**: RFM segmentation, customer 360, churn risk, customer behavior\n"
    "2. **Inventory Intelligence**: inventory health, stock status, inventory days, turnover, warehouse inventory\n"
    "3. **Product Intelligence**: product performance, top products, category performance, sales metrics, margins\n"
    "4. **Business KPIs**: revenue, cost, profit, margin, executive KPIs\n"
    "5. **ML Insights**: churn predictions, demand forecasts, next purchase predictions, product recommendations\n"
    "6. **Supply Chain BI**: high-level BI questions only (for detailed supply chain operations, use the separate Supply Chain Intelligence Genie Space)\n\n"
    "## Data Sources (9 tables)\n"
    "- mart_customer_360: Customer profiles with spend, orders, segments\n"
    "- mart_rfm_analysis: RFM segmentation (9 segments from Champions to Cant Lose Them)\n"
    "- mart_product_performance: Product sales performance (78 products)\n"
    "- mart_inventory_health: Inventory health by product and warehouse (1,560 records, 20 warehouses)\n"
    "- churn_predictions: Customer churn probability and risk (6,373 customers, High/Low Risk)\n"
    "- demand_forecast: Product demand forecasts with confidence bounds (150 records)\n"
    "- next_purchase_predictions: Predicted next purchase for each customer\n"
    "- product_recommendations: Top product recommendations per customer\n"
    "- int_sales_enriched: Enriched sales with cost, profit, margin, customer segment, warehouse (7,000 transactions)\n\n"
    "## UC Functions (11 trusted SQL functions)\n"
    "- get_rfm_segment_summary(): RFM segment distribution\n"
    "- get_churn_risk_customers(risk_segment): Churn predictions by risk level\n"
    "- get_top_products(metric, limit_count): Top products by revenue/quantity/orders\n"
    "- get_demand_forecast(product_name): Demand forecast lookup\n"
    "- get_customer_360_view(customer_name): 360-degree customer profile with RFM + churn\n"
    "- get_revenue_summary(): Revenue and business KPI summary\n"
    "- get_product_recommendations_for_customer(customer_id): Top product recommendations\n"
    "- get_inventory_health_summary(): Inventory health by warehouse (stock status, health, turnover)\n"
    "- get_sales_profitability_summary(category): Revenue, cost, profit, margin (overall or by category)\n"
    "- get_category_performance(): Category-level revenue, cost, profit, margin comparison\n"
    "- get_next_purchase_predictions(customer_id): Next purchase predictions for a customer\n\n"
    "## Guidelines\n"
    "- Prefer the Gold mart tables for business answers.\n"
    "- Use the trusted UC functions when their specialized purpose matches the question.\n"
    "- Use int_sales_enriched for cost, profit, and margin analysis (it has pre-computed transaction_margin and margin_pct).\n"
    "- Always filter prediction tables by prediction_date = CURRENT_DATE() for latest data.\n"
    "- Do not invent values. Do not fabricate customers, products, predictions, or metrics.\n"
    "- Use actual available data only.\n"
    "- When there is insufficient data, say so clearly.\n"
    "- Explain important results in simple business language.\n"
    "- For customer lookups, use CustomerName for mart_customer_360 and CustomerSK for mart_rfm_analysis.\n"
    "- Revenue is in the TotalSpend column of mart_customer_360 and TotalRevenue of mart_product_performance.\n"
    "- Monetary_Lakhs in mart_rfm_analysis is in lakhs (1 lakh = 100,000).\n"
    "- Cost and profit data come from fact_sales (CostPrice, sellingPrice) joined with dim_product_category.\n"
    "- Profit = (sellingPrice - CostPrice) * ProductQuantity. Margin % = Profit / Revenue * 100.\n"
    "- For detailed supply chain operations (stockouts, reorder, dead stock), direct users to the Supply Chain Intelligence Genie Space.\n"
)

example_questions = [
    {"id": generate_genie_id(), "question": ["What are our RFM customer segments?"], "sql": ["SELECT Segment, COUNT(*) as customer_count, AVG(Recency) as avg_recency, ", "AVG(Frequency) as avg_frequency, AVG(Monetary_Lakhs) as avg_monetary ", "FROM sap_dev.gold.mart_rfm_analysis ", "GROUP BY Segment ", "ORDER BY customer_count DESC"]},
    {"id": generate_genie_id(), "question": ["Show me the top 10 products by revenue."], "sql": ["SELECT ProductName, ProductCategory, TotalOrders, TotalQuantitySold, ", "TotalRevenue, AvgSaleValue, PerformanceCategory ", "FROM sap_dev.gold.mart_product_performance ", "ORDER BY TotalRevenue DESC ", "LIMIT 10"]},
    {"id": generate_genie_id(), "question": ["Which customers are at high risk of churning?"], "sql": ["SELECT c.CustomerName, c.TotalPurchases, c.TotalSpend, ch.churn_probability, ch.risk_segment ", "FROM sap_dev.gold.churn_predictions ch ", "INNER JOIN sap_dev.gold.mart_customer_360 c ON ch.customer_id = c.CustomerKey ", "WHERE ch.prediction_date = CURRENT_DATE() AND ch.risk_segment = 'High Risk' ", "ORDER BY ch.churn_probability DESC ", "LIMIT 20"]},
    {"id": generate_genie_id(), "question": ["What is the overall revenue summary?"], "sql": ["SELECT COUNT(*) as total_customers, SUM(TotalSpend) as total_revenue, ", "AVG(TotalSpend) as avg_customer_spend, AVG(AvgOrderValue) as avg_order_value ", "FROM sap_dev.gold.mart_customer_360"]},
    {"id": generate_genie_id(), "question": ["Show me the demand forecast for all products."], "sql": ["SELECT product_name, category, forecast_date, predicted_demand, ", "lower_bound, upper_bound, model_type ", "FROM sap_dev.gold.demand_forecast ", "WHERE prediction_date = CURRENT_DATE() ", "ORDER BY predicted_demand DESC ", "LIMIT 20"]},
    {"id": generate_genie_id(), "question": ["What product recommendations do we have for customer 1?"], "sql": ["SELECT recommended_product_name, recommendation_score, recommendation_rank ", "FROM sap_dev.gold.product_recommendations ", "WHERE prediction_date = CURRENT_DATE() AND customer_id = 1 ", "ORDER BY recommendation_rank ASC ", "LIMIT 10"]},
    {"id": generate_genie_id(), "question": ["What is the overall inventory health?"], "sql": ["SELECT warehouse_name, total_products, in_stock_count, low_stock_count, out_of_stock_count, ", "healthy_count, understock_count, avg_current_stock, avg_days_in_stock, avg_turnover_ratio, total_stock_value ", "FROM sap_dev.gold.get_inventory_health_summary() ", "WHERE warehouse_name = 'ALL'"]},
    {"id": generate_genie_id(), "question": ["Which warehouses have the weakest inventory health?"], "sql": ["SELECT warehouse_name, total_products, understock_count, low_stock_count, out_of_stock_count ", "FROM sap_dev.gold.get_inventory_health_summary() ", "WHERE warehouse_name != 'ALL' ", "ORDER BY understock_count DESC, out_of_stock_count DESC"]},
    {"id": generate_genie_id(), "question": ["What are revenue, cost, profit and margin?"], "sql": ["SELECT category, total_orders, total_quantity_sold, total_revenue, total_cost, gross_profit, profit_margin_pct, total_discount ", "FROM sap_dev.gold.get_sales_profitability_summary() ", "ORDER BY total_revenue DESC"]},
    {"id": generate_genie_id(), "question": ["Which product category has the highest revenue?"], "sql": ["SELECT category, total_orders, total_revenue, gross_profit, profit_margin_pct ", "FROM sap_dev.gold.get_category_performance() ", "ORDER BY total_revenue DESC ", "LIMIT 5"]},
    {"id": generate_genie_id(), "question": ["Which category has the highest profit margin?"], "sql": ["SELECT category, total_revenue, gross_profit, profit_margin_pct ", "FROM sap_dev.gold.get_category_performance() ", "ORDER BY profit_margin_pct DESC ", "LIMIT 5"]},
    {"id": generate_genie_id(), "question": ["What is the predicted next purchase for customer 1?"], "sql": ["SELECT customer_id, recommended_product_name, recommendation_score, recommendation_rank ", "FROM sap_dev.gold.get_next_purchase_predictions(1) ", "ORDER BY recommendation_rank ASC"]},
    {"id": generate_genie_id(), "question": ["Show me category-wise profitability."], "sql": ["SELECT category, total_orders, total_quantity_sold, total_revenue, total_cost, gross_profit, profit_margin_pct ", "FROM sap_dev.gold.get_category_performance() ", "ORDER BY total_revenue DESC"]},
    {"id": generate_genie_id(), "question": ["What are the executive KPIs?"], "sql": ["SELECT total_customers, active_customers, total_revenue, avg_customer_spend, avg_order_value, ", "total_products, total_orders, total_quantity_sold, churned_customers, high_risk_customers ", "FROM sap_dev.gold.get_revenue_summary()"]},
    {"id": generate_genie_id(), "question": ["Give me a 360 view of customer Aachal."], "sql": ["SELECT customer_name, email, total_purchases, total_spend, avg_order_value, customer_segment, ", "activity_status, rfm_segment, churn_probability, churn_risk_segment ", "FROM sap_dev.gold.get_customer_360_view('Aachal')"]},
]

print("✅ Genie Space configuration built")
print(f"  Tables: {len(tables_config)}")
print(f"  UC Functions: {len(UC_FUNCTIONS)}")
print(f"  Example questions: {len(example_questions)}")
print(f"  Instructions: {len(bi_instructions)} chars")

# Build sql_functions config for Genie
sql_functions_config = sorted(
    [{"id": generate_genie_id(), "identifier": f} for f in UC_FUNCTIONS],
    key=lambda x: (x["id"], x["identifier"]),
)

print(f"  SQL Functions config: {len(sql_functions_config)}")

# COMMAND ----------

# DBTITLE 1,Create or Update Genie Space
# ============================================================
# CREATE OR UPDATE GENIE SPACE
# ============================================================

w = WorkspaceClient()

# Check if a BI Genie Space already exists
existing_space_id = None
try:
    response = w.api_client.do(method='GET', path='/api/2.0/genie/spaces')
    spaces = response.get('spaces', [])
    for s in spaces:
        if s.get('title') == SPACE_TITLE:
            existing_space_id = s.get('space_id')
            print(f"Found existing Genie Space: {SPACE_TITLE} (ID: {existing_space_id})")
            break
except Exception as e:
    print(f"Warning listing Genie Spaces: {str(e)[:100]}")

# Build serialized_space configuration
space_config = {
    "version": 2,
    "data_sources": {
        "tables": tables_config
    },
    "instructions": {
        "text_instructions": [
            {
                "id": generate_genie_id(),
                "content": [bi_instructions]
            }
        ],
        "sql_functions": sql_functions_config,
        "example_question_sqls": example_questions,
    },
}

if existing_space_id:
    # UPDATE existing space
    print(f"\nUpdating existing Genie Space (ID: {existing_space_id})...")
    payload = {
        "warehouse_id": WAREHOUSE_ID,
        "serialized_space": json.dumps(space_config)
    }
    w.api_client.do(
        method='PATCH',
        path=f'/api/2.0/genie/spaces/{existing_space_id}',
        body=payload
    )
    space_id = existing_space_id
    print(f"✅ Genie Space updated: {space_id}")
else:
    # CREATE new space
    print(f"\nCreating new Genie Space: {SPACE_TITLE}...")
    payload = {
        "title": SPACE_TITLE,
        "description": SPACE_DESCRIPTION,
        "warehouse_id": WAREHOUSE_ID,
        "serialized_space": json.dumps(space_config)
    }
    response = w.api_client.do(
        method='POST',
        path='/api/2.0/genie/spaces',
        body=payload
    )
    space_id = response.get('space_id')
    print(f"✅ Genie Space created: {space_id}")

print(f"\n  Title: {SPACE_TITLE}")
print(f"  Space ID: {space_id}")
print(f"  Tables: {len(ALL_TABLES)}")
print(f"  UC Functions: {len(UC_FUNCTIONS)}")
print(f"  Example Questions: {len(example_questions)}")

# COMMAND ----------

# DBTITLE 1,Verify Genie Space
# ============================================================
# VERIFY GENIE SPACE
# ============================================================

print("=" * 80)
print("GENIE SPACE VERIFICATION")
print("=" * 80)

# List all Genie Spaces
try:
    response = w.api_client.do(method='GET', path='/api/2.0/genie/spaces')
    spaces = response.get('spaces', [])
    print(f"\nFound {len(spaces)} Genie Space(s):")
    for s in spaces:
        print(f"  - {s.get('title')} (ID: {s.get('space_id')})")
except Exception as e:
    print(f"Error listing spaces: {str(e)[:100]}")

# Verify the BI Space configuration
if space_id:
    try:
        detail = w.api_client.do(
            method='GET',
            path=f'/api/2.0/genie/spaces/{space_id}'
        )
        serialized = json.loads(detail.get('serialized_space', '{}'))
        
        # Check tables
        if 'data_sources' in serialized and 'tables' in serialized['data_sources']:
            configured_tables = sorted([t['identifier'] for t in serialized['data_sources']['tables']])
            expected_tables = sorted(ALL_TABLES)
            print(f"\nTables configured: {len(configured_tables)}")
            for t in configured_tables:
                print(f"  ✅ {t}")
            if configured_tables == expected_tables:
                print("  ✅ All expected tables present")
            else:
                print("  ⚠️  Table mismatch detected")
        
        # Check instructions
        if 'instructions' in serialized and 'text_instructions' in serialized['instructions']:
            instructions = serialized['instructions']['text_instructions']
            if instructions:
                content = instructions[0].get('content', [''])[0]
                print(f"\nInstructions: {len(content)} chars")
                print(f"  Preview: {content[:150]}...")
        
        # Check example questions
        if 'example_questions' in serialized:
            eq_count = len(serialized['example_questions'])
            print(f"\nExample questions: {eq_count}")
        
        print(f"\n✅ Genie Space '{SPACE_TITLE}' verified successfully")
        print(f"   Space ID: {space_id}")
        print(f"   URL: https://your-workspace.cloud.databricks.com/genie/{space_id}")
    except Exception as e:
        print(f"❌ Error verifying space: {str(e)[:200]}")
else:
    print("❌ No space_id available for verification")

print("=" * 80)

# COMMAND ----------

# DBTITLE 1,✅ BI Agent Deployment Complete
# MAGIC %md
# MAGIC # ✅ Business Intelligence Agent - Deployment Complete
# MAGIC
# MAGIC ## Genie Space Created
# MAGIC * **Title:** Business Intelligence Q&A
# MAGIC * **Description:** Natural language Q&A over business performance, customer analytics, ML predictions, and product insights
# MAGIC
# MAGIC ## Data Sources (8 tables)
# MAGIC | # | Table | Type | Rows |
# MAGIC |---|-------|------|------|
# MAGIC | 1 | mart_customer_360 | Mart | 6,373 |
# MAGIC | 2 | mart_rfm_analysis | Mart | 6,373 |
# MAGIC | 3 | mart_product_performance | Mart | 78 |
# MAGIC | 4 | mart_inventory_health | Mart | 1,560 |
# MAGIC | 5 | churn_predictions | ML | 6,373 |
# MAGIC | 6 | demand_forecast | ML | 150 |
# MAGIC | 7 | next_purchase_predictions | ML | 63,730 |
# MAGIC | 8 | product_recommendations | ML | 63,730 |
# MAGIC
# MAGIC ## UC Functions (7 functions)
# MAGIC | # | Function | Purpose |
# MAGIC |---|----------|---------|
# MAGIC | 1 | get_rfm_segment_summary() | RFM segment distribution |
# MAGIC | 2 | get_churn_risk_customers(risk_segment) | Churn predictions by risk |
# MAGIC | 3 | get_top_products(metric, limit_count) | Top products by metric |
# MAGIC | 4 | get_demand_forecast(product_name) | Demand forecast lookup |
# MAGIC | 5 | get_customer_360_view(customer_name) | Customer 360 profile |
# MAGIC | 6 | get_revenue_summary() | Revenue KPI summary |
# MAGIC | 7 | get_product_recommendations_for_customer(customer_id) | Product recommendations |
# MAGIC
# MAGIC ## Architecture
# MAGIC ```
# MAGIC Gold/Mart Tables + ML Predictions → UC Functions → Genie Space → User Q&A
# MAGIC ```
# MAGIC
# MAGIC **No Agent Bricks. No apps. Pure Genie Space + SQL functions.**
# MAGIC
# MAGIC **Idempotent:** Re-running this notebook updates the Genie Space without creating duplicates.

# COMMAND ----------

