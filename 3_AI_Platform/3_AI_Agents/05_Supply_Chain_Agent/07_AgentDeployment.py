# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Supply Chain Genie Space Setup
# MAGIC %md
# MAGIC # Supply Chain Intelligence - Genie Space Setup
# MAGIC
# MAGIC ## Purpose
# MAGIC Creates or updates the **Supply Chain Intelligence** Genie Space with:
# MAGIC - 5 data source tables (Gold predictions/alerts/metrics + Silver dim_product/dim_warehouse)
# MAGIC - 7 UC functions as trusted SQL functions
# MAGIC - Supply chain instructions and example SQL queries
# MAGIC - Join specifications for product/warehouse lookups
# MAGIC
# MAGIC ## Architecture
# MAGIC ```
# MAGIC Gold Tables → Genie Space → User asks NL questions → Genie returns answers
# MAGIC ```
# MAGIC
# MAGIC **No Agent Bricks dependency.** The Genie Space is the final user-facing layer.
# MAGIC
# MAGIC **Idempotent:** First run creates the space. Subsequent runs update it.

# COMMAND ----------

# DBTITLE 1,Genie Space Creation and Verification
# Databricks notebook source
# Supply Chain Intelligence - Genie Space Setup
# Creates or updates ONE Genie Space for supply chain Q&A

from databricks.sdk import WorkspaceClient
import json
import random
import datetime

# ============================================================
# CONFIGURATION
# ============================================================

WAREHOUSE_ID = "90e237c7e33b9e67"  # Serverless Starter Warehouse
SPACE_TITLE = "Supply Chain Intelligence"
SPACE_DESCRIPTION = "Natural language Q&A over supply chain predictions, alerts, and inventory metrics"

CATALOG = "sap_dev"
SCHEMA_GOLD = "gold"
SCHEMA_SILVER = "silver"

# Data source tables
GOLD_TABLES = [
    f"{CATALOG}.{SCHEMA_GOLD}.supply_chain_predictions",
    f"{CATALOG}.{SCHEMA_GOLD}.supply_chain_alerts",
    f"{CATALOG}.{SCHEMA_GOLD}.supply_chain_metrics",
]
SILVER_TABLES = [
    f"{CATALOG}.{SCHEMA_SILVER}.dim_product",
    f"{CATALOG}.{SCHEMA_SILVER}.dim_warehouse",
]
ALL_TABLES = GOLD_TABLES + SILVER_TABLES

# UC Functions to attach as trusted SQL functions
UC_FUNCTIONS = [
    f"{CATALOG}.{SCHEMA_GOLD}.get_critical_alerts",
    f"{CATALOG}.{SCHEMA_GOLD}.get_stockout_predictions",
    f"{CATALOG}.{SCHEMA_GOLD}.get_dead_stock",
    f"{CATALOG}.{SCHEMA_GOLD}.get_inventory_health",
    f"{CATALOG}.{SCHEMA_GOLD}.search_product_inventory",
    f"{CATALOG}.{SCHEMA_GOLD}.get_reorder_recommendations",
    f"{CATALOG}.{SCHEMA_GOLD}.get_warehouse_summary",
]

print("=" * 80)
print("SUPPLY CHAIN GENIE SPACE SETUP")
print("=" * 80)
print(f"  Warehouse ID: {WAREHOUSE_ID}")
print(f"  Space Title: {SPACE_TITLE}")
print(f"  Tables: {len(ALL_TABLES)}")
print(f"  UC Functions: {len(UC_FUNCTIONS)}")
print("=" * 80)

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
# BUILD SERIALIZED SPACE CONFIG (with correct 'config' wrapper)
# ============================================================

table_descriptions = {
    f"{CATALOG}.{SCHEMA_GOLD}.supply_chain_predictions": "Daily stockout predictions for all product-warehouse combinations. Includes current stock, sales velocity, days until stockout, risk levels, safety stock, reorder points, and recommended order quantities. Use prediction_date = CURRENT_DATE() for latest data.",
    f"{CATALOG}.{SCHEMA_GOLD}.supply_chain_alerts": "Actionable supply chain alerts including stockouts, excess inventory, and reorder needs. Contains denormalized product_name and warehouse_name. Use alert_date = CURRENT_DATE() for latest alerts.",
    f"{CATALOG}.{SCHEMA_GOLD}.supply_chain_metrics": "Daily KPI summary including total predictions, alert counts, inventory health metrics, and pipeline performance. Use metric_date = CURRENT_DATE() for latest metrics.",
    f"{CATALOG}.{SCHEMA_SILVER}.dim_product": "Product dimension table. Join with supply_chain_predictions on product_sk = ProductSK to get ProductName.",
    f"{CATALOG}.{SCHEMA_SILVER}.dim_warehouse": "Warehouse dimension table. Join with supply_chain_predictions on warehouse_sk = WareHouseSK to get WareHouseName and WareHouseCity.",
}

tables_config = sorted(
    [{"identifier": tbl, "description": [table_descriptions.get(tbl, "")]} for tbl in ALL_TABLES],
    key=lambda x: x["identifier"],
)

supply_chain_instructions = (
    "You are a Supply Chain Intelligence assistant for inventory risk, stockout prediction, "
    "dead stock, warehouse health, reorder recommendations, and supply-chain KPIs.\n\n"
    "## Data Sources\n"
    "You have access to supply_chain_predictions (daily stockout predictions), supply_chain_alerts (actionable alerts), "
    "supply_chain_metrics (KPI summary), dim_product (product names), and dim_warehouse (warehouse names).\n\n"
    "## Guidelines\n"
    "- Prefer the Gold supply chain tables for business answers.\n"
    "- Use the trusted UC functions when their specialized purpose matches the user's question.\n"
    "- For product or warehouse names, join supply_chain_predictions to dim_product (product_sk = ProductSK) or dim_warehouse (warehouse_sk = WareHouseSK).\n"
    "- supply_chain_alerts already has product_name and warehouse_name denormalized.\n"
    "- Always filter predictions by prediction_date = CURRENT_DATE() and alerts by alert_date = CURRENT_DATE().\n"
    "- Do not invent values. Do not fabricate products, warehouses, alerts, predictions, or recommendations.\n"
    "- Use actual available data only.\n"
    "- Clearly distinguish predictions from historical facts.\n"
    "- When there is insufficient data, say so clearly.\n"
    "- Prefer exact values from the data over assumptions.\n"
    "- Explain important results in simple business language.\n"
)

example_questions = [
    {"id": generate_genie_id(), "question": ["Show me the critical supply chain alerts."], "sql": ["SELECT alert_type, priority, product_name, warehouse_name, current_stock, ", "days_until_stockout, recommended_order_qty, alert_message, recommended_action ", "FROM sap_dev.gold.supply_chain_alerts ", "WHERE alert_date = CURRENT_DATE() AND priority = 'Critical' AND is_active = TRUE ", "ORDER BY days_until_stockout ASC"]},
    {"id": generate_genie_id(), "question": ["Which products are at risk of stockout?"], "sql": ["SELECT pr.ProductName, w.WareHouseName, p.current_stock, p.days_until_stockout, ", "p.stockout_risk_level, p.recommended_order_qty ", "FROM sap_dev.gold.supply_chain_predictions p ", "INNER JOIN sap_dev.silver.dim_product pr ON p.product_sk = pr.ProductSK ", "INNER JOIN sap_dev.silver.dim_warehouse w ON p.warehouse_sk = w.WareHouseSK ", "WHERE p.prediction_date = CURRENT_DATE() ", "AND p.stockout_risk_level IN ('Critical', 'High', 'Medium') ", "ORDER BY p.days_until_stockout ASC"]},
    {"id": generate_genie_id(), "question": ["Which products are dead stock?"], "sql": ["SELECT pr.ProductName, w.WareHouseName, p.current_stock, p.stock_value, ", "p.days_of_inventory, p.is_dead_stock, p.is_excess_inventory ", "FROM sap_dev.gold.supply_chain_predictions p ", "INNER JOIN sap_dev.silver.dim_product pr ON p.product_sk = pr.ProductSK ", "INNER JOIN sap_dev.silver.dim_warehouse w ON p.warehouse_sk = w.WareHouseSK ", "WHERE p.prediction_date = CURRENT_DATE() ", "AND (p.is_dead_stock = TRUE OR p.is_excess_inventory = TRUE) ", "ORDER BY p.stock_value DESC"]},
    {"id": generate_genie_id(), "question": ["What is the current inventory health?"], "sql": ["SELECT metric_date, total_predictions, total_alerts, critical_alerts, high_alerts, ", "products_at_risk, products_critical_stockout, products_excess_inventory, ", "products_dead_stock, products_healthy_stock, total_inventory_value, ", "avg_days_of_inventory, total_reorder_recommendations ", "FROM sap_dev.gold.supply_chain_metrics ", "WHERE metric_date = CURRENT_DATE()"]},
    {"id": generate_genie_id(), "question": ["Give me reorder recommendations."], "sql": ["SELECT pr.ProductName, w.WareHouseName, p.current_stock, p.days_until_stockout, ", "p.recommended_order_qty, p.safety_stock, p.reorder_point, p.order_cost_estimate, ", "p.stockout_risk_level, p.recommended_reorder_date ", "FROM sap_dev.gold.supply_chain_predictions p ", "INNER JOIN sap_dev.silver.dim_product pr ON p.product_sk = pr.ProductSK ", "INNER JOIN sap_dev.silver.dim_warehouse w ON p.warehouse_sk = w.WareHouseSK ", "WHERE p.prediction_date = CURRENT_DATE() ", "AND p.recommended_order_qty > 0 ", "ORDER BY p.days_until_stockout ASC"]},
    {"id": generate_genie_id(), "question": ["Show inventory for product Aashirvaad Atta 10kg."], "sql": ["SELECT pr.ProductName, w.WareHouseName, p.current_stock, p.stock_value, ", "p.stock_status, p.days_until_stockout, p.days_of_inventory, ", "p.stockout_risk_level, p.recommended_order_qty ", "FROM sap_dev.gold.supply_chain_predictions p ", "INNER JOIN sap_dev.silver.dim_product pr ON p.product_sk = pr.ProductSK ", "INNER JOIN sap_dev.silver.dim_warehouse w ON p.warehouse_sk = w.WareHouseSK ", "WHERE p.prediction_date = CURRENT_DATE() ", "AND pr.ProductName LIKE '%Aashirvaad%' ", "ORDER BY w.WareHouseName"]},
    {"id": generate_genie_id(), "question": ["Which warehouses have the highest supply chain risk?"], "sql": ["SELECT w.WareHouseName, COUNT(*) as total_products, ", "SUM(CASE WHEN p.stockout_risk_level IN ('Critical','High','Medium') THEN 1 ELSE 0 END) as products_at_risk, ", "SUM(CASE WHEN p.is_dead_stock = TRUE THEN 1 ELSE 0 END) as dead_stock_products, ", "ROUND(SUM(p.stock_value), 2) as total_inventory_value ", "FROM sap_dev.gold.supply_chain_predictions p ", "INNER JOIN sap_dev.silver.dim_warehouse w ON p.warehouse_sk = w.WareHouseSK ", "WHERE p.prediction_date = CURRENT_DATE() ", "GROUP BY w.WareHouseName ", "ORDER BY products_at_risk DESC"]},
    {"id": generate_genie_id(), "question": ["Summarize the current supply chain situation."], "sql": ["SELECT metric_date, total_predictions, total_alerts, critical_alerts, high_alerts, ", "products_at_risk, products_critical_stockout, products_excess_inventory, ", "products_dead_stock, products_healthy_stock, ", "ROUND(total_inventory_value, 2) as total_inventory_value, ", "ROUND(avg_days_of_inventory, 1) as avg_doi, ", "total_reorder_recommendations ", "FROM sap_dev.gold.supply_chain_metrics ", "WHERE metric_date = CURRENT_DATE()"]},
]

sql_functions_config = sorted(
    [{"id": generate_genie_id(), "identifier": f} for f in UC_FUNCTIONS],
    key=lambda x: (x["id"], x["identifier"]),
)

join_specs = [
    {"id": generate_genie_id(), "left": {"identifier": f"{CATALOG}.{SCHEMA_GOLD}.supply_chain_predictions", "alias": "predictions"}, "right": {"identifier": f"{CATALOG}.{SCHEMA_SILVER}.dim_product", "alias": "products"}, "sql": ["predictions.product_sk = products.ProductSK", "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"]},
    {"id": generate_genie_id(), "left": {"identifier": f"{CATALOG}.{SCHEMA_GOLD}.supply_chain_predictions", "alias": "predictions"}, "right": {"identifier": f"{CATALOG}.{SCHEMA_SILVER}.dim_warehouse", "alias": "warehouses"}, "sql": ["predictions.warehouse_sk = warehouses.WareHouseSK", "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"]},
]

sample_questions = [
    {"id": generate_genie_id(), "question": ["Which products are at risk of stockout?"]},
    {"id": generate_genie_id(), "question": ["Show me critical supply chain alerts."]},
    {"id": generate_genie_id(), "question": ["Which products are dead stock?"]},
    {"id": generate_genie_id(), "question": ["What is the current inventory health?"]},
    {"id": generate_genie_id(), "question": ["Give me reorder recommendations."]},
    {"id": generate_genie_id(), "question": ["Which warehouses have the highest supply chain risk?"]},
    {"id": generate_genie_id(), "question": ["Summarize the current supply chain situation."]},
]

serialized_space = {
    "version": 2,
    "data_sources": {"tables": tables_config},
    "instructions": {
        "text_instructions": [{"id": generate_genie_id(), "content": [supply_chain_instructions]}],
        "example_question_sqls": example_questions,
        "sql_functions": sql_functions_config,
        "join_specs": join_specs,
    },
}

print(f"\nSerialized space built with:")
print(f"  Tables: {len(tables_config)}")
print(f"  SQL functions: {len(sql_functions_config)}")
print(f"  Example questions: {len(example_questions)}")
print(f"  Join specs: {len(join_specs)}")
print(f"  Sample questions: {len(sample_questions)}")

# ============================================================
# CREATE OR UPDATE GENIE SPACE (idempotent)
# ============================================================

w = WorkspaceClient()

print("\n" + "=" * 80)
print("STEP 1: FIND EXISTING SUPPLY CHAIN GENIE SPACE")
print("=" * 80)

space_id = None
try:
    response = w.genie.list_spaces()
    spaces = response.spaces if hasattr(response, 'spaces') and response.spaces else []
    for s in spaces:
        if s.title == SPACE_TITLE:
            try:
                w.genie.get_space(space_id=s.space_id)
                space_id = s.space_id
                print(f"  Found active space: {s.title} (ID: {space_id})")
            except Exception:
                print(f"  Found trashed space: {s.title} (ID: {s.space_id}) - will create new")
except Exception as e:
    print(f"  Warning listing spaces: {e}")

print("\n" + "=" * 80)
print("STEP 2: CREATE OR UPDATE GENIE SPACE")
print("=" * 80)

serialized_json = json.dumps(serialized_space)

if space_id:
    print(f"  Updating existing space (ID: {space_id})...")
    w.api_client.do(method='PATCH', path=f'/api/2.0/genie/spaces/{space_id}', body={"warehouse_id": WAREHOUSE_ID, "serialized_space": serialized_json})
    action = "updated"
    print(f"  Space updated successfully")
else:
    print(f"  Creating new space: '{SPACE_TITLE}'...")
    response = w.api_client.do(method='POST', path='/api/2.0/genie/spaces', body={"title": SPACE_TITLE, "description": SPACE_DESCRIPTION, "warehouse_id": WAREHOUSE_ID, "serialized_space": serialized_json})
    space_id = response.get('space_id')
    action = "created"
    print(f"  Space created successfully (ID: {space_id})")

# ============================================================
# VERIFY GENIE SPACE
# ============================================================

print("\n" + "=" * 80)
print("STEP 3: VERIFY GENIE SPACE")
print("=" * 80)

try:
    verified_space = w.genie.get_space(space_id=space_id)
    print(f"  Title: {verified_space.title}")
    print(f"  Space ID: {verified_space.space_id}")
    print(f"  Warehouse ID: {verified_space.warehouse_id}")
    print(f"  Action: {action}")
except Exception as e:
    print(f"  Verification failed: {e}")

# ============================================================
# SUMMARY
# ============================================================

print("\n" + "=" * 80)
print("GENIE SPACE SETUP COMPLETE")
print("=" * 80)
print(f"  Space: {SPACE_TITLE}")
print(f"  Space ID: {space_id}")
print(f"  Warehouse: {WAREHOUSE_ID}")
print(f"  Action: {action}")
print(f"  Data source tables: {len(ALL_TABLES)}")
for t in ALL_TABLES:
    print(f"    - {t}")
print(f"  UC functions attached: {len(UC_FUNCTIONS)}")
for f in UC_FUNCTIONS:
    print(f"    - {f}")
print(f"  Example SQL questions: {len(example_questions)}")
print(f"  Join specs: {len(join_specs)}")
print("=" * 80)
print("Supply Chain Genie Space is ready for natural-language Q&A.")
print("=" * 80)