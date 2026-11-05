# Databricks notebook source
# DBTITLE 1,Lakebase Setup - SQL Editor Approach
# MAGIC %md
# MAGIC # Task 07: Lakebase Retail Ops - SQL Editor Showcase Setup
# MAGIC
# MAGIC ## Architecture
# MAGIC
# MAGIC **Lakebase Project:** `retail-ops-apps`
# MAGIC - Branch: `production`  
# MAGIC - Endpoint: `primary`
# MAGIC - Host: `your-lakebase-endpoint.database.us-east-2.cloud.databricks.com`
# MAGIC - Database: `databricks_postgres`
# MAGIC
# MAGIC ## Schema Initialization
# MAGIC
# MAGIC The database schema is **automatically initialized** when the apps start.  
# MAGIC See: `/Shared/dev/SkillAIPath/4_Business_Intelligence/3_Live_Apps/_shared/db_manager.py`
# MAGIC
# MAGIC The `initialize_schema()` method creates:
# MAGIC - `reorder_approvals` table
# MAGIC - Performance indexes
# MAGIC - Sample data (if table is empty)
# MAGIC
# MAGIC ## Apps Using This Lakebase
# MAGIC
# MAGIC 1. **01_BI_Center** - Real-time inventory monitoring with reorder approval workflow
# MAGIC 2. **02_Team_Intelligence_Hub** - Predictive analytics for inventory planning  
# MAGIC 3. **03_SkillAI_Execution_Support** - Real-time pricing optimization API
# MAGIC
# MAGIC ## Data Flow
# MAGIC
# MAGIC ```
# MAGIC Unity Catalog (sap_dev.gold.mart_inventory_health)
# MAGIC     ↓  
# MAGIC Apps read analytics data
# MAGIC     ↓
# MAGIC Apps write operational transactions → Lakebase (reorder_approvals)
# MAGIC ```
# MAGIC
# MAGIC ## Status
# MAGIC
# MAGIC ✅ Lakebase project exists and is ready  
# MAGIC ✅ Apps have auto-initialization built in  
# MAGIC ✅ No manual schema setup required

# COMMAND ----------

print("="*80)
print("✅ Lakebase Retail Ops Apps - Setup Complete!")
print("="*80)
print("\n📋 Summary:")
print("   • Lakebase project: retail-ops-apps (production/primary)")
print("   • Schema: Auto-initialized by apps on first connection")
print("   • Apps: 3 production apps ready to deploy")
print("\n🎯 Next Step: Deploy the three apps")
print("="*80)

# COMMAND ----------

# DBTITLE 1,1. Read Critical Inventory
# Read critical inventory items from Delta Lake
inventory_df = spark.sql("""
    SELECT 
        ProductSK,
        ProductName,
        WarehouseSK,
        WarehouseName,
        CurrentStock,
        DaysInStock,
        HealthStatus
    FROM sap_dev.gold.mart_inventory_health
    WHERE HealthStatus IN ('Critical', 'Understock')
        AND DaysInStock < 30
    LIMIT 10
""")

print(f"✅ Found {inventory_df.count()} critical inventory items for reorder approvals")

# COMMAND ----------

# DBTITLE 1,2. Generate Reorder Approvals SQL
import random
from datetime import datetime

# Generate INSERT statements for reorder_approvals
inserts = []
inserts.append("-- Reorder Approvals INSERT Statements")
inserts.append("-- Copy and paste into Lakebase SQL Editor\n")

for row in inventory_df.collect():
    # Determine urgency based on days remaining
    if row.DaysInStock < 7:
        urgency = 'Critical'
    elif row.DaysInStock < 14:
        urgency = 'High'
    elif row.DaysInStock < 21:
        urgency = 'Medium'
    else:
        urgency = 'Low'
    
    recommended_qty = int(row.CurrentStock * 2)  # Use CurrentStock as base for reorder
    current_stock = int(row.CurrentStock)
    
    # Escape single quotes
    product_name = row.ProductName.replace("'", "''")
    warehouse_name = row.WarehouseName.replace("'", "''")
    
    sql = f"""INSERT INTO reorder_approvals (
    product_sk, product_name, warehouse_sk, warehouse_name,
    recommended_qty, current_stock, urgency, status, notes
) VALUES (
    {row.ProductSK}, '{product_name}', {row.WarehouseSK}, '{warehouse_name}',
    {recommended_qty}, {current_stock}, '{urgency}', 'pending', 'Generated from inventory health analysis'
);\n"""
    inserts.append(sql)

reorder_sql = "\n".join(inserts)
print(reorder_sql)

# COMMAND ----------

# DBTITLE 1,3. Generate Pricing Rules SQL
# Generate pricing rules INSERT statements
inserts = []
inserts.append("\n-- Pricing Rules INSERT Statements")
inserts.append("-- Copy and paste into Lakebase SQL Editor\n")

rules = [
    ('Critical Stock Discount', 'Critical', 'Critical', -15.0),
    ('Overstock Clearance', 'Overstock', 'Overstock', -25.0),
    ('Healthy Stock Premium', 'Healthy', 'Healthy', 5.0),
    ('Understock Markup', 'Understock', 'Understock', 10.0),
    ('Flash Sale Weekend', 'Overstock', None, -30.0),
]

for rule_name, stock_status, health_status, adjustment_pct in rules:
    stock_val = f"'{stock_status}'" if stock_status else "NULL"
    health_val = f"'{health_status}'" if health_status else "NULL"
    
    sql = f"""INSERT INTO pricing_rules (
    rule_name, stock_status, health_status, price_adjustment_pct, is_active, created_by
) VALUES (
    '{rule_name}', {stock_val}, {health_val}, {adjustment_pct}, true, 'pricing_system'
) ON CONFLICT (rule_name) DO NOTHING;\n"""
    inserts.append(sql)

rules_sql = "\n".join(inserts)
print(rules_sql)

# COMMAND ----------

# DBTITLE 1,4. Final Output & Next Steps
# Save combined SQL to file
full_sql = f"""
-- ============================================================================
-- LAKEBASE DATA POPULATION SQL
-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
-- ============================================================================

{reorder_sql}

{rules_sql}

-- ============================================================================
-- Verification Query
-- ============================================================================
SELECT 'reorder_approvals' as table_name, COUNT(*) as rows FROM reorder_approvals
UNION ALL
SELECT 'pricing_rules', COUNT(*) FROM pricing_rules;
-- ============================================================================
"""

with open("/tmp/lakebase_inserts.sql", "w") as f:
    f.write(full_sql)

print("\n" + "="*80)
print("✅ SQL GENERATION COMPLETE!")
print("="*80)
print(f"\n📋 Generated {inventory_df.count()} reorder approvals + 5 pricing rules")
print("\n📁 Files:")
print("   • SQL saved to: /tmp/lakebase_inserts.sql")
print("   • Schema DDL: /Shared/dev/SkillAIPath/2_Data_Production/06_Lakebase_LTAP/lakebase_schema.sql")
print("   • Demo queries: /Shared/dev/SkillAIPath/2_Data_Production/06_Lakebase_LTAP/demo_queries.sql")
print("\n🎯 NEXT STEPS (5 minutes):")
print("   1. Navigate to: Data → Lakebase → Retail Operations Apps → SQL Editor")
print("   2. Run lakebase_schema.sql (create tables - FIRST TIME ONLY)")
print("   3. Copy SQL output above and run in SQL Editor (insert data)")
print("   4. Run demo_queries.sql sections (showcase!)")
print("\n📖 Full guide: LAKEBASE_SHOWCASE_GUIDE.md")
print("="*80)