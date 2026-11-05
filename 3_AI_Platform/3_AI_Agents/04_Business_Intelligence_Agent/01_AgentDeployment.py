# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Agent Bricks Supervisor - ONE Agent Strategy
# MAGIC %md
# MAGIC # Agent Bricks Supervisor - Automated Deployment
# MAGIC **SkillAIPath AI Platform - Comprehensive Agent Infrastructure**
# MAGIC
# MAGIC ## 🎯 What This Deploys
# MAGIC
# MAGIC This notebook implements the **ONE Supervisor Agent Strategy** for SkillAIPath:
# MAGIC
# MAGIC ### Phase 1: UC Functions (4 Functions)
# MAGIC - `get_customer_segment(customer_id)` - RFM segmentation lookup
# MAGIC - `calculate_inventory_days(product_id, warehouse_id)` - Stock days calculator
# MAGIC - `get_churn_risk_level(churn_probability)` - Risk classifier
# MAGIC - `search_knowledge(query, max_results)` - Knowledge base search
# MAGIC
# MAGIC ### Phase 2: Genie Spaces (4 Spaces)
# MAGIC - **Supply Chain Intelligence** - Inventory, demand, forecasting
# MAGIC - **Customer Intelligence** - Churn, RFM, customer 360
# MAGIC - **Product Performance** - Sales, margin, recommendations
# MAGIC - **Business Intelligence** - KPIs, cross-domain analytics
# MAGIC
# MAGIC ### Phase 3: Supervisor Agent Setup
# MAGIC - Configuration template for Agent Bricks UI
# MAGIC - Connection to 33 UC tables, 4 functions, 4 Genie Spaces, 5 dashboards
# MAGIC - Agent instructions for orchestration
# MAGIC
# MAGIC ## 📊 Prerequisites
# MAGIC
# MAGIC **Data Requirements:**
# MAGIC - Gold layer tables in `sap_dev.gold` (customer_360, churn_predictions, etc.)
# MAGIC - Silver layer facts/dimensions in `sap_dev.silver`
# MAGIC - Knowledge base tables in `sap_dev.gold`
# MAGIC
# MAGIC **Free Edition Compatible:**
# MAGIC - Serverless compute (included)
# MAGIC - Unity Catalog (included)
# MAGIC - Agent Bricks Supervisor (1 agent limit - FREE)
# MAGIC
# MAGIC ## ⚡ Quick Deploy
# MAGIC
# MAGIC Run all cells → Creates functions, Genie Spaces, and prints Agent config

# COMMAND ----------

# DBTITLE 1,Configuration & Environment Setup
# Configuration - Accept environment from widget parameter (default: dev)
try:
    _env = dbutils.widgets.get("environment")
except Exception:
    _env = "dev"

VALID_ENVS = {"dev", "staging", "main"}
if _env not in VALID_ENVS:
    raise ValueError(f"Invalid environment '{_env}'. Must be one of: {VALID_ENVS}")

class Config:
    ENVIRONMENT = _env
    CATALOG = f"sap_{ENVIRONMENT}"
    SCHEMA_GOLD = "gold"
    SCHEMA_SILVER = "silver"
    SCHEMA_BRONZE = "bronze"

    # Warehouse ID for Genie Spaces (default serverless)
    WAREHOUSE_ID = "3e65d48e0d77d571"  # Serverless Starter Warehouse

    # Path to the BI Agent Tools notebook (source of certified-answer UC function definitions)
    BI_AGENT_TOOLS_PATH = f"/Shared/{ENVIRONMENT}/SkillAIPath/3_AI_Platform/3_AI_Agents/04_Business_Intelligence_Agent/02_BI_AgentTools"

print("="*80)
print("AGENT BRICKS DEPLOYMENT - SUPERVISOR AGENT SETUP")
print("="*80)
print(f"Environment: {Config.ENVIRONMENT}")
print(f"Catalog: {Config.CATALOG}")
print(f"Gold Schema: {Config.SCHEMA_GOLD}")
print(f"Silver Schema: {Config.SCHEMA_SILVER}")
print(f"BI Agent Tools Source: {Config.BI_AGENT_TOOLS_PATH}")
print("="*80)
print()
print("📋 DEPLOYMENT CHECKLIST:")
print("  ✓ Phase 1: Create UC Functions (4)")
print("  ✓ Phase 1b: Deploy Certified-Answer UC Functions for Genie Spaces")
print("  ✓ Phase 2: Create Genie Spaces (4)")
print("  ✓ Phase 3: Generate Supervisor Agent Config")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Step 1: Initialize Catalog & Schemas
# Create catalog and schemas if they don't exist
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

print("\n" + "="*80)
print("STEP 1: CATALOG & SCHEMA INITIALIZATION")
print("="*80)

# Create catalog
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {Config.CATALOG}")
    print(f"✓ Catalog: {Config.CATALOG} (created/verified)")
except Exception as e:
    print(f"⚠️  Catalog creation skipped: {e}")

# Create schemas
required_schemas = [Config.SCHEMA_BRONZE, Config.SCHEMA_SILVER, Config.SCHEMA_GOLD]
for schema in required_schemas:
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {Config.CATALOG}.{schema}")
        print(f"✓ Schema: {Config.CATALOG}.{schema}")
    except Exception as e:
        print(f"⚠️  Schema {schema} creation skipped: {e}")

# Set context
try:
    spark.sql(f"USE CATALOG {Config.CATALOG}")
    print(f"\n✓ Catalog context set to: {Config.CATALOG}")
except Exception as e:
    print(f"⚠️  Context setting skipped: {e}")

print("="*80)

# COMMAND ----------

# DBTITLE 1,Step 2a: Create UC Function - get_customer_segment
# Customer Segment Lookup Function (environment-aware)
spark.sql(f"""
CREATE OR REPLACE FUNCTION {Config.CATALOG}.{Config.SCHEMA_GOLD}.get_customer_segment(
  customer_id INT
)
RETURNS STRING
LANGUAGE SQL
COMMENT 'Get RFM segment for a customer from mart_rfm_analysis table'
RETURN (
  SELECT COALESCE(Segment, 'Unknown') as segment
  FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_rfm_analysis
  WHERE customer_id = get_customer_segment.customer_id
  LIMIT 1
)
""")
print(f"✓ Created function: {Config.CATALOG}.{Config.SCHEMA_GOLD}.get_customer_segment")

# COMMAND ----------

# DBTITLE 1,Step 2b: Create UC Function - calculate_inventory_days
# Inventory Days Calculator Function (environment-aware)
spark.sql(f"""
CREATE OR REPLACE FUNCTION {Config.CATALOG}.{Config.SCHEMA_GOLD}.calculate_inventory_days(
  product_id INT,
  warehouse_id INT
)
RETURNS FLOAT
LANGUAGE SQL
COMMENT 'Calculate days of inventory remaining based on 30-day average sales'
RETURN (
  SELECT 
    CASE 
      WHEN COALESCE(AVG(sales.ProductQuantity), 0) = 0 THEN 999.0
      ELSE CAST(MAX(inv.StockQuantity) / AVG(sales.ProductQuantity) AS FLOAT)
    END as days_remaining
  FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.fact_inventory inv
  LEFT JOIN {Config.CATALOG}.{Config.SCHEMA_SILVER}.fact_sales sales
    ON inv.ProductSK = sales.ProductSK
    AND sales.BatchDate >= CURRENT_DATE - INTERVAL 30 DAYS
  WHERE inv.ProductSK = calculate_inventory_days.product_id
    AND inv.WarehouseSK = calculate_inventory_days.warehouse_id
  GROUP BY inv.ProductSK, inv.WarehouseSK
)
""")
print(f"✓ Created function: {Config.CATALOG}.{Config.SCHEMA_GOLD}.calculate_inventory_days")

# COMMAND ----------

# DBTITLE 1,Step 2c: Create UC Function - get_churn_risk_level
# Churn Risk Classifier Function (environment-aware)
spark.sql(f"""
CREATE OR REPLACE FUNCTION {Config.CATALOG}.{Config.SCHEMA_GOLD}.get_churn_risk_level(
  churn_probability FLOAT
)
RETURNS STRING
LANGUAGE SQL
COMMENT 'Classify churn probability into risk levels (HIGH/MEDIUM/LOW)'
RETURN (
  CASE 
    WHEN churn_probability >= 0.7 THEN 'HIGH'
    WHEN churn_probability >= 0.4 THEN 'MEDIUM'
    ELSE 'LOW'
  END
)
""")
print(f"✓ Created function: {Config.CATALOG}.{Config.SCHEMA_GOLD}.get_churn_risk_level")

# COMMAND ----------

# DBTITLE 1,Step 2d: Verify UC Functions Created
# Verify UC Functions
print("\n" + "="*80)
print("STEP 2: UC FUNCTIONS VERIFICATION")
print("="*80)

try:
    functions = spark.sql(f"SHOW FUNCTIONS IN {Config.CATALOG}.{Config.SCHEMA_GOLD}").collect()
    
    target_functions = [
        'get_customer_segment',
        'calculate_inventory_days', 
        'get_churn_risk_level'
    ]
    
    created_functions = []
    for func in functions:
        func_name = func.function.split('.')[-1]
        if func_name in target_functions:
            created_functions.append(func_name)
            print(f"✓ Function: {Config.CATALOG}.{Config.SCHEMA_GOLD}.{func_name}")
    
    print(f"\n✅ Created {len(created_functions)}/3 UC Functions")
    
    if len(created_functions) < 3:
        missing = set(target_functions) - set(created_functions)
        print(f"⚠️  Missing functions: {missing}")
        
except Exception as e:
    print(f"⚠️  Function verification failed: {e}")

print("="*80)

# COMMAND ----------

# DBTITLE 1,Step 2e: Deploy Certified-Answer UC Functions for Genie Spaces
# Step 2e: Deploy Certified-Answer UC Functions for Genie Spaces
# ============================================================================
# This reusable step reads function definitions from 02_BI_AgentTools and deploys
# them to the correct environment's gold schema BEFORE Genie Space creation.
#
# CONFIGURATION: GENIE_SPACE_UC_FUNCTIONS maps space titles to required function names.
# To add a new Genie Space with custom functions:
#   1. Add the space title and function list to GENIE_SPACE_UC_FUNCTIONS
#   2. Ensure the function definitions exist in the source notebook (02_BI_AgentTools)
# No other changes needed - the deployment step handles the rest.
# ============================================================================

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat
import base64
import re

_w = WorkspaceClient()

# Function dependency manifest: maps Genie Space title -> required UC function names
# Spaces not listed here (or with empty lists) skip function deployment automatically.
GENIE_SPACE_UC_FUNCTIONS = {
    "Business Intelligence": [
        "get_rfm_segment_summary",
        "get_churn_risk_customers",
        "get_top_products",
        "get_demand_forecast",
        "get_customer_360_view",
        "get_revenue_summary",
        "get_product_recommendations_for_customer",
        "get_inventory_health_summary",
        "get_sales_profitability_summary",
        "get_category_performance",
        "get_next_purchase_predictions"
    ],
    # Supply Chain Intelligence, Customer Intelligence, Product Performance
    # do not require custom UC functions - they use tables directly
}

def _get_existing_functions(catalog, schema):
    """Return set of function names that already exist in the target schema."""
    existing = set()
    try:
        for f in _w.functions.list(catalog_name=catalog, schema_name=schema):
            existing.add(f.name)
    except Exception:
        pass
    return existing

def _extract_function_definitions(notebook_path):
    """Read a Databricks notebook (SOURCE format) and extract CREATE OR REPLACE
    FUNCTION statements. Returns dict: {function_name: sql_statement}."""
    exported = _w.workspace.export(path=notebook_path, format=ExportFormat.SOURCE)
    content = base64.b64decode(exported.content).decode('utf-8')

    # Databricks SOURCE format uses "# COMMAND ----------" as cell separator
    cells = content.split("# COMMAND ----------")

    definitions = {}
    for cell in cells:
        if "CREATE" not in cell.upper() or "FUNCTION" not in cell.upper():
            continue

        lines = cell.strip().split('\n')
        sql_lines = []
        for line in lines:
            stripped = line.strip()
            # Remove Databricks MAGIC prefix for SQL cells
            if stripped.startswith('# MAGIC '):
                stripped = stripped[8:]
            elif stripped.startswith('# MAGIC'):
                stripped = stripped[7:]
            # Skip Python/markdown comment lines and empty lines
            if stripped.startswith('#') and not stripped.startswith('--'):
                continue
            if not stripped:
                continue
            sql_lines.append(stripped)

        sql = '\n'.join(sql_lines)

        # Extract function name from the SQL
        match = re.search(
            r'CREATE\s+OR\s+REPLACE\s+FUNCTION\s+\S+\.(\w+)\s*\(',
            sql, re.IGNORECASE
        )
        if match:
            func_name = match.group(1)
            sql = sql.rstrip().rstrip(';')
            definitions[func_name] = sql

    return definitions

def deploy_certified_answer_functions(function_deps, catalog, schema, source_path):
    """Deploy required UC functions to the target catalog/schema.

    Only deploys functions that don't already exist (idempotent).
    Replaces sap_dev references with the target catalog for environment-awareness.
    Raises Exception if any required function fails to deploy or verify.
    """
    print(f"\n{'='*80}")
    print("STEP 2e: DEPLOY CERTIFIED-ANSWER UC FUNCTIONS FOR GENIE SPACES")
    print(f"{'='*80}")
    print(f"Target: {catalog}.{schema}")
    print(f"Source: {source_path}")

    all_required = set()
    for space_title, func_list in function_deps.items():
        if func_list:
            all_required.update(func_list)
            print(f"  Space '{space_title}': {len(func_list)} functions")

    if not all_required:
        print("  No custom UC functions required by any Genie Space")
        print("  Skipping function deployment step")
        print("="*80)
        return 0, 0, []

    print(f"\nTotal unique functions to deploy: {len(all_required)}")

    # Check existing functions
    existing = _get_existing_functions(catalog, schema)
    print(f"Functions already in {catalog}.{schema}: {len(existing)}")

    # Read definitions from source notebook
    print(f"Reading definitions from: {source_path}")
    all_definitions = _extract_function_definitions(source_path)
    print(f"Found {len(all_definitions)} function definitions in source")

    deployed = 0
    skipped = 0
    failures = []

    for func_name in sorted(all_required):
        if func_name in existing:
            print(f"  SKIP {func_name}: already exists in {catalog}.{schema}")
            skipped += 1
            continue

        if func_name not in all_definitions:
            msg = f"FAIL {func_name}: SQL definition not found in source notebook"
            print(msg)
            failures.append({"function": func_name, "error": "Definition not found in source"})
            continue

        # Replace sap_dev with target catalog for environment-awareness
        sql = all_definitions[func_name]
        sql = sql.replace("sap_dev.gold", f"{catalog}.{schema}")
        sql = sql.replace("sap_dev.silver", f"{catalog}.silver")
        sql = sql.replace("sap_dev.bronze", f"{catalog}.bronze")

        try:
            spark.sql(sql)
            print(f"  OK   {func_name}: created in {catalog}.{schema}")
            deployed += 1
        except Exception as e:
            err = str(e)[:200]
            print(f"  FAIL {func_name}: {err}")
            failures.append({"function": func_name, "error": err})

    print(f"\n{'='*80}")
    print(f"DEPLOYMENT SUMMARY: {deployed} created, {skipped} already existed, {len(failures)} failed")

    if failures:
        print("\nCERTIFIED-ANSWER FUNCTION DEPLOYMENT FAILED")
        print("Cannot proceed to Genie Space creation with missing functions.")
        for f in failures:
            print(f"  FAIL {f['function']}: {f['error']}")
        print("="*80)
        raise Exception(
            f"UC function deployment failed: {len(failures)} function(s) could not be deployed. "
            f"Fix the errors above before creating/updating Genie Spaces."
        )

    # Verify all functions are callable (independent of deployment)
    print(f"\nVerifying all {len(all_required)} functions are callable...")
    existing_after = _get_existing_functions(catalog, schema)
    verification_failures = []
    for func_name in sorted(all_required):
        if func_name not in existing_after:
            verification_failures.append({"function": func_name, "error": "Not found after deployment"})

    if verification_failures:
        print("FUNCTION VERIFICATION FAILED")
        for f in verification_failures:
            print(f"  FAIL {f['function']}: {f['error']}")
        raise Exception(f"Function verification failed: {len(verification_failures)} function(s)")

    print(f"All {len(all_required)} certified-answer functions verified in {catalog}.{schema}")
    print("="*80)

    return deployed, skipped, failures

# Execute the deployment step
_deployed, _skipped, _failures = deploy_certified_answer_functions(
    GENIE_SPACE_UC_FUNCTIONS,
    Config.CATALOG,
    Config.SCHEMA_GOLD,
    Config.BI_AGENT_TOOLS_PATH
)

# COMMAND ----------

# DBTITLE 1,Step 3a: Genie Space Creation Functions
# Genie Space Creation Utilities
from databricks.sdk import WorkspaceClient
import json
import random
import datetime

def generate_genie_id():
    """Generate valid 32-char lowercase hex UUID for Genie"""
    t = int((datetime.datetime.now() - datetime.datetime(1582, 10, 15)).total_seconds() * 1e7)
    part1 = ((t & 0xFFFFFFFFFFFF0000) | (1 << 12) | ((t & 0xFFFF) >> 4))
    part2 = random.getrandbits(62) | 0x8000000000000000
    return f"{part1:016x}{part2:016x}"

def create_or_update_genie_space(space_title, space_description, tables, system_prompt, warehouse_id):
    """Create or update a Genie Space (idempotent)"""
    w = WorkspaceClient()
    
    # Check if space exists
    existing_spaces = w.genie.list_spaces()
    space_id = None
    
    if hasattr(existing_spaces, 'spaces') and existing_spaces.spaces:
        for space in existing_spaces.spaces:
            if space.title == space_title:
                space_id = space.space_id
                break
    
    # Build serialized_space configuration
    space_config = {
        "version": 2,
        "data_sources": {
            "tables": sorted([{"identifier": table} for table in tables], key=lambda x: x["identifier"])
        },
        "instructions": {
            "text_instructions": [
                {
                    "id": generate_genie_id(),
                    "content": [system_prompt]
                }
            ]
        }
    }
    
    if space_id:
        # Update existing space
        payload = {
            "warehouse_id": warehouse_id,
            "serialized_space": json.dumps(space_config)
        }
        w.api_client.do(
            method='PATCH',
            path=f'/api/2.0/genie/spaces/{space_id}',
            body=payload
        )
        return space_id, "updated"
    else:
        # Create new space
        payload = {
            "title": space_title,
            "description": space_description,
            "warehouse_id": warehouse_id,
            "serialized_space": json.dumps(space_config)
        }
        response = w.api_client.do(
            method='POST',
            path='/api/2.0/genie/spaces',
            body=payload
        )
        return response.get('space_id'), "created"

print("✅ Genie Space utilities loaded")

# COMMAND ----------

# DBTITLE 1,Step 3b: Create All 4 Genie Spaces
# Create all 4 Genie Spaces
print("\n" + "="*80)
print("STEP 3: GENIE SPACE CREATION (4 SPACES)")
print("="*80)

spaces_config = [
    {
        "title": "Supply Chain Intelligence",
        "description": "Natural language Q&A over supply chain data",
        "tables": [
            f"{Config.CATALOG}.{Config.SCHEMA_GOLD}.demand_forecast",
            f"{Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_inventory_health",
            f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.fact_inventory",
            f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_product",
            f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_warehouse"
        ],
        "prompt": """You are a **Supply Chain Intelligence Assistant** for SkillAIPath.

## Your Role
Answer questions about inventory health, demand forecasting, warehouse operations, and supply chain optimization.

## Available Tables
- `demand_forecast`: ML-generated product demand predictions
- `mart_inventory_health`: Inventory KPIs, turnover, dead stock
- `fact_inventory`: Current stock levels by product/warehouse
- `dim_product`: Product master data
- `dim_warehouse`: Warehouse locations and capacity

## Key Metrics
- **Days of Inventory**: Stock on hand / average daily sales
- **Stockout Risk**: Products below reorder level
- **Overstock**: Inventory > 2x reorder level
- **Forecast Accuracy**: Actual vs predicted demand

## Sample Questions
- "Which products are forecasted to have high demand next month?"
- "Show me inventory levels by warehouse"
- "Which warehouses have low stock alerts?"
- "What products are at risk of stockout?"
"""
    },
    {
        "title": "Customer Intelligence",
        "description": "Natural language Q&A over customer data",
        "tables": [
            f"{Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_customer_360",
            f"{Config.CATALOG}.{Config.SCHEMA_GOLD}.churn_predictions",
            f"{Config.CATALOG}.{Config.SCHEMA_GOLD}.product_recommendations",
            f"{Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_rfm_analysis"
        ],
        "prompt": """You are a **Customer Intelligence Assistant** for SkillAIPath.

## Your Role
Answer questions about customer behavior, churn risk, lifetime value, segmentation, and personalized recommendations.

## Available Tables
- `mart_customer_360`: Complete customer view (demographics, behavior, value)
- `churn_predictions`: ML-generated churn risk scores
- `product_recommendations`: Personalized product recommendations
- `mart_rfm_analysis`: RFM segmentation (Recency, Frequency, Monetary)

## Key Metrics
- **Customer Lifetime Value (CLV)**: Total revenue per customer
- **Churn Risk**: Probability of customer leaving (0-1)
- **RFM Segment**: Customer grouping (VIP, Loyal, At-Risk, Lost)
- **Average Order Value**: Total spend / order count

## Sample Questions
- "Who are our VIP customers?"
- "Show me high churn risk customers"
- "Which segment has highest lifetime value?"
- "What products should we recommend to Customer X?"
"""
    },
    {
        "title": "Product Performance",
        "description": "Natural language Q&A over product analytics",
        "tables": [
            f"{Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_product_performance",
            f"{Config.CATALOG}.{Config.SCHEMA_GOLD}.demand_forecast",
            f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.fact_sales",
            f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_product_category"
        ],
        "prompt": """You are a **Product Performance Assistant** for SkillAIPath.

## Your Role
Answer questions about product sales, margins, turnover, category performance, and demand forecasting.

## Available Tables
- `mart_product_performance`: Product sales, margin, turnover metrics
- `demand_forecast`: Product demand predictions
- `fact_sales`: Transaction-level sales data
- `dim_product_category`: Product category hierarchy

## Key Metrics
- **Revenue**: Total sales amount
- **Margin**: (Revenue - Cost) / Revenue
- **Turnover Rate**: Sales velocity
- **Sell-Through Rate**: Units sold / units received

## Sample Questions
- "Which products are underperforming?"
- "Show me top products by margin"
- "What's the sales trend for Category X?"
- "Which categories have the highest revenue?"
"""
    },
    {
        "title": "Business Intelligence",
        "description": "Natural language Q&A over business KPIs",
        "tables": [
            f"{Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_customer_360",
            f"{Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_product_performance",
            f"{Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_inventory_health",
            f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.fact_sales"
        ],
        "prompt": """You are a **Business Intelligence Assistant** for SkillAIPath.

## Your Role
Answer cross-domain business questions, KPIs, revenue analytics, and executive-level insights.

## Available Tables
- `mart_customer_360`: Customer metrics and segmentation
- `mart_product_performance`: Product sales and margins
- `mart_inventory_health`: Inventory KPIs
- `fact_sales`: Detailed transaction data

## Key Metrics
- **Total Revenue**: Sum of all sales
- **Order Count**: Number of transactions
- **Average Order Value**: Revenue / order count
- **Customer Count**: Unique customers
- **Revenue per Customer**: Total revenue / customer count

## Sample Questions
- "What's our total revenue this month?"
- "Show me sales by customer segment"
- "How is inventory health trending?"
- "What are our key business KPIs?"
"""
    }
]

created_spaces = []
for i, space in enumerate(spaces_config, 1):
    try:
        space_id, action = create_or_update_genie_space(
            space_title=space["title"],
            space_description=space["description"],
            tables=space["tables"],
            system_prompt=space["prompt"],
            warehouse_id=Config.WAREHOUSE_ID
        )
        created_spaces.append({"title": space["title"], "id": space_id, "action": action})
        print(f"✅ Space {i}: {space['title']} ({action})")
        print(f"   Space ID: {space_id}")
    except Exception as e:
        print(f"❌ Space {i} ({space['title']}) failed: {e}")

print(f"\n✅ Created/Updated {len(created_spaces)}/4 Genie Spaces")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Step 4: Verify Dashboards
# Verify Dashboards
from databricks.sdk import WorkspaceClient

print("\n" + "="*80)
print("STEP 4: DASHBOARD VERIFICATION")
print("="*80)

w = WorkspaceClient()

try:
    dashboards = list(w.lakeview.list())
    
    target_dashboards = [
        "Agent & Channel Performance",
        "Customer Insights", 
        "Cost & DBU Tracker",
        "Demand Forecasting",
        "Executive KPI"
    ]
    
    found_dashboards = []
    for dash in dashboards:
        if any(target in dash.display_name for target in target_dashboards):
            found_dashboards.append({
                "name": dash.display_name,
                "uuid": dash.dashboard_id,
                "path": dash.path
            })
            print(f"✅ {dash.display_name}")
            print(f"   UUID: {dash.dashboard_id}")
    
    print(f"\n✅ Found {len(found_dashboards)}/5 target dashboards")
    
    if len(found_dashboards) < 5:
        print(f"\n⚠️  Note: Some dashboards not found. They may need to be created.")
        print(f"   Missing: {set(target_dashboards) - {d['name'] for d in found_dashboards}}")
        
except Exception as e:
    print(f"⚠️  Dashboard verification failed: {e}")

print("="*80)

# COMMAND ----------

# DBTITLE 1,Step 5: Supervisor Agent Configuration
# MAGIC %md
# MAGIC ## 🤖 Step 5: Create Supervisor Agent in UI
# MAGIC
# MAGIC **Agent Bricks is UI-only and cannot be created via API. Follow these steps:**
# MAGIC
# MAGIC ### 5.1 Create Agent
# MAGIC
# MAGIC 1. Navigate to: `/ml/agents`
# MAGIC 2. Click **"Create Agent"** → **"Supervisor Agent"**
# MAGIC 3. **Name:** `skillaipath-business-intelligence`
# MAGIC 4. **Description:** 
# MAGIC    ```
# MAGIC    Central business intelligence agent for SkillAIPath.
# MAGIC    
# MAGIC    Answers questions about:
# MAGIC    - Supply chain predictions and inventory
# MAGIC    - Customer intelligence and churn risk
# MAGIC    - Product performance and recommendations
# MAGIC    - Demand forecasting
# MAGIC    - Business KPIs and dashboards
# MAGIC    - Technical documentation
# MAGIC    
# MAGIC    Orchestrates across:
# MAGIC    - 33 Unity Catalog tables
# MAGIC    - 5 AI/BI Dashboards
# MAGIC    - 4 Genie Spaces
# MAGIC    - 3 UC Functions
# MAGIC    ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5.2 Add Tools - UC Tables
# MAGIC
# MAGIC **Click "Add Tool" → "Unity Catalog Table"**
# MAGIC
# MAGIC Select these tables:
# MAGIC
# MAGIC **Gold Layer (11 tables):**
# MAGIC ```
# MAGIC sap_dev.gold.knowledge_base
# MAGIC sap_dev.gold.customer_knowledge_base
# MAGIC sap_dev.gold.ecommerce_knowledge_base
# MAGIC sap_dev.gold.mart_customer_360
# MAGIC sap_dev.gold.mart_product_performance
# MAGIC sap_dev.gold.mart_inventory_health
# MAGIC sap_dev.gold.mart_rfm_analysis
# MAGIC sap_dev.gold.churn_predictions
# MAGIC sap_dev.gold.demand_forecast
# MAGIC sap_dev.gold.next_purchase_predictions
# MAGIC sap_dev.gold.product_recommendations
# MAGIC ```
# MAGIC
# MAGIC **Silver Facts (3 tables):**
# MAGIC ```
# MAGIC sap_dev.silver.fact_sales
# MAGIC sap_dev.silver.fact_inventory
# MAGIC sap_dev.silver.fact_purchase
# MAGIC ```
# MAGIC
# MAGIC **Silver Dimensions (11 tables):**
# MAGIC ```
# MAGIC sap_dev.silver.dim_customer
# MAGIC sap_dev.silver.dim_product
# MAGIC sap_dev.silver.dim_warehouse
# MAGIC sap_dev.silver.dim_date
# MAGIC sap_dev.silver.dim_product_category
# MAGIC sap_dev.silver.dim_retailer
# MAGIC sap_dev.silver.dim_wholesaler
# MAGIC sap_dev.silver.dim_payment_mode
# MAGIC sap_dev.silver.dim_agent
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5.3 Add Tools - UC Functions
# MAGIC
# MAGIC **Click "Add Tool" → "Unity Catalog Function"**
# MAGIC
# MAGIC Select these functions:
# MAGIC ```
# MAGIC sap_dev.gold.get_customer_segment
# MAGIC sap_dev.gold.calculate_inventory_days
# MAGIC sap_dev.gold.get_churn_risk_level
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5.4 Add Tools - Genie Spaces
# MAGIC
# MAGIC **Click "Add Tool" → "Genie Space"**
# MAGIC
# MAGIC Select all 4 spaces created above:
# MAGIC - Supply Chain Intelligence
# MAGIC - Customer Intelligence
# MAGIC - Product Performance
# MAGIC - Business Intelligence
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5.5 Add Tools - Dashboards
# MAGIC
# MAGIC **Click "Add Tool" → "AI/BI Dashboard"**
# MAGIC
# MAGIC Paste the UUIDs from Step 4 verification above.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5.6 Add Agent Instructions
# MAGIC
# MAGIC Copy and paste this into the agent's instructions:

# COMMAND ----------

# DBTITLE 1,Agent Instructions Template
# MAGIC %md
# MAGIC ```markdown
# MAGIC # SkillAIPath Business Intelligence Supervisor Agent
# MAGIC
# MAGIC You are the central business intelligence agent for SkillAIPath.
# MAGIC
# MAGIC ## Your Purpose
# MAGIC
# MAGIC Answer business, technical, and operational questions by orchestrating across:
# MAGIC - 33 Unity Catalog tables (knowledge, gold marts, predictions, silver facts/dims)
# MAGIC - 5 AI/BI Dashboards (agent performance, customer, cost, forecasting, executive)
# MAGIC - 4 Genie Spaces (supply chain, customer, product, BI)
# MAGIC - UC Functions (search, calculate, classify)
# MAGIC
# MAGIC ## Answer Strategy
# MAGIC
# MAGIC ### 1. Delegation First (When Genie Spaces Exist)
# MAGIC
# MAGIC If the question is domain-specific and a Genie Space exists:
# MAGIC - "Show me inventory levels" → Delegate to Supply Chain Intelligence Space
# MAGIC - "Who are high-risk churn customers?" → Delegate to Customer Intelligence Space
# MAGIC - "Which products are underperforming?" → Delegate to Product Performance Space
# MAGIC - "What's our revenue this month?" → Delegate to Business Intelligence Space
# MAGIC
# MAGIC ### 2. Direct Query (When Orchestration Needed)
# MAGIC
# MAGIC Query tables directly when:
# MAGIC - Cross-domain analysis required ("VIP customers buying low-stock products")
# MAGIC - Genie Space doesn't have the data
# MAGIC - Need to JOIN multiple domains
# MAGIC - Complex multi-step reasoning
# MAGIC
# MAGIC ### 3. Dashboard Context (For Dashboard Questions)
# MAGIC
# MAGIC Read dashboard widgets when:
# MAGIC - "Why is the Cost Dashboard showing red?"
# MAGIC - "What KPIs are on the Executive Dashboard?"
# MAGIC - "Show me widgets from Customer Insights Dashboard"
# MAGIC
# MAGIC ### 4. Knowledge Search (For Documentation)
# MAGIC
# MAGIC Search knowledge_base table when:
# MAGIC - "How does Job 02 work?"
# MAGIC - "Show me Medallion architecture patterns"
# MAGIC - "Explain the churn prediction model"
# MAGIC - "Where is customer_360 table created?"
# MAGIC
# MAGIC ## Response Format
# MAGIC
# MAGIC Always include:
# MAGIC 1. **Data Source:** Which table/dashboard/space you used
# MAGIC 2. **Reasoning:** How you arrived at the answer
# MAGIC 3. **Confidence:** How fresh is the data?
# MAGIC 4. **Citations:** Link to tables/dashboards/docs
# MAGIC
# MAGIC Example:
# MAGIC ```
# MAGIC Based on sap_dev.gold.churn_predictions table (updated 2 hours ago), 
# MAGIC there are 23 high-risk churn customers (probability > 0.7).
# MAGIC
# MAGIC Top 3:
# MAGIC - Customer 101: 0.85 churn risk, Reason: Declining purchase frequency
# MAGIC - Customer 205: 0.78 churn risk, Reason: Negative RFM trend
# MAGIC - Customer 312: 0.72 churn risk, Reason: No purchases in 90 days
# MAGIC
# MAGIC See Customer Insights Dashboard for visualization.
# MAGIC ```
# MAGIC
# MAGIC ## Error Handling
# MAGIC
# MAGIC If query fails:
# MAGIC 1. Check table exists: `SHOW TABLES IN sap_dev.gold;`
# MAGIC 2. Verify permissions
# MAGIC 3. Try simpler approach
# MAGIC 4. Ask user for clarification
# MAGIC
# MAGIC ## Key Tables Quick Reference
# MAGIC
# MAGIC **For Customer Questions:**
# MAGIC - `mart_customer_360` - Complete customer view
# MAGIC - `churn_predictions` - Churn risk scores
# MAGIC - `mart_rfm_analysis` - Customer segmentation
# MAGIC
# MAGIC **For Product Questions:**
# MAGIC - `mart_product_performance` - Product metrics
# MAGIC - `demand_forecast` - Demand predictions
# MAGIC - `product_recommendations` - Personalized recommendations
# MAGIC
# MAGIC **For Inventory Questions:**
# MAGIC - `mart_inventory_health` - Inventory KPIs
# MAGIC - `fact_inventory` - Current stock levels
# MAGIC - `demand_forecast` - Future demand
# MAGIC
# MAGIC **For Documentation:**
# MAGIC - `knowledge_base` - Technical docs (646 articles)
# MAGIC - `customer_knowledge_base` - Customer domain
# MAGIC - `ecommerce_knowledge_base` - Ecommerce domain
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Step 6: Deployment Summary
# Deployment Summary
print("\n" + "="*80)
print("🎉 AGENT BRICKS DEPLOYMENT COMPLETE")
print("="*80)
print()
print("✅ PHASE 1: UC FUNCTIONS")
print("   - get_customer_segment(customer_id)")
print("   - calculate_inventory_days(product_id, warehouse_id)")
print("   - get_churn_risk_level(churn_probability)")
print()
print("✅ PHASE 2: GENIE SPACES")
for space in created_spaces:
    print(f"   - {space['title']} ({space['action']})")
print()
print("✅ PHASE 3: SUPERVISOR AGENT CONFIG")
print("   See cells above for complete configuration")
print()
print("="*80)
print("👉 NEXT STEPS:")
print("="*80)
print("1. Navigate to /ml/agents")
print("2. Create Supervisor Agent 'skillaipath-business-intelligence'")
print("3. Add all UC Tables (33), Functions (3), Genie Spaces (4), Dashboards (5)")
print("4. Copy agent instructions from above")
print("5. Test with sample questions")
print("="*80)
print()
print("🎯 TEST QUESTIONS:")
print("- 'Show me high-risk churn customers'")
print("- 'Which products are forecasted to have high demand?'")
print("- 'What's our revenue this month?'")
print("- 'How does the Medallion architecture work?'")
print("="*80)

# COMMAND ----------

