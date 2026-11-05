# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "faker",
#   "dbldatagen",
# ]
# ///
# DBTITLE 1,SkillAIPath Data Generator - Test Data for Pipeline Development
# MAGIC %md
# MAGIC # SkillAIPath Data Generator
# MAGIC
# MAGIC **Purpose:** Generate realistic Indian retail test data for pipeline development and testing
# MAGIC
# MAGIC ## Features
# MAGIC
# MAGIC **Data Generation Modes:**
# MAGIC - **Batch Mode**: Initial historical data load (configurable date range)
# MAGIC   - Generates Sales, Purchase, and Stock Status tables
# MAGIC - **Incremental Mode**: Daily delta loads + automatic CDC detection
# MAGIC   - Cell 12: Generates incremental sales (50% unchanged + 20% changed + 30% new)
# MAGIC   - Cell 13: Auto-detects CDC events (INSERT + UPDATE) from incremental data
# MAGIC
# MAGIC **Data Coverage:**
# MAGIC - Sales transactions (retail stores across India)
# MAGIC - Purchase orders (wholesale procurement)
# MAGIC - Stock status (warehouse inventory)
# MAGIC - Customer CDC events (profile changes)
# MAGIC - Product CDC events (catalog updates)
# MAGIC
# MAGIC **Validation:**
# MAGIC - Record count verification
# MAGIC - Data quality checks
# MAGIC - Schema validation
# MAGIC - Business rule validation (GST calculations, date ranges)
# MAGIC
# MAGIC ## Usage Pattern
# MAGIC
# MAGIC ```python
# MAGIC # Step 1: Configure parameters via widgets
# MAGIC # Step 2: Run batch generation for historical data
# MAGIC # Step 3: Run incremental generation for daily deltas
# MAGIC # Step 4: Validate generated data
# MAGIC # Step 5: Integrate with bronze layer notebooks
# MAGIC ```
# MAGIC
# MAGIC **Target Schemas:**
# MAGIC - Bronze: sap_dev.bronze (raw staging tables)
# MAGIC - Generated files can be loaded via Auto Loader patterns
# MAGIC - Compatible with Unity Catalog and Hive metastore

# COMMAND ----------

# DBTITLE 1,Load Config
# MAGIC %run /Workspace/Shared/dev/SkillAIPath/1_Canvas/1_Setup/1_Config

# COMMAND ----------

# DBTITLE 1,Load Centralized Config
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random
import uuid

# Widget Configuration
try:
    dbutils.widgets.dropdown("mode", "batch", ["batch", "incremental", "cdc"], "Generation Mode")
    dbutils.widgets.dropdown("run_source", "notebook", ["notebook", "workflow"], "Run Source")
    dbutils.widgets.text("BATCH_DAYS", "", "Batch Days (override, leave empty for 7-day default)")
    dbutils.widgets.text("incremental_date", "", "Incremental: Date (YYYY-MM-DD)")
    dbutils.widgets.text("records_per_day", "1000", "Records Per Day")
    dbutils.widgets.dropdown("write_mode", "overwrite", ["overwrite", "append"], "Write Mode")
    
    MODE = dbutils.widgets.get("mode")
    RUN_SOURCE = dbutils.widgets.get("run_source")
    BATCH_DAYS_PARAM = dbutils.widgets.get("BATCH_DAYS").strip()
    
    # BATCH_DAYS: Default to 7 days always, override only if parameter provided
    if BATCH_DAYS_PARAM and BATCH_DAYS_PARAM.isdigit():
        BATCH_DAYS = int(BATCH_DAYS_PARAM)
    else:
        BATCH_DAYS = 7  # Default: 7 days for both notebook and workflow
    
    INCREMENTAL_DATE = dbutils.widgets.get("incremental_date")
    RECORDS_PER_DAY = int(dbutils.widgets.get("records_per_day"))
    WRITE_MODE = dbutils.widgets.get("write_mode")
except:
    # Fallback defaults if widgets fail (should never happen)
    MODE = "batch"
    RUN_SOURCE = "notebook"
    BATCH_DAYS = 7  # Default: 7 days
    INCREMENTAL_DATE = ""
    RECORDS_PER_DAY = 1000
    WRITE_MODE = "overwrite"

print("="*70)
print("SKILLAIPPATH DATA GENERATOR - CONFIGURATION")
print("="*70)
print(f"Mode: {MODE}")
print(f"Run Source: {RUN_SOURCE} → Batch Days: {BATCH_DAYS}")
print(f"Records Per Day: {RECORDS_PER_DAY}")
print(f"Write Mode: {WRITE_MODE}")
print(f"Target Catalog: {Config.CATALOG}")
print(f"Payment Modes: {Config.PAYMENT_MODES}")
print(f"Tax Rates: CGST={Config.CGST_RATE}, SGST={Config.SGST_RATE}")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Indian Retail Reference Data - Cities, Products, Names
# Note: Customer names and cities are generated dynamically using Faker (en_IN locale)
# No need for hardcoded INDIAN_CITIES, FIRST_NAMES, LAST_NAMES lists

# Indian Product Names by Category
PRODUCT_CATALOG = {
    "Electronics": [
        "Samsung Galaxy A54", "OnePlus Nord 3", "Realme Narzo 60", "Redmi Note 12 Pro",
        "Sony Bravia 48 inch", "LG OLED TV", "HP Pavilion Laptop", "Dell Inspiron",
        "boAt Airdopes", "JBL Speaker", "Noise Smartwatch", "Fire TV Stick"
    ],
    "Clothing": [
        "Fabindia Kurta", "Allen Solly Shirt", "Peter England Trousers", "Manyavar Sherwani",
        "Biba Anarkali", "W for Women Kurti", "Levis Jeans", "Puma T-Shirt",
        "Jockey Innerwear", "VIP Suitcase", "Woodland Shoes", "Bata Sandals"
    ],
    "Groceries": [
        "Tata Tea Premium", "Amul Butter 500g", "Fortune Rice 25kg", "Aashirvaad Atta 10kg",
        "Britannia Marie Biscuit", "Parle-G Pack", "Haldiram Namkeen", "Dabur Honey",
        "Mother Dairy Milk 1L", "Kissan Ketchup", "Everest Masala", "Vim Bar"
    ],
    "Furniture": [
        "Godrej Almirah", "Nilkamal Chair", "Sleepwell Mattress", "Durian Sofa Set",
        "Urban Ladder Bookshelf", "Pepperfry Dining Table", "IKEA Study Desk",
        "Supreme Plastic Table", "Cello Storage Box", "Kurl-On Bed"
    ],
    "Toys": [
        "Funskool Jenga", "Mattel Hot Wheels", "Hasbro Monopoly India", "Lego Classic Set",
        "Fisher-Price Baby Toy", "Nerf Blaster", "Barbie Doll", "Remote Car"
    ],
    "Books": [
        "NCERT Class 10 Math", "Chetan Bhagat Novel", "RD Sharma Guide", "Panchatantra Stories",
        "Ramayana by Devdutt", "India After Gandhi", "Wings of Fire APJ", "Malgudi Days RK"
    ],
    "Sports": [
        "Nivia Football", "Cosco Cricket Bat", "Yonex Badminton", "Decathlon Cycle",
        "Adidas Running Shoes", "Nike Sports Tshirt", "Puma Gym Bag", "MRF Cricket Ball"
    ],
    "Beauty": [
        "Lakme Lipstick", "Himalaya Face Wash", "Dabur Vatika Shampoo", "Patanjali Aloe Gel",
        "Maybelline Kajal", "Lotus Sunscreen", "Garnier Face Cream", "Biotique Soap"
    ]
}

# Warehouse Names (20 warehouses across major Indian cities)
WAREHOUSES = [
    "Mumbai Central WH", "Delhi North WH", "Bangalore South WH", "Hyderabad East WH",
    "Chennai Port WH", "Kolkata WH", "Pune WH", "Ahmedabad WH",
    "Jaipur WH", "Lucknow WH", "Kanpur WH", "Nagpur WH",
    "Indore WH", "Bhopal WH", "Visakhapatnam WH", "Patna WH",
    "Vadodara WH", "Ludhiana WH", "Agra WH", "Nashik WH"
]

# Wholesaler Names
WHOLESALERS = [
    "Metro Cash & Carry", "Reliance Wholesale", "Walmart India B2B", "Udaan Marketplace",
    "Jumbotail Wholesale", "Best Price Modern Wholesale", "Star Bazaar Wholesale"
]

print("Reference Data Loaded:")
print(f"- Product Categories: {len(PRODUCT_CATALOG)}")
print(f"- Products: {sum(len(v) for v in PRODUCT_CATALOG.values())}")
print(f"- Warehouses: {len(WAREHOUSES)}")
print(f"- Wholesalers: {len(WHOLESALERS)}")
print("\nNote: Customer names and cities are generated dynamically using Faker")

# COMMAND ----------

# DBTITLE 1,Install Libraries
# MAGIC %pip install faker dbldatagen

# COMMAND ----------

# DBTITLE 1,Import dbldatagen and Faker
# Import dbldatagen and faker after installation
import dbldatagen as dg
from faker import Faker
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, when, rand, floor, explode, sequence, lit, concat, split, sum as _sum
import numpy as np

print("SUCCESS: dbldatagen and faker imported successfully")
print("  - Faker ready for Indian locale (en_IN)")

# COMMAND ----------

# DBTITLE 1,Set Random Seeds for Reproducibility
# ============================================================================
# SEED INITIALIZATION - For Reproducible Data Generation
# ============================================================================
# Set seeds for all random generators to ensure same data every run
# Seed value: 42 (standard ML/data science convention)
# ============================================================================

SEED_VALUE = 42

# Set seeds for all random libraries
Faker.seed(SEED_VALUE)           # Faker library (names, addresses, emails)
random.seed(SEED_VALUE)          # Python's built-in random module
np.random.seed(SEED_VALUE)       # NumPy random operations

print("="*70)
print("RANDOM SEED INITIALIZATION")
print("="*70)
print(f"✓ Faker.seed({SEED_VALUE})")
print(f"✓ random.seed({SEED_VALUE})")
print(f"✓ np.random.seed({SEED_VALUE})")
print()
print("Result: All data generation will be reproducible across runs")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Helper Functions - Random Data Generation
# NOTE: Most helper functions removed - now using faker and dbldatagen
# Only keeping this file for potential future use

print("NOTE: Helper functions cell (currently unused)")
print("  All data generation now handled by:")
print("  - Faker (en_IN) for Indian names, addresses, emails, contacts")
print("  - dbldatagen for structured data generation")
print("  - PRODUCT_UNIT_COST_MAPPING (Cell 7) for fixed product prices")

# COMMAND ----------

# DBTITLE 1,Shared Product Price Mapping
# ============================================================================
# SHARED PRODUCT UNIT COST MAPPING - FIXED PRICES FOR ALL 78 PRODUCTS
# ============================================================================
# This mapping ensures: sales.CostPrice == purchase.UnitCost for every product
# Each product has ONE fixed wholesale price (no random, no ranges)
# Used by BOTH Cell 8 (sales) and Cell 9 (purchase)
# ============================================================================

print("="*70)
print("BUILDING SHARED PRODUCT UNIT COST MAPPING")
print("="*70)
print("Each product has ONE fixed wholesale price (deterministic)")
print()

# FIXED wholesale unit cost for EVERY product (78 products total)
PRODUCT_UNIT_COST_MAPPING = {
    # Electronics (12 products) - High-value items
    "Samsung Galaxy A54": 18500,
    "OnePlus Nord 3": 22000,
    "Realme Narzo 60": 16000,
    "Redmi Note 12 Pro": 17500,
    "Sony Bravia 48 inch": 35000,
    "LG OLED TV": 45000,
    "HP Pavilion Laptop": 42000,
    "Dell Inspiron": 38000,
    "boAt Airdopes": 1800,
    "JBL Speaker": 3500,
    "Noise Smartwatch": 2500,
    "Fire TV Stick": 3000,
    
    # Clothing (12 products) - Mid-range items
    "Fabindia Kurta": 1200,
    "Allen Solly Shirt": 1500,
    "Peter England Trousers": 1800,
    "Manyavar Sherwani": 4500,
    "Biba Anarkali": 2200,
    "W for Women Kurti": 1000,
    "Levis Jeans": 2500,
    "Puma T-Shirt": 800,
    "Jockey Innerwear": 400,
    "VIP Suitcase": 3000,
    "Woodland Shoes": 2800,
    "Bata Sandals": 900,
    
    # Groceries (12 products) - Low-value essentials
    "Tata Tea Premium": 220,
    "Amul Butter 500g": 250,
    "Fortune Rice 25kg": 1400,
    "Aashirvaad Atta 10kg": 350,
    "Britannia Marie Biscuit": 40,
    "Parle-G Pack": 30,
    "Haldiram Namkeen": 150,
    "Dabur Honey": 180,
    "Mother Dairy Milk 1L": 60,
    "Kissan Ketchup": 120,
    "Everest Masala": 80,
    "Vim Bar": 25,
    
    # Furniture (10 products) - High-value home items
    "Godrej Almirah": 12000,
    "Nilkamal Chair": 1500,
    "Sleepwell Mattress": 8000,
    "Durian Sofa Set": 18000,
    "Urban Ladder Bookshelf": 5000,
    "Pepperfry Dining Table": 9000,
    "IKEA Study Desk": 4500,
    "Supreme Plastic Table": 800,
    "Cello Storage Box": 600,
    "Kurl-On Bed": 7000,
    
    # Toys (8 products) - Mid-range kids items
    "Funskool Jenga": 600,
    "Mattel Hot Wheels": 400,
    "Hasbro Monopoly India": 1200,
    "Lego Classic Set": 2500,
    "Fisher-Price Baby Toy": 1500,
    "Nerf Blaster": 1800,
    "Barbie Doll": 1000,
    "Remote Car": 1400,
    
    # Books (8 products) - Low to mid-range educational/leisure
    "NCERT Class 10 Math": 250,
    "Chetan Bhagat Novel": 180,
    "RD Sharma Guide": 400,
    "Panchatantra Stories": 150,
    "Ramayana by Devdutt": 350,
    "India After Gandhi": 500,
    "Wings of Fire APJ": 200,
    "Malgudi Days RK": 180,
    
    # Sports (8 products) - Mid to high-range sports gear
    "Nivia Football": 800,
    "Cosco Cricket Bat": 2200,
    "Yonex Badminton": 1500,
    "Decathlon Cycle": 6000,
    "Adidas Running Shoes": 3500,
    "Nike Sports Tshirt": 1200,
    "Puma Gym Bag": 1000,
    "MRF Cricket Ball": 600,
    
    # Beauty (8 products) - Low to mid-range cosmetics
    "Lakme Lipstick": 400,
    "Himalaya Face Wash": 150,
    "Dabur Vatika Shampoo": 220,
    "Patanjali Aloe Gel": 180,
    "Maybelline Kajal": 200,
    "Lotus Sunscreen": 350,
    "Garnier Face Cream": 280,
    "Biotique Soap": 100
}

print(f"✓ Fixed wholesale unit costs assigned to ALL {len(PRODUCT_UNIT_COST_MAPPING)} products")
print(f"✓ Price range: ₹{min(PRODUCT_UNIT_COST_MAPPING.values()):.2f} - ₹{max(PRODUCT_UNIT_COST_MAPPING.values()):.2f}")
print()
print("Usage in downstream cells:")
print("  - Cell 8 (Sales): CostPrice = PRODUCT_UNIT_COST_MAPPING[product]")
print("  - Cell 9 (Purchase): UnitCost = PRODUCT_UNIT_COST_MAPPING[product]")
print("  - Cell 8 (Sales): SellingPrice = CostPrice × 1.45 (45% markup)")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Generate Batch Data - Sales Transactions
# Complete rewrite using dbldatagen + faker
# Generates ALL required columns with proper Indian data

if MODE == "batch":
    print("="*70)
    print("GENERATING SALES DATA USING DBLDATAGEN + FAKER")
    print("="*70)
    
    # Re-initialize seeds for reproducibility
    Faker.seed(SEED_VALUE)
    random.seed(SEED_VALUE)
    np.random.seed(SEED_VALUE)
    
    # Create bronze schema
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {Config.CATALOG}.{Config.SCHEMA_BRONZE}")
    print(f"SUCCESS: Schema ready: {Config.CATALOG}.{Config.SCHEMA_BRONZE}")
    
    # Initialize Faker for Indian data
    fake = Faker("en_IN")
    
    # Calculate total rows from widget parameters
    TOTAL_ROWS = BATCH_DAYS * RECORDS_PER_DAY
    print(f"\n📊 Target: {TOTAL_ROWS:,} rows ({BATCH_DAYS} days × {RECORDS_PER_DAY} records/day)")
    
    # STEP 1: Generate Faker Arrays
    print("\n🔧 Step 1: Generating faker arrays...")
    
    customer_cnt = 25000
    customer_names = [fake.unique.name() for _ in range(customer_cnt)]
    customer_ages = [fake.random_int(min=18, max=75) for _ in range(customer_cnt)]
    customer_addresses = [fake.address() for _ in range(customer_cnt)]
    customer_contacts = [str(fake.random_int(min=6000000000, max=9999999999)) for _ in range(customer_cnt)]
    customer_mails = [fake.email() for _ in range(customer_cnt)]
    
    # Gender based on name heuristic
    customer_genders = []
    for name in customer_names:
        first_name = name.split()[0]
        if first_name[-1].lower() in ['a', 'i', 'e', 'y']:
            customer_genders.append("Female")
        else:
            customer_genders.append("Male")
    
    # Warehouse mapping (20 warehouses across India)
    warehouse_mapping = {
        "Mumbai Central WH": "Mumbai",
        "Delhi North WH": "Delhi",
        "Bangalore South WH": "Bangalore",
        "Hyderabad East WH": "Hyderabad",
        "Chennai Port WH": "Chennai",
        "Kolkata WH": "Kolkata",
        "Pune WH": "Pune",
        "Ahmedabad WH": "Ahmedabad",
        "Jaipur WH": "Jaipur",
        "Lucknow WH": "Lucknow",
        "Kanpur WH": "Kanpur",
        "Nagpur WH": "Nagpur",
        "Indore WH": "Indore",
        "Bhopal WH": "Bhopal",
        "Visakhapatnam WH": "Visakhapatnam",
        "Patna WH": "Patna",
        "Vadodara WH": "Vadodara",
        "Ludhiana WH": "Ludhiana",
        "Agra WH": "Agra",
        "Nashik WH": "Nashik"
    }
    warehouse_names = list(warehouse_mapping.keys())
    warehouse_cities = [warehouse_mapping[n] for n in warehouse_names]
    
    # Agents
    agent_cnt = 250
    agent_names = [fake.name() for _ in range(agent_cnt)]
    agent_cities = [fake.city() for _ in range(agent_cnt)]
    agent_contacts = [str(fake.random_int(min=6000000000, max=9999999999)) for _ in range(agent_cnt)]
    agent_mails = [fake.email() for _ in range(agent_cnt)]
    
    # Retailers
    retailer_cnt = 150
    retailer_names = [f"{fake.company()} Store" for _ in range(retailer_cnt)]
    retailer_cities = [fake.city() for _ in range(retailer_cnt)]
    retailer_contacts = [str(fake.random_int(min=6000000000, max=9999999999)) for _ in range(retailer_cnt)]
    retailer_mails = [fake.email() for _ in range(retailer_cnt)]
    
    print(f"  - Customers: {customer_cnt}")
    print(f"  - Warehouses: {len(warehouse_names)}")
    print(f"  - Agents: {agent_cnt}")
    print(f"  - Retailers: {retailer_cnt}")
    
    # STEP 2: Use Shared Product Price Mapping (from Cell 7)
    print("\n🔧 Step 2: Using shared product price mapping (from Cell 7)...")
    
    # Calculate date range
    start_date = (datetime.now() - timedelta(days=BATCH_DAYS)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")
    
    # Get all products from PRODUCT_CATALOG
    all_products = []
    product_category_map = {}
    
    for category, products in PRODUCT_CATALOG.items():
        for product in products:
            all_products.append(product)
            product_category_map[product] = category
    
    print(f"  - Loaded {len(all_products)} products")
    
    # FIX: Use PRODUCT_UNIT_COST_MAPPING from Cell 7 (shared with purchase table)
    # This ensures: sales.CostPrice == purchase.UnitCost for every product
    # FIX: Calculate SellingPrice = CostPrice × 1.45 (45% fixed markup)
    
    cost_price_case = "CASE "
    selling_price_case = "CASE "
    
    for product in all_products:
        safe_product = product.replace("'", "\\'")  # Escape quotes
        unit_cost = PRODUCT_UNIT_COST_MAPPING[product]  # From Cell 7
        selling_price = round(unit_cost * 1.45, 2)      # 45% markup
        
        cost_price_case += f"WHEN ProductName = '{safe_product}' THEN {unit_cost} "
        selling_price_case += f"WHEN ProductName = '{safe_product}' THEN {selling_price} "
    
    cost_price_case += "ELSE 100 END"
    selling_price_case += "ELSE 145 END"
    
    print("  - Price expressions built from shared mapping (Cell 7)")
    print("  - CostPrice = PRODUCT_UNIT_COST_MAPPING[product]")
    print("  - SellingPrice = CostPrice × 1.45 (45% markup)")
    
    # STEP 3: Build Base DataFrame with dbldatagen
    print("\n🔧 Step 3: Building base dataframe...")
    
    df_base = (
        dg.DataGenerator(spark, name="sap_sales", rows=TOTAL_ROWS, partitions=4)
        .withColumn(
            "internal_device_id", "long",
            minValue=0x1000000000000,
            uniqueValues=100000,
            omit=True,
            baseColumnType="hash"
        )
        .withColumn("ProductName", StringType(), values=all_products, baseColumn="internal_device_id")
        .withColumn("ProductCategory", StringType(), expr="CASE " + " ".join([f"WHEN ProductName = '{p}' THEN '{c}'" for p, c in product_category_map.items()]) + " ELSE 'Other' END")
        .withColumn("OrderDate", "Date", begin=start_date, end=end_date, random=True)
        .withColumn("MFDDate", "Date", expr="date_sub(OrderDate, int(rand() * 365) + 30)")
        .withColumn("EXPDate", "Date", expr="date_add(OrderDate, int(rand() * 730) + 180)")
        .withColumn("DeliveryDate", "Date", expr="date_add(OrderDate, int(rand() * 7) + 1)")
        # Fixed prices per product (from mapping)
        .withColumn("CostPrice", "double", expr=cost_price_case)
        .withColumn("SellingPrice", "double", expr=selling_price_case)
        .withColumn("DiscountPercentage", "double", minValue=0, maxValue=25, random=True)
        .withColumn("Discount", "double", expr="ROUND(SellingPrice * (DiscountPercentage / 100), 2)")
        # Amount after discount (before tax)
        .withColumn("AmountAfterDiscount", "double", expr="ROUND(SellingPrice - Discount, 2)")
        .withColumn("ProductQuantity", "integer", minValue=1, maxValue=10, random=True)
        .withColumn("WareHouseName", StringType(), values=warehouse_names, baseColumn="internal_device_id")
        .withColumn("WareHouseCity", StringType(), values=warehouse_cities, baseColumn="internal_device_id")
        .withColumn("PaymentMode", StringType(), values=Config.PAYMENT_MODES, random=True)
        .withColumn("AgentName", StringType(), values=agent_names, baseColumn="internal_device_id")
        .withColumn("AgentCity", StringType(), values=agent_cities, baseColumn="internal_device_id")
        .withColumn("AgentContact", StringType(), values=agent_contacts, baseColumn="internal_device_id")
        .withColumn("AgentMail", StringType(), values=agent_mails, baseColumn="internal_device_id")
        .withColumn("RetailerName", StringType(), values=retailer_names, baseColumn="internal_device_id")
        .withColumn("RetailerCity", StringType(), values=retailer_cities, baseColumn="internal_device_id")
        .withColumn("RetailerContact", StringType(), values=retailer_contacts, baseColumn="internal_device_id")
        .withColumn("RetailerMail", StringType(), values=retailer_mails, baseColumn="internal_device_id")
    ).build()
    
    print("  - Base dataframe built")
    
    # STEP 4: Add Customer Repeat Logic
    print("\n🔧 Step 4: Adding customer repeat logic...")
    
    # Dynamic segmentation based on customer_cnt
    # 30% OneTime, 20% Light, 20% Medium, 20% Heavy, 10% VeryHeavy
    onetime_threshold = int(customer_cnt * 0.30)      # 30% = 7,500
    light_threshold = int(customer_cnt * 0.50)        # +20% = 12,500
    medium_threshold = int(customer_cnt * 0.70)       # +20% = 17,500
    heavy_threshold = int(customer_cnt * 0.90)        # +20% = 22,500
    # Remaining 10% = VeryHeavy = 2,500
    
    cust_df = spark.range(1, customer_cnt + 1).toDF("CustomerID")
    cust_df = cust_df.withColumn(
        "CustomerSegment",
        when(col("CustomerID") <= onetime_threshold, "OneTime")
        .when(col("CustomerID") <= light_threshold, "Light")
        .when(col("CustomerID") <= medium_threshold, "Medium")
        .when(col("CustomerID") <= heavy_threshold, "Heavy")
        .otherwise("VeryHeavy")
    )
    
    cust_df = cust_df.withColumn(
        "OrderCount",
        when(col("CustomerSegment") == "OneTime", lit(1))
        .when(col("CustomerSegment") == "Light", floor(rand()*9) + 2)        # 2-10 orders
        .when(col("CustomerSegment") == "Medium", floor(rand()*90) + 11)     # 11-100 orders
        .when(col("CustomerSegment") == "Heavy", floor(rand()*900) + 101)    # 101-1000 orders
        .otherwise(floor(rand()*1000) + 1001)                                  # 1001-2000 orders
    )
    
    print(f"  ✓ Segmentation thresholds (dynamic):")
    print(f"    - OneTime (30%): CustomerID 1-{onetime_threshold:,} = 1 order each")
    print(f"    - Light (20%): CustomerID {onetime_threshold+1:,}-{light_threshold:,} = 2-10 orders")
    print(f"    - Medium (20%): CustomerID {light_threshold+1:,}-{medium_threshold:,} = 11-100 orders")
    print(f"    - Heavy (20%): CustomerID {medium_threshold+1:,}-{heavy_threshold:,} = 101-1,000 orders")
    print(f"    - VeryHeavy (10%): CustomerID {heavy_threshold+1:,}-{customer_cnt:,} = 1,001-2,000 orders")
    
    # Scale to match TOTAL_ROWS from widget parameters
    natural_total = cust_df.select(_sum("OrderCount")).first()[0]
    scale_factor = TOTAL_ROWS / natural_total
    print(f"  ✓ Natural total: {natural_total:,} orders")
    print(f"  ✓ Scaling by {scale_factor:.4f} to reach target: {TOTAL_ROWS:,}")
    
    cust_df = cust_df.withColumn("OrderCount", floor(col("OrderCount") * lit(scale_factor)))
    
    # Enforce minimums per segment (scaled proportionally if scale_factor < 1)
    if scale_factor < 1:
        # Aggressive scale down - use scaled minimums
        min_onetime = 1
        min_light = max(1, int(2 * scale_factor))
        min_medium = max(2, int(11 * scale_factor))
        min_heavy = max(3, int(101 * scale_factor))
        min_veryheavy = max(5, int(1001 * scale_factor))
        print(f"  ✓ Scaled minimums: OneTime=1, Light={min_light}, Medium={min_medium}, Heavy={min_heavy}, VeryHeavy={min_veryheavy}")
    else:
        # Scale up or maintain - use original minimums
        min_onetime = 1
        min_light = 2
        min_medium = 11
        min_heavy = 101
        min_veryheavy = 1001
    
    cust_df = cust_df.withColumn(
        "OrderCount",
        when(col("CustomerSegment") == "OneTime", min_onetime)
        .when((col("CustomerSegment") == "Light") & (col("OrderCount") < min_light), min_light)
        .when((col("CustomerSegment") == "Medium") & (col("OrderCount") < min_medium), min_medium)
        .when((col("CustomerSegment") == "Heavy") & (col("OrderCount") < min_heavy), min_heavy)
        .when((col("CustomerSegment") == "VeryHeavy") & (col("OrderCount") < min_veryheavy), min_veryheavy)
        .otherwise(col("OrderCount"))
    )
    
    # Adjust to exact TOTAL_ROWS by distributing delta across customers
    current_total = cust_df.select(_sum("OrderCount")).first()[0]
    delta = TOTAL_ROWS - current_total
    print(f"  ✓ After scaling: {current_total:,} orders (delta: {delta:,})")
    
    if delta != 0:
        # Distribute delta across customers proportionally
        # Get customers we can adjust (not OneTime, have room to adjust)
        cust_df = cust_df.withColumn("_row_num", row_number().over(Window.orderBy(rand())))
        
        if abs(delta) < 100000:  # Small delta - distribute 1 order at a time
            cust_df = cust_df.withColumn(
                "OrderCount",
                when(
                    (col("CustomerSegment") != "OneTime") & 
                    (col("_row_num") <= abs(delta)),
                    col("OrderCount") + (1 if delta > 0 else -1)
                ).otherwise(col("OrderCount"))
            )
        else:  # Large delta - distribute proportionally
            adjustable_customers = 24000  # All except OneTime (7500)
            orders_per_customer = int(abs(delta) / adjustable_customers)
            remainder = abs(delta) % adjustable_customers
            
            cust_df = cust_df.withColumn(
                "OrderCount",
                when(
                    col("CustomerSegment") != "OneTime",
                    col("OrderCount") + 
                    (orders_per_customer if delta > 0 else -orders_per_customer) +
                    when(col("_row_num") <= remainder, 1 if delta > 0 else -1).otherwise(0)
                ).otherwise(col("OrderCount"))
            )
        
        cust_df = cust_df.drop("_row_num")
        final_total = cust_df.select(_sum("OrderCount")).first()[0]
        print(f"  ✓ Final total: {final_total:,} orders (target: {TOTAL_ROWS:,})")
    
    # Explode to individual orders
    cust_orders = cust_df.withColumn(
        "seq",
        explode(sequence(lit(1), col("OrderCount")))
    ).drop("seq", "OrderCount", "CustomerSegment")
    
    # Join with base sales
    df_base = df_base.withColumn("row_id", row_number().over(Window.orderBy(rand())))
    cust_orders = cust_orders.withColumn("row_id", row_number().over(Window.orderBy(rand())))
    df_sales = df_base.join(cust_orders.select("row_id", "CustomerID"), "row_id").drop("row_id")
    
    print("  ✓ Customer assignments complete")
    
    # STEP 5: Add Stable Customer Dimensions
    print("\n🔧 Step 5: Adding customer dimensions...")
    
    cust_master = spark.range(1, customer_cnt + 1).toDF("CustomerID")
    
    name_df = spark.createDataFrame(
        [(i+1, customer_names[i]) for i in range(customer_cnt)],
        ["CustomerID", "CustomerName"]
    )
    age_df = spark.createDataFrame(
        [(i+1, customer_ages[i]) for i in range(customer_cnt)],
        ["CustomerID", "CustomerAge"]
    )
    gender_df = spark.createDataFrame(
        [(i+1, customer_genders[i]) for i in range(customer_cnt)],
        ["CustomerID", "CustomerGender"]
    )
    addr_df = spark.createDataFrame(
        [(i+1, customer_addresses[i]) for i in range(customer_cnt)],
        ["CustomerID", "CustomerAddress"]
    )
    contact_df = spark.createDataFrame(
        [(i+1, customer_contacts[i]) for i in range(customer_cnt)],
        ["CustomerID", "CustomerContact"]
    )
    mail_df = spark.createDataFrame(
        [(i+1, customer_mails[i]) for i in range(customer_cnt)],
        ["CustomerID", "CustomerMail"]
    )
    
    cust_master = (
        cust_master
        .join(name_df, "CustomerID")
        .join(age_df, "CustomerID")
        .join(gender_df, "CustomerID")
        .join(addr_df, "CustomerID")
        .join(contact_df, "CustomerID")
        .join(mail_df, "CustomerID")
    )
    
    df_sales = df_sales.join(cust_master, "CustomerID")
    
    print("  ✓ Customer dimensions joined")
    
    # STEP 6: Add Tax and Metadata
    print("\n🔧 Step 6: Adding CGST, SGST, finalPrice, metadata...")
    
    # Get max existing OrderID for globally unique IDs across batches
    try:
        max_order_id = spark.sql(f"SELECT COALESCE(MAX(OrderID), 0) FROM {Config.TBL_STG_SALES}").collect()[0][0]
    except:
        max_order_id = 0
    print(f"  ✓ Max existing OrderID: {max_order_id} (new IDs start at {max_order_id + 1})")
    
    df_sales = (
        df_sales
        # Tax calculated on amount after discount
        .withColumn("CGST", F.round(col("AmountAfterDiscount") * lit(Config.CGST_RATE), 2))
        .withColumn("SGST", F.round(col("AmountAfterDiscount") * lit(Config.SGST_RATE), 2))
        # finalPrice = Amount after discount + CGST + SGST (per unit)
        .withColumn("finalPrice", F.round(col("AmountAfterDiscount") + col("CGST") + col("SGST"), 2))
        # TotalAmount = finalPrice × ProductQuantity (total order value)
        .withColumn("TotalAmount", F.round(col("finalPrice") * col("ProductQuantity"), 2))
        .withColumn("BatchDate", col("OrderDate"))
        .withColumn("LoadedAt", lit(datetime.now()))
        .withColumn("OrderID", (row_number().over(Window.orderBy(lit(1))) + lit(max_order_id)).cast("int"))
        .drop("AmountAfterDiscount")  # Drop temporary column
    )
    
    print("  ✓ Tax columns added and finalPrice calculated (includes CGST + SGST)")
    
    # STEP 7: Final Column Order
    print("\n🔧 Step 7: Ordering columns...")
    
    final_columns = [
        "OrderID", "CustomerID", "CustomerName", "CustomerAge", "CustomerGender", "CustomerAddress", "CustomerContact", "CustomerMail",
        "ProductName", "ProductCategory",
        "MFDDate", "EXPDate", "OrderDate", "DeliveryDate",
        "CostPrice", "SellingPrice", "DiscountPercentage", "Discount", "CGST", "SGST", "finalPrice", "ProductQuantity", "TotalAmount",
        "WareHouseName", "WareHouseCity",
        "PaymentMode",
        "AgentName", "AgentCity", "AgentContact", "AgentMail",
        "RetailerName", "RetailerCity", "RetailerContact", "RetailerMail",
        "BatchDate", "LoadedAt"
    ]
    
    df_sales = df_sales.select(*final_columns)
    
    print("  ✓ Columns ordered")
    
    # STEP 8: Write to Table
    print("\n🔧 Step 8: Writing to Unity Catalog...")
    
    # Delete-first for idempotency when using append mode (prevents duplicate BatchDates)
    if WRITE_MODE == "append":
        print(f"  🗑️  Deleting existing data for BatchDate range {start_date} to {end_date}...")
        try:
            spark.sql(f"""
                DELETE FROM {Config.TBL_STG_SALES}
                WHERE BatchDate BETWEEN '{start_date}' AND '{end_date}'
            """)
            print(f"  ✓ Deleted existing data for overlapping dates (idempotent)")
        except Exception as e:
            print(f"  ℹ️  Table may not exist yet (first run): {e}")
    
    df_sales.write.mode(WRITE_MODE).option("overwriteSchema", "true").saveAsTable(Config.TBL_STG_SALES)
    
    record_count = df_sales.count()
    
    print("\n" + "="*70)
    print("✅ SALES DATA GENERATION COMPLETE")
    print("="*70)
    print(f"📊 Records Generated: {record_count:,}")
    print(f"📅 Date Range: {start_date} to {end_date}")
    print(f"👥 Unique Customers: {customer_cnt:,}")
    print(f"📦 Products: {len(all_products)}")
    print(f"🏢 Warehouses: {len(warehouse_names)}")
    print(f"🎯 Table: {Config.TBL_STG_SALES}")
    print("="*70)
    
    print("\n📋 Sample Data (first 10 rows):")
    display(df_sales.limit(10))
    
else:
    print("⏭️  Skipping batch sales generation (mode != batch)")

# COMMAND ----------

# DBTITLE 1,Generate Batch Data - Purchase Orders
# Complete rewrite using dbldatagen + faker
# Purchase orders link retailers with wholesalers

if MODE == "batch":
    print("="*70)
    print("GENERATING PURCHASE DATA USING DBLDATAGEN + FAKER")
    print("="*70)
    
    # Re-initialize seeds for reproducibility
    Faker.seed(SEED_VALUE)
    random.seed(SEED_VALUE)
    np.random.seed(SEED_VALUE)
    
    # Ensure bronze schema exists
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {Config.CATALOG}.{Config.SCHEMA_BRONZE}")
    print(f"✓ Schema ready: {Config.CATALOG}.{Config.SCHEMA_BRONZE}")
    
    # Initialize Faker
    fake = Faker("en_IN")
    
    # Calculate total rows (fewer purchases than sales - wholesale procurement)
    TOTAL_PURCHASE_ROWS = (BATCH_DAYS * RECORDS_PER_DAY) // 10
    print(f"\n📊 Target: {TOTAL_PURCHASE_ROWS:,} purchase orders (10% of sales volume)")
    
    # FIX: Use WHOLESALERS from reference data (Cell 5), not generated names
    # FIX: Use WareHouseNames from stg_sales table
    # STEP 1: Load Wholesalers and Warehouses
    print("\n🔧 Step 1: Loading wholesalers and warehouses...")
    print(f"  ✓ Wholesalers from Cell 5: {len(WHOLESALERS)}")
    
    # Get warehouse names from sales table
    try:
        warehouse_df = spark.sql(f"SELECT DISTINCT WareHouseName FROM {Config.TBL_STG_SALES}")
        warehouse_list = [r["WareHouseName"] for r in warehouse_df.collect()]
        print(f"  ✓ Warehouses from stg_sales: {len(warehouse_list)}")
    except Exception as e:
        print(f"  ⚠️  Sales table not found, using default warehouses")
        warehouse_list = warehouse_names  # Fallback to Cell 7's warehouse list
    
    # STEP 2: Product Unit Cost Mapping (Wholesale Prices)
    print("\n🔧 Step 2: Building product wholesale unit cost mapping...")
    
    # Get all products from PRODUCT_CATALOG
    all_products = []
    for category, products in PRODUCT_CATALOG.items():
        all_products.extend(products)
    
    # FIX: Use PRODUCT_UNIT_COST_MAPPING from Cell 7 (shared with sales table)
    # This ensures: purchase.UnitCost == sales.CostPrice for every product
    
    print(f"  ✓ Using PRODUCT_UNIT_COST_MAPPING from Cell 7")
    print(f"  ✓ This ensures: purchase.UnitCost = sales.CostPrice")
    
    # STEP 3: Build Purchase DataFrame
    print("\n🔧 Step 3: Building purchase dataframe with dbldatagen...")
    
    # Calculate date range
    start_date = (datetime.now() - timedelta(days=BATCH_DAYS)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")
    
    # Build UnitCost CASE expression using shared mapping from Cell 7
    unit_cost_case_expr = "CASE "
    for product in all_products:
        safe_product = product.replace("'", "\\'")
        unit_cost = PRODUCT_UNIT_COST_MAPPING[product]  # From Cell 7
        unit_cost_case_expr += f"WHEN ProductName = '{safe_product}' THEN {unit_cost} "
    unit_cost_case_expr += "ELSE 100 END"
    
    # FIX: Build dataframe with proper purchase structure
    df_purchase = (
        dg.DataGenerator(spark, name="sap_purchase", rows=TOTAL_PURCHASE_ROWS, partitions=4)
        .withColumn(
            "internal_device_id", "long",
            minValue=0x1000000000000,
            uniqueValues=100000,
            omit=True,
            baseColumnType="hash"
        )
        # Product
        .withColumn("ProductName", StringType(), values=all_products, baseColumn="internal_device_id")
        
        # FIX: Renamed Price to UnitCost
        .withColumn("UnitCost", "double", expr=unit_cost_case_expr)
        
        # Quantity (bulk orders 80-150 units)
        .withColumn("Quantity", "integer", minValue=80, maxValue=150, random=True)
        
        # FIX: WholeSalerName from WHOLESALERS reference data
        .withColumn("WholeSalerName", StringType(), values=WHOLESALERS, baseColumn="internal_device_id")
        
        # FIX: WareHouseName from stg_sales (wholesaler sells TO warehouse)
        .withColumn("WareHouseName", StringType(), values=warehouse_list, baseColumn="internal_device_id")
        
        # Purchase Date - dynamic date range
        .withColumn("PurchaseDate", "Date", begin=start_date, end=end_date, random=True)
        
        # Metadata
        .withColumn("BatchDate", "Date", expr="PurchaseDate")
        .withColumn("LoadedAt", "timestamp", expr="current_timestamp()")
    ).build()
    
    print("  ✓ Base purchase dataframe built")
    
    # STEP 4: Add Calculated Columns
    print("\n🔧 Step 4: Adding calculated columns (PurchaseID, GrossAmount, GSTAmount, NetAmount)...")
    
    # Get max existing PurchaseID for globally unique IDs across batches
    try:
        max_purchase_id = spark.sql(f"SELECT COALESCE(MAX(PurchaseID), 0) FROM {Config.TBL_STG_PURCHASE}").collect()[0][0]
    except:
        max_purchase_id = 0
    print(f"  ✓ Max existing PurchaseID: {max_purchase_id} (new IDs start at {max_purchase_id + 1})")
    
    # FIX: Add PurchaseID as sequential number (globally unique across batches)
    df_purchase = df_purchase.withColumn("PurchaseID", (row_number().over(Window.orderBy(lit(1))) + lit(max_purchase_id)).cast("int"))
    
    # FIX: Add GrossAmount = UnitCost * Quantity
    df_purchase = df_purchase.withColumn("GrossAmount", F.round(col("UnitCost") * col("Quantity"), 2))
    
    # FIX: Add GSTAmount = 18% of GrossAmount (CGST 9% + SGST 9%)
    df_purchase = df_purchase.withColumn(
        "GSTAmount", 
        F.round(col("GrossAmount") * lit(Config.CGST_RATE + Config.SGST_RATE), 2)
    )
    
    # FIX: Add NetAmount = GrossAmount + GSTAmount
    df_purchase = df_purchase.withColumn("NetAmount", F.round(col("GrossAmount") + col("GSTAmount"), 2))
    
    print("  ✓ Calculated columns added")
    
    # STEP 5: Final Column Order
    print("\n🔧 Step 5: Ordering columns...")
    
    # FIX: Final column order (no retailer info, no wholesaler address/contact)
    # FIX: Added WareHouseName (wholesaler → warehouse)
    final_columns = [
        "PurchaseID",
        "ProductName",
        "UnitCost",
        "Quantity",
        "GrossAmount",
        "GSTAmount",
        "NetAmount",
        "WholeSalerName",
        "WareHouseName",
        "PurchaseDate",
        "BatchDate",
        "LoadedAt"
    ]
    
    df_purchase = df_purchase.select(*final_columns)
    
    print("  ✓ Columns ordered")
    
    # STEP 6: Write to Table
    print("\n🔧 Step 6: Writing to Unity Catalog...")
    
    # Delete-first for idempotency when using append mode (prevents duplicate BatchDates)
    if WRITE_MODE == "append":
        print(f"  🗑️  Deleting existing data for BatchDate range {start_date} to {end_date}...")
        try:
            spark.sql(f"""
                DELETE FROM {Config.TBL_STG_PURCHASE}
                WHERE BatchDate BETWEEN '{start_date}' AND '{end_date}'
            """)
            print(f"  ✓ Deleted existing data for overlapping dates (idempotent)")
        except Exception as e:
            print(f"  ℹ️  Table may not exist yet (first run): {e}")
    
    df_purchase.write.mode(WRITE_MODE).option("overwriteSchema", "true").saveAsTable(Config.TBL_STG_PURCHASE)
    
    record_count = df_purchase.count()
    
    print("\n" + "="*70)
    print("✅ PURCHASE DATA GENERATION COMPLETE")
    print("="*70)
    print(f"📊 Records Generated: {record_count:,}")
    print(f"📅 Date Range: {start_date} to {end_date}")
    print(f"🏭 Wholesalers: {len(WHOLESALERS)}")
    print(f"🏢 Warehouses: {len(warehouse_list)}")
    print(f"📦 Products: {len(all_products)}")
    print(f"🎯 Table: {Config.TBL_STG_PURCHASE}")
    print("📋 Columns: PurchaseID, ProductName, UnitCost, Quantity, GrossAmount, GSTAmount, NetAmount, WholeSalerName, WareHouseName, PurchaseDate, BatchDate, LoadedAt")
    print("="*70)
    
    print("\n📋 Sample Data (first 10 rows):")
    display(df_purchase.limit(10))
    
else:
    print("⏭️  Skipping batch purchase generation (mode != batch)")

# COMMAND ----------

# DBTITLE 1,Generate Batch Data - Stock Status
# Complete rewrite using dbldatagen
# Stock Status: Daily inventory snapshots for each product-warehouse combination

if MODE == "batch":
    print("="*70)
    print("GENERATING STOCK STATUS DATA USING CARTESIAN PRODUCT")
    print("(FIXED: Guarantees unique Product-Warehouse-Date combinations)")
    print("="*70)
    
    # Re-initialize seeds for reproducibility
    Faker.seed(SEED_VALUE)
    random.seed(SEED_VALUE)
    np.random.seed(SEED_VALUE)
    
    # Import required functions for Cartesian product approach
    from pyspark.sql.functions import explode, sequence, to_date, rand, current_timestamp
    
    # Ensure bronze schema exists
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {Config.CATALOG}.{Config.SCHEMA_BRONZE}")
    print(f"✓ Schema ready: {Config.CATALOG}.{Config.SCHEMA_BRONZE}")
    
    # STEP 1: Get Products and Warehouses
    print("\n🔧 Step 1: Loading products and warehouses...")
    
    # Get all products from PRODUCT_CATALOG (same as sales/purchase)
    all_products = []
    for category, products in PRODUCT_CATALOG.items():
        all_products.extend(products)
    
    # Get all warehouses (same as sales/purchase)
    warehouse_mapping = {
        "Mumbai Central WH": "Mumbai",
        "Delhi North WH": "Delhi",
        "Bangalore South WH": "Bangalore",
        "Hyderabad East WH": "Hyderabad",
        "Chennai Port WH": "Chennai",
        "Kolkata WH": "Kolkata",
        "Pune WH": "Pune",
        "Ahmedabad WH": "Ahmedabad",
        "Jaipur WH": "Jaipur",
        "Lucknow WH": "Lucknow",
        "Kanpur WH": "Kanpur",
        "Nagpur WH": "Nagpur",
        "Indore WH": "Indore",
        "Bhopal WH": "Bhopal",
        "Visakhapatnam WH": "Visakhapatnam",
        "Patna WH": "Patna",
        "Vadodara WH": "Vadodara",
        "Ludhiana WH": "Ludhiana",
        "Agra WH": "Agra",
        "Nashik WH": "Nashik"
    }
    warehouse_names = list(warehouse_mapping.keys())
    
    print(f"  ✓ Products: {len(all_products)}")
    print(f"  ✓ Warehouses: {len(warehouse_names)}")
    
    # STEP 2: Calculate Date Range
    print("\n🔧 Step 2: Calculating date range...")
    
    # Generate daily snapshots for the entire batch period
    start_date = (datetime.now() - timedelta(days=BATCH_DAYS)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")
    
    # Total rows = products × warehouses × days (daily snapshot per product-warehouse combo)
    TOTAL_STOCK_ROWS = len(all_products) * len(warehouse_names) * BATCH_DAYS
    
    print(f"  ✓ Date range: {start_date} to {end_date} ({BATCH_DAYS} days)")
    print(f"  ✓ Target rows: {TOTAL_STOCK_ROWS:,} (78 products × 20 warehouses × {BATCH_DAYS} days)")
    
    # STEP 3: Build Stock DataFrame using Cartesian Product (FIXED)
    print("\n🔧 Step 3: Building stock dataframe using Cartesian Product...")
    print("  ℹ️  This ensures EXACTLY one record per Product-Warehouse-Date combination")
    
    # Create Products DataFrame
    products_df = spark.createDataFrame(
        [(p,) for p in all_products], 
        ["ProductName"]
    )
    
    # Create Warehouses DataFrame
    warehouses_df = spark.createDataFrame(
        [(w,) for w in warehouse_names], 
        ["WarehouseName"]
    )
    
    # Create Dates DataFrame (all dates in range)
    dates_df = spark.sql(f"""
        SELECT explode(sequence(
            to_date('{start_date}'), 
            to_date('{end_date}'), 
            interval 1 day
        )) as StockDate
    """)
    
    print(f"  ✓ Created {len(all_products)} products")
    print(f"  ✓ Created {len(warehouse_names)} warehouses")
    print(f"  ✓ Created {BATCH_DAYS} dates")
    
    # CARTESIAN PRODUCT: Guarantees all combinations exist exactly once
    df_stock = (
        products_df
        .crossJoin(warehouses_df)  # 78 × 20 = 1,560 combinations
        .crossJoin(dates_df)        # 1,560 × 30 = 46,800 unique combinations
    )
    
    print(f"  ✓ Cartesian product created: {TOTAL_STOCK_ROWS:,} unique combinations")
    
    # Now add random StockQuantity (0 to 250 units)
    df_stock = df_stock.withColumn(
        "StockQuantity", 
        (rand(SEED_VALUE) * 250).cast("int")
    )
    
    # Add Reorder Level (category-based thresholds)
    df_stock = df_stock.withColumn(
        "ReorderLevel",
        when(col("ProductName").like("%TV%") | col("ProductName").like("%Laptop%"), 50)
        .when(col("ProductName").like("%Furniture%") | col("ProductName").like("%Sofa%") | col("ProductName").like("%Bed%"), 30)
        .when(col("ProductName").like("%Rice%") | col("ProductName").like("%Atta%") | col("ProductName").like("%Tea%"), 500)
        .when(col("ProductName").like("%Clothing%") | col("ProductName").like("%Shirt%") | col("ProductName").like("%Jeans%"), 200)
        .otherwise(100)
    )
    
    # Add LoadedAt timestamp
    df_stock = df_stock.withColumn("LoadedAt", current_timestamp())
    
    print("  ✓ Base stock dataframe built")
    
    # STEP 4: Add Stock Status Logic
    print("\n🔧 Step 4: Adding stock status logic...")
    
    df_stock = df_stock.withColumn(
        "StockStatus",
        when(col("StockQuantity") == 0, "Out of Stock")
        .when(col("StockQuantity") < col("ReorderLevel"), "Low Stock")
        .otherwise("In Stock")
    )
    
    # Get max existing StockID for globally unique IDs across batches
    try:
        max_stock_id = spark.sql(f"SELECT COALESCE(MAX(StockID), 0) FROM {Config.TBL_STG_STOCK}").collect()[0][0]
    except:
        max_stock_id = 0
    print(f"  ✓ Max existing StockID: {max_stock_id} (new IDs start at {max_stock_id + 1})")
    
    # Add StockID as sequential number (globally unique across batches)
    df_stock = df_stock.withColumn("StockID", (row_number().over(Window.orderBy(lit(1))) + lit(max_stock_id)).cast("int"))
    
    # Add BatchDate (same as StockDate)
    df_stock = df_stock.withColumn("BatchDate", col("StockDate"))
    
    print("  ✓ Stock status logic applied")
    
    # STEP 5: Final Column Order
    print("\n🔧 Step 5: Ordering columns...")
    
    final_columns = [
        "StockID",
        "ProductName",
        "WarehouseName",
        "StockDate",
        "StockQuantity",
        "ReorderLevel",
        "StockStatus",
        "BatchDate",
        "LoadedAt"
    ]
    
    df_stock = df_stock.select(*final_columns)
    
    print("  ✓ Columns ordered")
    
    # STEP 6: Write to Table
    print("\n🔧 Step 6: Writing to Unity Catalog...")
    
    # Delete-first for idempotency when using append mode (prevents duplicate BatchDates)
    if WRITE_MODE == "append":
        print(f"  🗑️  Deleting existing data for BatchDate range {start_date} to {end_date}...")
        try:
            spark.sql(f"""
                DELETE FROM {Config.TBL_STG_STOCK}
                WHERE BatchDate BETWEEN '{start_date}' AND '{end_date}'
            """)
            print(f"  ✓ Deleted existing data for overlapping dates (idempotent)")
        except Exception as e:
            print(f"  ℹ️  Table may not exist yet (first run): {e}")
    
    df_stock.write.mode(WRITE_MODE).option("overwriteSchema", "true").saveAsTable(Config.TBL_STG_STOCK)
    
    record_count = df_stock.count()
    
    # Stock Status Summary
    status_summary = df_stock.groupBy("StockStatus").count().collect()
    
    print("\n" + "="*70)
    print("✅ STOCK STATUS DATA GENERATION COMPLETE")
    print("="*70)
    print(f"📊 Records Generated: {record_count:,}")
    print(f"📅 Date Range: {start_date} to {end_date}")
    print(f"📦 Products: {len(all_products)}")
    print(f"🏢 Warehouses: {len(warehouse_names)}")
    print(f"🎯 Table: {Config.TBL_STG_STOCK}")
    print(f"\n✅ DATA QUALITY: DUPLICATE-FREE!")
    print(f"  ✓ Each Product-Warehouse-Date combination appears EXACTLY ONCE")
    print(f"  ✓ Expected: {len(all_products)} × {len(warehouse_names)} × {BATCH_DAYS} = {TOTAL_STOCK_ROWS:,} records")
    print(f"  ✓ Actual: {record_count:,} records")
    print(f"\n📄 Stock Status Distribution:")
    for row in status_summary:
        print(f"  - {row['StockStatus']}: {row['count']:,} records")
    print("="*70)
    
    print("\n📋 Sample Data (first 10 rows):")
    display(df_stock.limit(10))
    
else:
    print("⏭️  Skipping batch stock generation (mode != batch)")

# COMMAND ----------

# DBTITLE 1,Generate Incremental Sales Data - Daily Delta Load
# Complete rewrite: Generate incremental sales with FULL schema matching batch mode

# Import required functions for incremental mode
from pyspark.sql.functions import coalesce, to_date

if MODE == "incremental":
    print("="*70)
    print("GENERATING INCREMENTAL SALES DATA (DAILY DELTA LOAD)")
    print("="*70)
    
    # STEP 0: Determine target date range (gap-filling logic)
    print("\n🔧 Step 0: Determining target date range...")
    
    # Get last date from batch table
    try:
        max_batch_date_row = spark.sql(f"""
            SELECT MAX(BatchDate) as max_date 
            FROM {Config.TBL_STG_SALES}
        """).collect()[0]
        
        max_batch_date = max_batch_date_row['max_date']
        
        if max_batch_date is None:
            # Table exists but empty
            print("  ℹ️  Batch table is empty - starting fresh")
            max_batch_date = None
        else:
            print(f"  ✓ Last batch date: {max_batch_date}")
    except Exception as e:
        print(f"  ℹ️  Batch table not found - this will be the first run")
        max_batch_date = None
    
    # Determine date range
    if INCREMENTAL_DATE:
        # User specified exact date - use that only
        target_dates = [INCREMENTAL_DATE]
        print(f"  ✓ User-specified date: {INCREMENTAL_DATE}")
    else:
        # Auto-detect gap and fill
        today = datetime.now().date()
        
        if max_batch_date is None:
            # First run - generate only today
            target_dates = [today.strftime("%Y-%m-%d")]
            print(f"  ✓ First run - generating for today: {today}")
        else:
            # Calculate gap
            from datetime import timedelta
            max_batch_date_obj = datetime.strptime(str(max_batch_date), "%Y-%m-%d").date()
            start_date = max_batch_date_obj + timedelta(days=1)
            
            if start_date > today:
                # No gap detected - but still generate if this is an explicit incremental_date request
                # For auto-detection mode, we need an explicit date to avoid skipping
                if INCREMENTAL_DATE:
                    # User specified a date - honor it even if it would create a gap
                    print(f"  ⚠️  Warning: Specified date {INCREMENTAL_DATE} creates a gap (last batch: {max_batch_date})")
                    target_dates = [INCREMENTAL_DATE]
                else:
                    # No date specified and no gap - use today as default
                    print(f"  ℹ️  Batch table up-to-date (last date: {max_batch_date})")
                    print(f"  ℹ️  Defaulting to today: {today} (idempotent)")
                    target_dates = [today.strftime("%Y-%m-%d")]
            else:
                # Generate list of missing dates (gap-filling)
                target_dates = []
                current = start_date
                while current <= today:
                    target_dates.append(current.strftime("%Y-%m-%d"))
                    current += timedelta(days=1)
                
                print(f"  ✓ Gap detected: {len(target_dates)} day(s) missing")
                print(f"  ✓ Date range: {target_dates[0]} to {target_dates[-1]}")
    
    if len(target_dates) == 0:
        print("\n⏭️  No dates to process - incremental generation skipped")
    else:
        print(f"\n📅 Will generate {len(target_dates)} day(s) of data")
        print(f"📊 Records per day: {RECORDS_PER_DAY:,}")
        print(f"📊 Total records: {RECORDS_PER_DAY * len(target_dates):,}")
        
        # Initialize Faker (outside loop for consistency)
        fake = Faker("en_IN")
        
        # FIX: Accumulate all dates and write once at the end
        all_incremental_dfs = []  # List to store DataFrames for all dates
        
        # STEP 1: Get existing customers from historical table (ONCE, before loop)
        print("\n🔧 Step 1: Loading existing customers from historical table...")
        try:
            df_existing_customers = spark.sql(f"""
                SELECT DISTINCT
                    CustomerID,
                    CustomerName,
                    CustomerAge,
                    CustomerGender,
                    CustomerAddress,
                    CustomerContact,
                    CustomerMail
                FROM {Config.TBL_STG_SALES}
            """)
            existing_customers = df_existing_customers.collect()
            existing_customer_ids = [row.CustomerID for row in existing_customers]
            max_customer_id = max(existing_customer_ids) if existing_customer_ids else 0
            print(f"  ✓ Found {len(existing_customers):,} existing customers (max ID: {max_customer_id})")
        except Exception as e:
            print(f"  ⚠️  Historical table not found: {e}")
            print("  ✓ Starting fresh with CustomerID from 1")
            existing_customers = []
            existing_customer_ids = []
            max_customer_id = 0
        
        # Process each date
        for date_idx, target_date in enumerate(target_dates, 1):
            print("\n" + "="*70)
            print(f"PROCESSING DATE {date_idx}/{len(target_dates)}: {target_date}")
            print(f"  📌 Current max CustomerID: {max_customer_id}")
            print("="*70)
    
            # STEP 2: Generate incremental data using dbldatagen
            print(f"\n🔧 Step 2: Building incremental sales dataframe for {target_date}...")
            
            # Get products from PRODUCT_CATALOG
            all_products = []
            product_category_map = {}
            for category, products in PRODUCT_CATALOG.items():
                for product in products:
                    all_products.append(product)
                    product_category_map[product] = category
            
            # Build price expressions (matching Cell 9 logic)
            cost_price_case = "CASE "
            selling_price_case = "CASE "
            for product in all_products:
                safe_product = product.replace("'", "\\'")  # Escape quotes
                unit_cost = PRODUCT_UNIT_COST_MAPPING[product]  # From Cell 7
                selling_price = round(unit_cost * 1.45, 2)      # 45% markup
                cost_price_case += f"WHEN ProductName = '{safe_product}' THEN {unit_cost} "
                selling_price_case += f"WHEN ProductName = '{safe_product}' THEN {selling_price} "
            cost_price_case += "ELSE 100 END"
            selling_price_case += "ELSE 145 END"
            
            # Warehouses (from Cell 9)
            warehouse_mapping = {
        "Mumbai Central WH": "Mumbai",
        "Delhi North WH": "Delhi",
        "Bangalore South WH": "Bangalore",
        "Hyderabad East WH": "Hyderabad",
        "Chennai Port WH": "Chennai",
        "Kolkata WH": "Kolkata",
        "Pune WH": "Pune",
        "Ahmedabad WH": "Ahmedabad",
        "Jaipur WH": "Jaipur",
        "Lucknow WH": "Lucknow",
        "Kanpur WH": "Kanpur",
        "Nagpur WH": "Nagpur",
        "Indore WH": "Indore",
        "Bhopal WH": "Bhopal",
        "Visakhapatnam WH": "Visakhapatnam",
        "Patna WH": "Patna",
        "Vadodara WH": "Vadodara",
        "Ludhiana WH": "Ludhiana",
        "Agra WH": "Agra",
                "Nashik WH": "Nashik"
            }
            warehouse_names = list(warehouse_mapping.keys())
            warehouse_cities = [warehouse_mapping[n] for n in warehouse_names]
            
            # Build base incremental dataframe
            df_incremental = (
                dg.DataGenerator(spark, name="sap_sales_incremental", rows=RECORDS_PER_DAY, partitions=2)
                .withColumn(
                    "internal_device_id", "long",
                    minValue=0x1000000000000,
                    uniqueValues=10000,
                    omit=True,
                    baseColumnType="hash"
                )
                # Product
                .withColumn("ProductName", StringType(), values=all_products, baseColumn="internal_device_id")
                .withColumn("ProductCategory", StringType(), expr="CASE " + " ".join([f"WHEN ProductName = '{p}' THEN '{c}'" for p, c in product_category_map.items()]) + " ELSE 'Other' END")
                
                # Dates - all for target_date
                .withColumn("OrderDate", "Date", expr=f"to_date('{target_date}')")
                .withColumn("MFDDate", "Date", expr=f"date_sub(to_date('{target_date}'), int(rand() * 365) + 30)")
                .withColumn("EXPDate", "Date", expr=f"date_add(to_date('{target_date}'), int(rand() * 730) + 180)")
                .withColumn("DeliveryDate", "Date", expr=f"date_add(to_date('{target_date}'), int(rand() * 7) + 1)")
        
                # Pricing (matching Cell 9)
                .withColumn("CostPrice", "double", expr=cost_price_case)
                .withColumn("SellingPrice", "double", expr=selling_price_case)
                .withColumn("DiscountPercentage", "double", minValue=0, maxValue=25, random=True)
                .withColumn("Discount", "double", expr="ROUND(SellingPrice * (DiscountPercentage / 100), 2)")
                .withColumn("AmountAfterDiscount", "double", expr="ROUND(SellingPrice - Discount, 2)")
                .withColumn("ProductQuantity", "integer", minValue=1, maxValue=10, random=True)
                
                # Warehouse
                .withColumn("WareHouseName", StringType(), values=warehouse_names, baseColumn="internal_device_id")
                .withColumn("WareHouseCity", StringType(), values=warehouse_cities, baseColumn="internal_device_id")
                
                # Payment
                .withColumn("PaymentMode", StringType(), values=Config.PAYMENT_MODES, random=True)
                
                # Metadata
                .withColumn("LoadedAt", "timestamp", expr="current_timestamp()")
            ).build()
            
            print("  ✓ Base incremental dataframe built")
            
            # STEP 3: Add Customer Logic (50% unchanged + 20% changed + 30% new)
            print("\n🔧 Step 3: Adding customer assignments (50% unchanged + 20% changed + 30% new)...")
            
            # Calculate split
            unchanged_count = int(RECORDS_PER_DAY * 0.5)  # 50% unchanged existing customers
            changed_count = int(RECORDS_PER_DAY * 0.2)     # 20% changed existing customers
            existing_count = unchanged_count + changed_count  # 70% total existing
            new_count = RECORDS_PER_DAY - existing_count   # 30% new customers
            
            print(f"  ✓ Unchanged customers: {unchanged_count:,} records (50%)")
            print(f"  ✓ Changed customers: {changed_count:,} records (20%)")
            print(f"  ✓ New customers: {new_count:,} records (30%)")
            print(f"  ✓ Total existing: {existing_count:,} records (70%)")
            
            # Add row number for splitting
            df_incremental = df_incremental.withColumn("_row_num", row_number().over(Window.orderBy(lit(1))))
            
            # Tag as unchanged, changed, or new
            df_incremental = df_incremental.withColumn(
                "_customer_type",
                when(col("_row_num") <= unchanged_count, "unchanged")
                .when(col("_row_num") <= existing_count, "changed")
                .otherwise("new")
            )
            
            # For EXISTING customers: split into unchanged (50%) and changed (20%)
            if len(existing_customers) > 0:
                # Sample customers for both unchanged and changed buckets
                sampled_unchanged = random.choices(existing_customers, k=unchanged_count)
                sampled_changed = random.choices(existing_customers, k=changed_count)
                
                # UNCHANGED CUSTOMERS (50%) - keep as-is
                print(f"\n  ✓ Processing {unchanged_count:,} unchanged customers (50%)...")
                unchanged_data = []
                for i in range(unchanged_count):
                    cust = sampled_unchanged[i]
                    unchanged_data.append((
                        i + 1,  # row_num
                        cust.CustomerID, cust.CustomerName, cust.CustomerAge, cust.CustomerGender,
                        cust.CustomerAddress, cust.CustomerContact, cust.CustomerMail
                    ))
                
                unchanged_df = spark.createDataFrame(
                    unchanged_data,
                    ["_row_num", "CustomerID", "CustomerName", "CustomerAge", "CustomerGender",
                     "CustomerAddress", "CustomerContact", "CustomerMail"]
                )
                
                # CHANGED CUSTOMERS (20%) - modify attributes for CDC detection
                print(f"\n  🔄 Processing {changed_count:,} changed customers (20%)...")
                print(f"     Introducing attribute changes for CDC UPDATE detection...")
                
                changed_data = []
                change_types = {"address": 0, "contact": 0, "email": 0, "multiple": 0}
                
                for i in range(changed_count):
                    cust = sampled_changed[i]
                    # Each changed customer gets one type of change
                    change_type = random.choice(["address", "contact", "email", "multiple"])
                    change_types[change_type] += 1
                    
                    if change_type == "address":
                        # Customer moved to new city
                        changed_data.append((
                            unchanged_count + i + 1,  # row_num (after unchanged)
                            cust.CustomerID, cust.CustomerName, cust.CustomerAge, cust.CustomerGender,
                            fake.address(),  # New address
                            cust.CustomerContact, cust.CustomerMail
                        ))
                    elif change_type == "contact":
                        # Customer changed phone number
                        changed_data.append((
                            unchanged_count + i + 1,
                            cust.CustomerID, cust.CustomerName, cust.CustomerAge, cust.CustomerGender,
                            cust.CustomerAddress,
                            str(fake.random_int(min=6000000000, max=9999999999)),  # New contact
                            cust.CustomerMail
                        ))
                    elif change_type == "email":
                        # Customer updated email
                        changed_data.append((
                            unchanged_count + i + 1,
                            cust.CustomerID, cust.CustomerName, cust.CustomerAge, cust.CustomerGender,
                            cust.CustomerAddress, cust.CustomerContact,
                            fake.email()  # New email
                        ))
                    else:  # multiple changes
                        # Customer moved AND changed contact
                        changed_data.append((
                            unchanged_count + i + 1,
                            cust.CustomerID, cust.CustomerName, cust.CustomerAge, cust.CustomerGender,
                            fake.address(),  # New address
                            str(fake.random_int(min=6000000000, max=9999999999)),  # New contact
                            cust.CustomerMail
                        ))
                
                changed_df = spark.createDataFrame(
                    changed_data,
                    ["_row_num", "CustomerID", "CustomerName", "CustomerAge", "CustomerGender",
                     "CustomerAddress", "CustomerContact", "CustomerMail"]
                )
                
                print(f"     ✓ Address changes: {change_types['address']} customers")
                print(f"     ✓ Contact changes: {change_types['contact']} customers")
                print(f"     ✓ Email changes: {change_types['email']} customers")
                print(f"     ✓ Multiple changes: {change_types['multiple']} customers")
                
                # Union unchanged and changed customers
                all_existing_df = unchanged_df.union(changed_df)
                
                # Join to incremental data - use alias to avoid column ambiguity
                df_incremental = df_incremental.alias("base").join(
                    all_existing_df.alias("existing"),
                    (col("base._row_num") == col("existing._row_num")) & 
                    (col("base._customer_type").isin(["unchanged", "changed"])),
                    "left"
                ).select(
                    col("base.*"),
                    col("existing.CustomerID").alias("existing_CustomerID"),
                    col("existing.CustomerName").alias("existing_CustomerName"),
                    col("existing.CustomerAge").alias("existing_CustomerAge"),
                    col("existing.CustomerGender").alias("existing_CustomerGender"),
                    col("existing.CustomerAddress").alias("existing_CustomerAddress"),
                    col("existing.CustomerContact").alias("existing_CustomerContact"),
                    col("existing.CustomerMail").alias("existing_CustomerMail")
                )
                
                print(f"\n  ✓ Assigned {existing_count:,} total existing customers (unchanged + changed)")
            
            # For NEW customers: generate fresh data
            new_customer_names = [fake.unique.name() for _ in range(new_count)]
            new_customer_ages = [fake.random_int(min=18, max=75) for _ in range(new_count)]
            new_customer_addresses = [fake.address() for _ in range(new_count)]
            new_customer_contacts = [str(fake.random_int(min=6000000000, max=9999999999)) for _ in range(new_count)]
            new_customer_mails = [fake.email() for _ in range(new_count)]
            
            # Gender based on name heuristic
            new_customer_genders = []
            for name in new_customer_names:
                first_name = name.split()[0]
                if first_name[-1].lower() in ['a', 'i', 'e', 'y']:
                    new_customer_genders.append("Female")
                else:
                    new_customer_genders.append("Male")
            
            # New customer IDs (sequential from max existing)
            new_customer_ids = list(range(max_customer_id + 1, max_customer_id + new_count + 1))
            
            new_cust_df = spark.createDataFrame(
                [(existing_count + i + 1, new_customer_ids[i], new_customer_names[i],
                  new_customer_ages[i], new_customer_genders[i], new_customer_addresses[i],
                  new_customer_contacts[i], new_customer_mails[i])
                 for i in range(new_count)],
                ["new_row_num", "CustomerID", "CustomerName", "CustomerAge", "CustomerGender",
                 "CustomerAddress", "CustomerContact", "CustomerMail"]
            )
            
            # Join new customers - use alias to avoid column ambiguity
            df_incremental = df_incremental.alias("base2").join(
                new_cust_df.alias("new"),
                (col("base2._row_num") == col("new.new_row_num")) & 
                (col("_customer_type") == "new"),
                "left"
            ).select(
                col("base2.*"),
                col("new.CustomerID").alias("new_CustomerID"),
                col("new.CustomerName").alias("new_CustomerName"),
                col("new.CustomerAge").alias("new_CustomerAge"),
                col("new.CustomerGender").alias("new_CustomerGender"),
                col("new.CustomerAddress").alias("new_CustomerAddress"),
                col("new.CustomerContact").alias("new_CustomerContact"),
                col("new.CustomerMail").alias("new_CustomerMail")
            )
            
            print(f"  ✓ Generated {new_count:,} new customers (IDs: {max_customer_id+1}-{max_customer_id+new_count})")
            
            # Coalesce customer columns: take existing if present, otherwise new
            df_incremental = df_incremental.withColumn(
                "CustomerID", coalesce(col("existing_CustomerID"), col("new_CustomerID"))
            ).withColumn(
                "CustomerName", coalesce(col("existing_CustomerName"), col("new_CustomerName"))
            ).withColumn(
                "CustomerAge", coalesce(col("existing_CustomerAge"), col("new_CustomerAge"))
            ).withColumn(
                "CustomerGender", coalesce(col("existing_CustomerGender"), col("new_CustomerGender"))
            ).withColumn(
                "CustomerAddress", coalesce(col("existing_CustomerAddress"), col("new_CustomerAddress"))
            ).withColumn(
                "CustomerContact", coalesce(col("existing_CustomerContact"), col("new_CustomerContact"))
            ).withColumn(
                "CustomerMail", coalesce(col("existing_CustomerMail"), col("new_CustomerMail"))
            ).drop(
                "existing_CustomerID", "existing_CustomerName", "existing_CustomerAge", "existing_CustomerGender",
                "existing_CustomerAddress", "existing_CustomerContact", "existing_CustomerMail",
                "new_CustomerID", "new_CustomerName", "new_CustomerAge", "new_CustomerGender",
                "new_CustomerAddress", "new_CustomerContact", "new_CustomerMail"
            )
            
            # Drop helper columns
            df_incremental = df_incremental.drop("_row_num", "_customer_type")
            
            # STEP 4: Add Retailer and Agent (reuse from Cell 9 logic - generate fresh for incremental)
            print("\n🔧 Step 4: Adding retailer and agent data...")
            
            retailer_names = [f"{fake.company()} Store" for _ in range(50)]
            retailer_cities = [fake.city() for _ in range(50)]
            retailer_contacts = [str(fake.random_int(min=6000000000, max=9999999999)) for _ in range(50)]
            retailer_mails = [fake.email() for _ in range(50)]
            
            agent_names = [fake.name() for _ in range(50)]
            agent_cities = [fake.city() for _ in range(50)]
            agent_contacts = [str(fake.random_int(min=6000000000, max=9999999999)) for _ in range(50)]
            agent_mails = [fake.email() for _ in range(50)]
            
            # Add retailer columns
            df_incremental = df_incremental.withColumn(
                "_retailer_idx",
                (rand() * 50).cast("int")
            )
            
            retailer_df = spark.createDataFrame(
                [(i, retailer_names[i], retailer_cities[i], retailer_contacts[i], retailer_mails[i])
                 for i in range(50)],
                ["_retailer_idx", "RetailerName", "RetailerCity", "RetailerContact", "RetailerMail"]
            )
            
            df_incremental = df_incremental.join(retailer_df, "_retailer_idx").drop("_retailer_idx")
            
            # Add agent columns
            df_incremental = df_incremental.withColumn(
                "_agent_idx",
                (rand() * 50).cast("int")
            )
            
            agent_df = spark.createDataFrame(
                [(i, agent_names[i], agent_cities[i], agent_contacts[i], agent_mails[i])
                 for i in range(50)],
                ["_agent_idx", "AgentName", "AgentCity", "AgentContact", "AgentMail"]
            )
            
            df_incremental = df_incremental.join(agent_df, "_agent_idx").drop("_agent_idx")
            
            print("  ✓ Retailer and agent data added")
            
            # STEP 5: Add Tax Calculations (matching Cell 9 batch schema: CGST + SGST)
            print("\n🔧 Step 5: Adding CGST, SGST, finalPrice (matching batch schema)...")
            
            # FIX: Use CGST + SGST columns (matching Cell 9 batch schema)
            df_incremental = df_incremental.withColumn(
                "CGST",
                F.round(col("AmountAfterDiscount") * lit(Config.CGST_RATE), 2)
            )
            
            df_incremental = df_incremental.withColumn(
                "SGST",
                F.round(col("AmountAfterDiscount") * lit(Config.SGST_RATE), 2)
            )
            
            # finalPrice = Amount after discount + CGST + SGST (per unit)
            df_incremental = df_incremental.withColumn(
                "finalPrice",
                F.round(col("AmountAfterDiscount") + col("CGST") + col("SGST"), 2)
            )
            
            # TotalAmount = finalPrice × ProductQuantity (total order value)
            df_incremental = df_incremental.withColumn(
                "TotalAmount",
                F.round(col("finalPrice") * col("ProductQuantity"), 2)
            )
            
            print("  ✓ Tax columns added (CGST + SGST) - schema aligned with batch mode")
            
            # STEP 6: Add BatchDate, LoadedAt, OrderID
            print("\n🔧 Step 6: Adding metadata...")
            
            df_incremental = df_incremental.withColumn("BatchDate", to_date(lit(target_date)))
            df_incremental = df_incremental.withColumn("LoadedAt", lit(datetime.now()))
            df_incremental = df_incremental.withColumn(
                "OrderID",
                row_number().over(Window.orderBy(lit(1)))
            )
            
            print("  ✓ Metadata added (BatchDate, OrderID, LoadedAt)")
            
            # STEP 7: Final Column Order (match Cell 9 batch schema exactly)
            print("\n🔧 Step 7: Ordering columns (matching batch schema)...")
            
            # FIX: Match exact batch schema (with CustomerID, CGST, SGST, finalPrice, TotalAmount)
            final_columns = [
                "OrderID", "CustomerID", "CustomerName", "CustomerAge", "CustomerGender",
                "CustomerAddress", "CustomerContact", "CustomerMail",
                "ProductName", "ProductCategory",
                "MFDDate", "EXPDate", "OrderDate", "DeliveryDate",
                "CostPrice", "SellingPrice", "DiscountPercentage", "Discount",
                "CGST", "SGST", "finalPrice", "ProductQuantity", "TotalAmount",
                "WareHouseName", "WareHouseCity",
                "PaymentMode",
                "AgentName", "AgentCity", "AgentContact", "AgentMail",
                "RetailerName", "RetailerCity", "RetailerContact", "RetailerMail",
                "BatchDate", "LoadedAt"
            ]
            
            df_incremental = df_incremental.select(*final_columns)
            
            print("  ✓ Columns ordered")
            
            # STEP 8: Accumulate DataFrame for this date
            print("\n🔧 Step 8: Accumulating DataFrame for this date...")
            
            all_incremental_dfs.append(df_incremental)
            
            record_count = df_incremental.count()
            
            print(f"  ✓ Date {date_idx}/{len(target_dates)} complete: {target_date} ({record_count:,} records)")
            print(f"  📝 Customer Split for {target_date}:")
            print(f"    - Unchanged customers (50%): {unchanged_count:,} records")
            print(f"    - Changed customers (20%): {changed_count:,} records")
            print(f"    - New customers (30%): {new_count:,} records")
            
            # FIX: Update max_customer_id for next iteration to prevent ID reuse
            max_customer_id = max_customer_id + new_count
            print(f"  ✓ Updated max CustomerID to {max_customer_id} for next day")
        
        # End of for loop - FINAL STEP: Union all dates and write once
        print("\n" + "="*70)
        print("WRITING ALL DATES TO STAGING TABLE")
        print("="*70)
        
        if len(all_incremental_dfs) > 0:
            # Union all DataFrames for all dates
            print(f"\n🔧 Combining {len(all_incremental_dfs)} date(s) of data...")
            df_all_dates = all_incremental_dfs[0]
            for df in all_incremental_dfs[1:]:
                df_all_dates = df_all_dates.union(df)
            
            total_records = df_all_dates.count()
            print(f"  ✓ Total records across all dates: {total_records:,}")
            
            # Delete existing data for these dates first (idempotent - prevents duplicates)
            print(f"\n🔧 Checking for existing data in staging table...")
            try:
                existing_dates = spark.sql(f"""
                    SELECT DISTINCT BatchDate 
                    FROM {Config.TBL_STG_SALES_INCREMENTAL}
                    WHERE BatchDate IN ({','.join([f"'{d}'" for d in target_dates])})
                """).collect()
                
                if len(existing_dates) > 0:
                    dates_to_delete = [row.BatchDate for row in existing_dates]
                    print(f"  ⚠️  Found existing data for {len(dates_to_delete)} date(s): {dates_to_delete}")
                    print(f"  🗑️  Deleting to prevent duplicates...")
                    
                    spark.sql(f"""
                        DELETE FROM {Config.TBL_STG_SALES_INCREMENTAL}
                        WHERE BatchDate IN ({','.join([f"'{d}'" for d in target_dates])})
                    """)
                    
                    print(f"  ✓ Deleted existing data for {len(dates_to_delete)} date(s)")
                else:
                    print(f"  ✓ No existing data found for these dates")
            except Exception as e:
                print(f"  ℹ️  Staging table doesn't exist yet (first run): {e}")
            
            # Append new data (safe now - duplicates removed)
            print(f"\n🔧 Writing to staging table: {Config.TBL_STG_SALES_INCREMENTAL}...")
            df_all_dates.write \
                .mode("append") \
                .saveAsTable(Config.TBL_STG_SALES_INCREMENTAL)
            
            print("\n" + "="*70)
            print("✅ INCREMENTAL SALES DATA GENERATION COMPLETE")
            print("="*70)
            print(f"📊 Total Records Generated: {total_records:,}")
            print(f"📅 Date Range: {target_dates[0]} to {target_dates[-1]}")
            print(f"📅 Number of Dates: {len(target_dates)}")
            print(f"📊 Records per Day: {RECORDS_PER_DAY:,}")
            print(f"🎯 Written to STAGING: {Config.TBL_STG_SALES_INCREMENTAL}")
            print(f"\n🔍 Expected CDC Events (Cell 13):")
            print(f"  - INSERT: {new_count:,} new customers per day (30%)")
            print(f"  - UPDATE: {changed_count:,} changed customers per day (20%)")
            print(f"  - NO CHANGE: {unchanged_count:,} customers per day (50% - won't appear in CDC)")
            print(f"\n🔄 Next Step: Cell 13 will detect CDC from this staging data")
            print("="*70)
            
            print("\n📋 Sample Data (first 10 rows):")
            display(df_all_dates.orderBy("BatchDate", "OrderID").limit(10))
            
            print("\n📅 Records by Date:")
            display(df_all_dates.groupBy("BatchDate").count().orderBy("BatchDate"))
        else:
            print("\n⚠️  No data generated")
    
else:
    print("⏭️  Skipping incremental generation (mode != incremental)")

# COMMAND ----------

# DBTITLE 1,Write Incremental Sales to Streaming Volume (For DLT Auto Loader)
# Write incremental sales data to streaming volume as CSV files for DLT Auto Loader

if MODE == "incremental":
    print("="*70)
    print("WRITING INCREMENTAL SALES TO STREAMING VOLUME")
    print("="*70)
    
    # Environment-aware volume path
    STREAMING_VOLUME_PATH = f"/Volumes/{Config.CATALOG}/{Config.SCHEMA_BRONZE}/streaming_production_data"
    
    print(f"\n🎯 Target Volume: {STREAMING_VOLUME_PATH}")
    
    # STEP 1: Create volume if it doesn't exist (environment-aware)
    print("\n🔧 Step 1: Ensuring streaming volume exists...")
    
    try:
        spark.sql(f"""
            CREATE VOLUME IF NOT EXISTS {Config.CATALOG}.{Config.SCHEMA_BRONZE}.streaming_production_data
        """)
        print(f"  ✓ Volume ready: {Config.CATALOG}.{Config.SCHEMA_BRONZE}.streaming_production_data")
    except Exception as e:
        print(f"  ⚠️  Warning: Could not create volume: {e}")
        print(f"  ℹ️  Attempting to write anyway (volume may already exist)")
    
    # STEP 2: Check if incremental sales data exists
    print("\n🔧 Step 2: Checking for incremental sales data...")
    
    try:
        df_incremental = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_BRONZE}.stg_sales_incremental")
        record_count = df_incremental.count()
        
        if record_count == 0:
            print("  ⏭️  No incremental sales data found - skipping volume write")
        else:
            print(f"  ✓ Found {record_count:,} incremental sales records")
            
            # STEP 3: Generate timestamped filename
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"sales_{timestamp}.csv"
            output_path = f"{STREAMING_VOLUME_PATH}/{filename}"
            
            print(f"\n🔧 Step 3: Writing CSV to streaming volume...")
            print(f"  📁 Filename: {filename}")
            print(f"  📂 Full path: {output_path}")
            
            # STEP 4: Write as CSV (single file for Auto Loader)
            df_incremental.coalesce(1) \
                .write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(output_path)
            
            print("\n✅ SUCCESS: Incremental sales written to streaming volume")
            print("="*70)
            print("DLT Bronze Pipeline can now ingest this data via Auto Loader")
            print(f"Source: {output_path}")
            print(f"Target: {Config.CATALOG}.{Config.SCHEMA_BRONZE}.strm_sales_raw")
            print("="*70)
    
    except Exception as e:
        print(f"\n⚠️  Error: Could not write to streaming volume")
        print(f"  Error details: {e}")
        print(f"  ℹ️  This is expected if cell 13 hasn't run yet or produced no data")

else:
    print("ℹ️  Skipping streaming volume write (only runs in incremental mode)")

# COMMAND ----------

# DBTITLE 1,Generate Incremental Data - Purchase Orders (Daily Delta Load)
# Generate incremental purchase orders - New entries

if MODE == "incremental":
    print("="*70)
    print("GENERATING INCREMENTAL PURCHASE DATA (DAILY DELTA LOAD)")
    print("="*70)
    
    # STEP 0: Determine target date range (gap-filling logic)
    print("\n🔧 Step 0: Determining target date range...")
    
    # Get last date from batch table
    try:
        max_batch_date_row = spark.sql(f"""
            SELECT MAX(BatchDate) as max_date 
            FROM {Config.TBL_STG_PURCHASE}
        """).collect()[0]
        
        max_batch_date = max_batch_date_row['max_date']
        
        if max_batch_date is None:
            print("  ℹ️  Batch table is empty - starting fresh")
            max_batch_date = None
        else:
            print(f"  ✓ Last batch date: {max_batch_date}")
    except Exception as e:
        print(f"  ℹ️  Batch table not found - this will be the first run")
        max_batch_date = None
    
    # Determine date range
    if INCREMENTAL_DATE:
        target_dates = [INCREMENTAL_DATE]
        print(f"  ✓ User-specified date: {INCREMENTAL_DATE}")
    else:
        today = datetime.now().date()
        
        if max_batch_date is None:
            target_dates = [today.strftime("%Y-%m-%d")]
            print(f"  ✓ First run - generating for today: {today}")
        else:
            from datetime import timedelta
            max_batch_date_obj = datetime.strptime(str(max_batch_date), "%Y-%m-%d").date()
            start_date = max_batch_date_obj + timedelta(days=1)
            
            if start_date > today:
                # No gap - default to today for incremental staging
                if INCREMENTAL_DATE:
                    print(f"  ⚠️  Warning: Specified date {INCREMENTAL_DATE} creates a gap")
                    target_dates = [INCREMENTAL_DATE]
                else:
                    print(f"  ℹ️  Batch up-to-date - defaulting to today: {today}")
                    target_dates = [today.strftime("%Y-%m-%d")]
            else:
                target_dates = []
                current = start_date
                while current <= today:
                    target_dates.append(current.strftime("%Y-%m-%d"))
                    current += timedelta(days=1)
                
                print(f"  ✓ Gap detected: {len(target_dates)} day(s) missing")
                print(f"  ✓ Date range: {target_dates[0]} to {target_dates[-1]}")
    
    if len(target_dates) == 0:
        print("\n⏭️  No dates to process - incremental purchase generation skipped")
    else:
        # Purchase volume: 10% of sales volume
        PURCHASE_RECORDS_PER_DAY = RECORDS_PER_DAY // 10
        
        print(f"\n📅 Will generate {len(target_dates)} day(s) of purchase data")
        print(f"📊 Records per day: {PURCHASE_RECORDS_PER_DAY:,} (10% of sales)")
        print(f"📊 Total records: {PURCHASE_RECORDS_PER_DAY * len(target_dates):,}")
        
        # Initialize Faker
        fake = Faker("en_IN")
        
        # STEP 1: Load reference data (ONCE, before loop)
        print("\n🔧 Step 1: Loading reference data...")
        
        # Wholesalers from Cell 3
        print(f"  ✓ Wholesalers: {len(WHOLESALERS)}")
        
        # Warehouses from sales table
        try:
            warehouse_df = spark.sql(f"SELECT DISTINCT WareHouseName FROM {Config.TBL_STG_SALES}")
            warehouse_list = [r["WareHouseName"] for r in warehouse_df.collect()]
            print(f"  ✓ Warehouses from stg_sales: {len(warehouse_list)}")
        except:
            # Fallback to hardcoded list
            warehouse_list = [
                "Mumbai Central WH", "Delhi North WH", "Bangalore South WH", "Hyderabad East WH",
                "Chennai Port WH", "Kolkata WH", "Pune WH", "Ahmedabad WH", "Jaipur WH",
                "Lucknow WH", "Kanpur WH", "Nagpur WH", "Indore WH", "Bhopal WH",
                "Visakhapatnam WH", "Patna WH", "Vadodara WH", "Ludhiana WH", "Agra WH", "Nashik WH"
            ]
            print(f"  ✓ Using default warehouses: {len(warehouse_list)}")
        
        # Products from PRODUCT_CATALOG
        all_products = []
        for category, products in PRODUCT_CATALOG.items():
            all_products.extend(products)
        print(f"  ✓ Products: {len(all_products)}")
        
        # Get max PurchaseID from history
        try:
            max_purchase_id = spark.sql(f"""
                SELECT COALESCE(MAX(PurchaseID), 0) as max_id
                FROM {Config.TBL_STG_PURCHASE}
            """).collect()[0]['max_id']
            print(f"  ✓ Starting PurchaseID from: {max_purchase_id + 1}")
        except:
            max_purchase_id = 0
            print("  ✓ Starting PurchaseID from: 1")
        
        # Build UnitCost CASE expression (using shared mapping from Cell 8)
        unit_cost_case_expr = "CASE "
        for product in all_products:
            safe_product = product.replace("'", "\\'")  # Escape quotes
            unit_cost = PRODUCT_UNIT_COST_MAPPING[product]
            unit_cost_case_expr += f"WHEN ProductName = '{safe_product}' THEN {unit_cost} "
        unit_cost_case_expr += "ELSE 100 END"
        
        # Accumulate all dates
        all_incremental_purchase_dfs = []
        
        # Process each date
        for date_idx, target_date in enumerate(target_dates, 1):
            print("\n" + "="*70)
            print(f"PROCESSING PURCHASE DATE {date_idx}/{len(target_dates)}: {target_date}")
            print("="*70)
            
            # STEP 2: Generate incremental purchase data
            print(f"\n🔧 Step 2: Building incremental purchase dataframe for {target_date}...")
            
            # Calculate PurchaseID range for this date
            start_id = max_purchase_id + ((date_idx - 1) * PURCHASE_RECORDS_PER_DAY) + 1
            end_id = start_id + PURCHASE_RECORDS_PER_DAY - 1
            
            df_incremental_purchase = (
                dg.DataGenerator(spark, name="sap_purchase_incremental", rows=PURCHASE_RECORDS_PER_DAY, partitions=2)
                .withColumn("PurchaseID", "integer", minValue=start_id, maxValue=end_id, step=1)
                
                # Product
                .withColumn("ProductName", StringType(), values=all_products, random=True)
                
                # Pricing (matching batch table schema)
                .withColumn("UnitCost", "double", expr=unit_cost_case_expr)
                .withColumn("Quantity", "integer", minValue=50, maxValue=500, random=True)
                .withColumn("GrossAmount", "double", expr="ROUND(UnitCost * Quantity, 2)")
                .withColumn("GSTAmount", "double", expr="ROUND(GrossAmount * 0.18, 2)")
                .withColumn("NetAmount", "double", expr="ROUND(GrossAmount + GSTAmount, 2)")
                
                # Wholesaler and Warehouse (matching batch column names)
                .withColumn("WholeSalerName", StringType(), values=WHOLESALERS, random=True)
                .withColumn("WareHouseName", StringType(), values=warehouse_list, random=True)
                
                # Dates - all for target_date
                .withColumn("PurchaseDate", "Date", expr=f"to_date('{target_date}')")
                .withColumn("BatchDate", "Date", expr=f"to_date('{target_date}')")
                
                # Metadata
                .withColumn("LoadedAt", "timestamp", expr="current_timestamp()")
            ).build()
            
            print(f"  ✓ Generated {PURCHASE_RECORDS_PER_DAY:,} purchase orders for {target_date}")
            print(f"  ✓ PurchaseID range: {start_id} to {end_id}")
            
            # Collect for batch write
            all_incremental_purchase_dfs.append(df_incremental_purchase)
            
            # Update max_purchase_id
            max_purchase_id = end_id
        
        # STEP 3: Write all dates to incremental staging table
        if len(all_incremental_purchase_dfs) > 0:
            print("\n" + "="*70)
            print("STEP 3: WRITING TO INCREMENTAL STAGING TABLE")
            print("="*70)
            
            # Union all dates
            df_final_purchase = all_incremental_purchase_dfs[0]
            for df in all_incremental_purchase_dfs[1:]:
                df_final_purchase = df_final_purchase.union(df)
            
            total_records = df_final_purchase.count()
            
            print(f"\n📊 Total incremental purchase records: {total_records:,}")
            print(f"📅 Date range: {target_dates[0]} to {target_dates[-1]}")
            print(f"🎯 Target table: {Config.TBL_STG_PURCHASE_INCREMENTAL}")
            
            # Use replaceWhere for idempotent writes (partition-aware)
            date_range_filter = f"BatchDate >= '{target_dates[0]}' AND BatchDate <= '{target_dates[-1]}'"
            
            df_final_purchase.write \
                .format("delta") \
                .mode("overwrite") \
                .option("replaceWhere", date_range_filter) \
                .option("overwriteSchema", "true") \
                .saveAsTable(Config.TBL_STG_PURCHASE_INCREMENTAL)
            
            print("\n✅ INCREMENTAL PURCHASE DATA GENERATION COMPLETE")
            print("="*70)
            
            # Display sample
            print("\n📋 Sample of incremental purchase data:")
            display(spark.table(Config.TBL_STG_PURCHASE_INCREMENTAL).orderBy("PurchaseDate", "PurchaseID").limit(5))

else:
    print("⏭️  Skipping incremental purchase generation (mode != incremental)")

# COMMAND ----------

# DBTITLE 1,Generate Incremental Data - Stock Status (Daily Delta Load)
# Generate incremental stock status - Daily snapshots for all product-warehouse combinations

from pyspark.sql.functions import to_date

if MODE == "incremental":
    print("="*70)
    print("GENERATING INCREMENTAL STOCK STATUS DATA (DAILY DELTA LOAD)")
    print("="*70)
    
    # STEP 0: Determine target date range (gap-filling logic)
    print("\n🔧 Step 0: Determining target date range...")
    
    # Get last date from batch table
    try:
        max_batch_date_row = spark.sql(f"""
            SELECT MAX(BatchDate) as max_date 
            FROM {Config.TBL_STG_STOCK}
        """).collect()[0]
        
        max_batch_date = max_batch_date_row['max_date']
        
        if max_batch_date is None:
            print("  ℹ️  Batch table is empty - starting fresh")
            max_batch_date = None
        else:
            print(f"  ✓ Last batch date: {max_batch_date}")
    except Exception as e:
        print(f"  ℹ️  Batch table not found - this will be the first run")
        max_batch_date = None
    
    # Determine date range
    if INCREMENTAL_DATE:
        target_dates = [INCREMENTAL_DATE]
        print(f"  ✓ User-specified date: {INCREMENTAL_DATE}")
    else:
        today = datetime.now().date()
        
        if max_batch_date is None:
            target_dates = [today.strftime("%Y-%m-%d")]
            print(f"  ✓ First run - generating for today: {today}")
        else:
            from datetime import timedelta
            max_batch_date_obj = datetime.strptime(str(max_batch_date), "%Y-%m-%d").date()
            start_date = max_batch_date_obj + timedelta(days=1)
            
            if start_date > today:
                # No gap - default to today for incremental staging
                if INCREMENTAL_DATE:
                    print(f"  ⚠️  Warning: Specified date {INCREMENTAL_DATE} creates a gap")
                    target_dates = [INCREMENTAL_DATE]
                else:
                    print(f"  ℹ️  Batch up-to-date - defaulting to today: {today}")
                    target_dates = [today.strftime("%Y-%m-%d")]
            else:
                target_dates = []
                current = start_date
                while current <= today:
                    target_dates.append(current.strftime("%Y-%m-%d"))
                    current += timedelta(days=1)
                
                print(f"  ✓ Gap detected: {len(target_dates)} day(s) missing")
                print(f"  ✓ Date range: {target_dates[0]} to {target_dates[-1]}")
    
    if len(target_dates) == 0:
        print("\n⏭️  No dates to process - incremental stock generation skipped")
    else:
        # STEP 1: Load reference data (ONCE, before loop)
        print("\n🔧 Step 1: Loading reference data...")
        
        # Products from PRODUCT_CATALOG
        all_products = []
        for category, products in PRODUCT_CATALOG.items():
            all_products.extend(products)
        print(f"  ✓ Products: {len(all_products)}")
        
        # Warehouses
        warehouse_mapping = {
            "Mumbai Central WH": "Mumbai",
            "Delhi North WH": "Delhi",
            "Bangalore South WH": "Bangalore",
            "Hyderabad East WH": "Hyderabad",
            "Chennai Port WH": "Chennai",
            "Kolkata WH": "Kolkata",
            "Pune WH": "Pune",
            "Ahmedabad WH": "Ahmedabad",
            "Jaipur WH": "Jaipur",
            "Lucknow WH": "Lucknow",
            "Kanpur WH": "Kanpur",
            "Nagpur WH": "Nagpur",
            "Indore WH": "Indore",
            "Bhopal WH": "Bhopal",
            "Visakhapatnam WH": "Visakhapatnam",
            "Patna WH": "Patna",
            "Vadodara WH": "Vadodara",
            "Ludhiana WH": "Ludhiana",
            "Agra WH": "Agra",
            "Nashik WH": "Nashik"
        }
        warehouse_names = list(warehouse_mapping.keys())
        print(f"  ✓ Warehouses: {len(warehouse_names)}")
        
        # Calculate records per day: products × warehouses (daily snapshot for each combo)
        STOCK_RECORDS_PER_DAY = len(all_products) * len(warehouse_names)
        
        print(f"\n📅 Will generate {len(target_dates)} day(s) of stock snapshots")
        print(f"📊 Records per day: {STOCK_RECORDS_PER_DAY:,} ({len(all_products)} products × {len(warehouse_names)} warehouses)")
        print(f"📊 Total records: {STOCK_RECORDS_PER_DAY * len(target_dates):,}")
        
        # Get max StockID from history
        try:
            max_stock_id = spark.sql(f"""
                SELECT COALESCE(MAX(StockID), 0) as max_id
                FROM {Config.TBL_STG_STOCK}
            """).collect()[0]['max_id']
            print(f"  ✓ Starting StockID from: {max_stock_id + 1}")
        except:
            max_stock_id = 0
            print("  ✓ Starting StockID from: 1")
        
        # Accumulate all dates
        all_incremental_stock_dfs = []
        
        # Process each date
        for date_idx, target_date in enumerate(target_dates, 1):
            print("\n" + "="*70)
            print(f"PROCESSING STOCK DATE {date_idx}/{len(target_dates)}: {target_date}")
            print("="*70)
            
            # STEP 2: Generate incremental stock status data
            print(f"\n🔧 Step 2: Building incremental stock dataframe for {target_date}...")
            
            # Calculate StockID range for this date
            start_stock_id = max_stock_id + ((date_idx - 1) * STOCK_RECORDS_PER_DAY) + 1
            end_stock_id = start_stock_id + STOCK_RECORDS_PER_DAY - 1
            
            # Create product-warehouse cartesian product for this date
            df_products = spark.createDataFrame([(p,) for p in all_products], ["ProductName"])
            df_warehouses = spark.createDataFrame([(w,) for w in warehouse_names], ["WarehouseName"])
            
            # Cross join to get all combinations
            df_combinations = df_products.crossJoin(df_warehouses)
            
            # Add sequential StockID using row_number
            from pyspark.sql.window import Window
            window_spec = Window.orderBy("ProductName", "WarehouseName")
            
            # Add stock metrics (matching batch table schema)
            df_incremental_stock = df_combinations \
                .withColumn("_row_num", row_number().over(window_spec)) \
                .withColumn("StockID", col("_row_num") + start_stock_id - 1) \
                .withColumn("StockQuantity", (rand() * 2900 + 100).cast("integer")) \
                .withColumn("ReorderLevel", lit(100)) \
                .withColumn("StockStatus", 
                    when(col("StockQuantity") > 1000, "In Stock")
                    .when(col("StockQuantity") > 500, "Low Stock")
                    .otherwise("Out of Stock")
                ) \
                .withColumn("StockDate", to_date(lit(target_date))) \
                .withColumn("BatchDate", to_date(lit(target_date))) \
                .withColumn("LoadedAt", lit(datetime.now())) \
                .drop("_row_num") \
                .select("StockID", "ProductName", "WarehouseName", "StockDate", 
                       "StockQuantity", "ReorderLevel", "StockStatus", "BatchDate", "LoadedAt")
            
            print(f"  ✓ Generated {STOCK_RECORDS_PER_DAY:,} stock snapshots for {target_date}")
            print(f"  ✓ StockID range: {start_stock_id} to {end_stock_id}")
            
            # Collect for batch write
            all_incremental_stock_dfs.append(df_incremental_stock)
            
            # Update max_stock_id
            max_stock_id = end_stock_id
        
        # STEP 3: Write all dates to incremental staging table
        if len(all_incremental_stock_dfs) > 0:
            print("\n" + "="*70)
            print("STEP 3: WRITING TO INCREMENTAL STAGING TABLE")
            print("="*70)
            
            # Union all dates
            df_final_stock = all_incremental_stock_dfs[0]
            for df in all_incremental_stock_dfs[1:]:
                df_final_stock = df_final_stock.union(df)
            
            total_records = df_final_stock.count()
            
            print(f"\n📊 Total incremental stock records: {total_records:,}")
            print(f"📅 Date range: {target_dates[0]} to {target_dates[-1]}")
            print(f"🎯 Target table: {Config.TBL_STG_STOCK_INCREMENTAL}")
            
            # Use replaceWhere for idempotent writes (partition-aware)
            date_range_filter = f"BatchDate >= '{target_dates[0]}' AND BatchDate <= '{target_dates[-1]}'"
            
            df_final_stock.write \
                .format("delta") \
                .mode("overwrite") \
                .option("replaceWhere", date_range_filter) \
                .option("overwriteSchema", "true") \
                .saveAsTable(Config.TBL_STG_STOCK_INCREMENTAL)
            
            print("\n✅ INCREMENTAL STOCK STATUS DATA GENERATION COMPLETE")
            print("="*70)
            
            # Display sample
            print("\n📋 Sample of incremental stock data:")
            display(spark.table(Config.TBL_STG_STOCK_INCREMENTAL).orderBy("StockDate", "ProductName", "WarehouseName").limit(5))

else:
    print("⏭️  Skipping incremental stock generation (mode != incremental)")

# COMMAND ----------

# DBTITLE 1,Generate CDC Data - Customer Changes
# Complete rewrite: Detect REAL customer CDC from incremental staging data
# FIX 1: Run when MODE == "incremental" (not "cdc")
# FIX 2: Read from actual staging table (stg_sales_incremental)
# FIX 3: Compare with historical data (stg_sales)
# FIX 4: Detect NEW customers (INSERT) and CHANGED customers (UPDATE)
# FIX 5: Match schema from Cell 12 (CustomerName, CustomerAddress, CustomerContact, etc.)

if MODE == "incremental":
    print("="*70)
    print("DETECTING CUSTOMER CDC FROM INCREMENTAL DATA")
    print("="*70)
    
    # STEP 1: Check if staging table exists (Cell 12 must run first)
    print("\n🔧 Step 1: Validating staging table...")
    try:
        staging_count = spark.table(Config.TBL_STG_SALES_INCREMENTAL).count()
        print(f"  ✓ Staging table exists: {Config.TBL_STG_SALES_INCREMENTAL}")
        print(f"  ✓ Records in staging: {staging_count:,}")
        
        # Check if table exists but is empty - this is OK, just means no CDC events
        if staging_count == 0:
            print("  ℹ️  Staging table is empty (no new records to process)")
            print("  ⏭️  Skipping CDC detection - nothing to process")
            dbutils.notebook.exit("CDC_SKIPPED: Staging table empty")
            
    except Exception as e:
        print(f"  ❌ ERROR: Staging table not found: {Config.TBL_STG_SALES_INCREMENTAL}")
        print(f"  ❌ {str(e)}")
        print("\n⚠️  This indicates a CONFIGURATION ERROR:")
        print("   • Cell 12 must generate incremental staging data BEFORE Cell 16 runs")
        print("   • For incremental load, the staging table MUST exist")
        print("   • Without staging, incremental processing is not possible")
        print("\n🔧 Troubleshooting:")
        print("   1. Ensure Cell 12 ran successfully")
        print("   2. Check if 'mode' parameter is set to 'incremental'")
        print("   3. Verify Cell 12 didn't skip generation (check logs above)")
        raise Exception("CONFIGURATION ERROR: Incremental staging table not found. Cell 12 must generate data first.")
    
    # STEP 2: Read incremental data (today's new/changed customers)
    print("\n🔧 Step 2: Reading incremental customer data...")
    df_incremental = spark.table(Config.TBL_STG_SALES_INCREMENTAL)
    
    # Extract distinct customers from incremental data
    df_incr_customers = df_incremental.select(
        "CustomerID",
        "CustomerName",
        "CustomerAge",
        "CustomerGender",
        "CustomerAddress",
        "CustomerContact",
        "CustomerMail"
    ).distinct()
    
    incr_customer_count = df_incr_customers.count()
    print(f"  ✓ Distinct customers in incremental: {incr_customer_count:,}")
    
    # STEP 3: Read historical data (baseline for comparison)
    print("\n🔧 Step 3: Reading historical customer data...")
    try:
        df_historical = spark.table(Config.TBL_STG_SALES)
        
        # Extract distinct customers from historical data
        df_hist_customers = df_historical.select(
            "CustomerID",
            "CustomerName",
            "CustomerAge",
            "CustomerGender",
            "CustomerAddress",
            "CustomerContact",
            "CustomerMail"
        ).distinct()
        
        hist_customer_count = df_hist_customers.count()
        print(f"  ✓ Historical table exists: {Config.TBL_STG_SALES}")
        print(f"  ✓ Distinct customers in historical: {hist_customer_count:,}")
        
        historical_exists = True
    except Exception as e:
        print(f"  ⚠️  Historical table not found (first run): {Config.TBL_STG_SALES}")
        print("  ℹ️  All incremental customers will be treated as NEW (INSERT)")
        df_hist_customers = spark.createDataFrame([], df_incr_customers.schema)
        historical_exists = False
    
    # STEP 4: Detect NEW customers (INSERT operations)
    print("\n🔧 Step 4: Detecting NEW customers...")
    
    # FIX: Use CustomerID (unique identifier) instead of CustomerName (can have duplicates)
    # Left anti join: customers in incremental but NOT in historical (by CustomerID)
    df_new_customers = df_incr_customers.join(
        df_hist_customers.select("CustomerID").distinct(),
        "CustomerID",
        "left_anti"
    )
    
    # FIX: Join puts CustomerName first - reorder to match schema (CustomerID first)
    df_new_customers = df_new_customers.select(
        "CustomerID",
        "CustomerName",
        "CustomerAge",
        "CustomerGender",
        "CustomerAddress",
        "CustomerContact",
        "CustomerMail"
    )
    
    new_count = df_new_customers.count()
    print(f"  ✓ NEW customers detected: {new_count:,}")
    
    # Add CDC metadata for INSERT operations
    df_new_customers = df_new_customers.withColumn("Operation", lit("INSERT"))
    df_new_customers = df_new_customers.withColumn(
        "ChangeTimestamp",
        F.current_timestamp().cast("string")
    )
    df_new_customers = df_new_customers.withColumn(
        "CDC_EventID",
        F.expr("uuid()")
    )
    
    # STEP 5: Detect CHANGED customers (UPDATE operations)
    print("\n🔧 Step 5: Detecting CHANGED customers...")
    
    if historical_exists:
        # FIX: Use CustomerID (unique identifier) instead of CustomerName (can have duplicates)
        # Inner join: find customers that exist in both (by CustomerID)
        df_existing = df_incr_customers.alias("incr").join(
            df_hist_customers.alias("hist"),
            col("incr.CustomerID") == col("hist.CustomerID"),
            "inner"
        )
        
        # Detect changes in any attribute
        df_changed_customers = df_existing.filter(
            (col("incr.CustomerAddress") != col("hist.CustomerAddress")) |
            (col("incr.CustomerContact") != col("hist.CustomerContact")) |
            (col("incr.CustomerAge") != col("hist.CustomerAge")) |
            (col("incr.CustomerGender") != col("hist.CustomerGender")) |
            (col("incr.CustomerMail") != col("hist.CustomerMail"))
        )
        
        # Select only incremental columns (new values)
        df_changed_customers = df_changed_customers.select(
            col("incr.CustomerID"),
            col("incr.CustomerName"),
            col("incr.CustomerAge"),
            col("incr.CustomerGender"),
            col("incr.CustomerAddress"),
            col("incr.CustomerContact"),
            col("incr.CustomerMail")
        )
        
        changed_count = df_changed_customers.count()
        print(f"  ✓ CHANGED customers detected: {changed_count:,}")
        
        # Add CDC metadata for UPDATE operations
        df_changed_customers = df_changed_customers.withColumn("Operation", lit("UPDATE"))
        df_changed_customers = df_changed_customers.withColumn(
            "ChangeTimestamp",
            F.current_timestamp().cast("string")
        )
        df_changed_customers = df_changed_customers.withColumn(
            "CDC_EventID",
            F.expr("uuid()")
        )
    else:
        # No historical data - no changes to detect
        df_changed_customers = spark.createDataFrame([], df_new_customers.schema)
        changed_count = 0
        print(f"  ℹ️  No historical data - skipping change detection")
    
    # STEP 6: Combine INSERT and UPDATE events
    print("\n🔧 Step 6: Combining CDC events...")
    
    df_cdc_events = df_new_customers.union(df_changed_customers)
    
    total_cdc_count = df_cdc_events.count()
    print(f"  ✓ Total CDC events: {total_cdc_count:,}")
    print(f"    - INSERT: {new_count:,}")
    print(f"    - UPDATE: {changed_count:,}")
    
    # STEP 7: Write CDC events to customer_changes_raw
    print("\n🔧 Step 7: Writing CDC events...")
    
    if total_cdc_count > 0:
        # Write to CDC table (append mode - accumulate all CDC events)
        df_cdc_events.write.mode("append").saveAsTable(Config.TBL_BRONZE_CUSTOMER_CDC)
        
        print("\n" + "="*70)
        print("✅ CUSTOMER CDC DETECTION COMPLETE")
        print("="*70)
        print(f"📝 CDC Events Written: {total_cdc_count:,}")
        print(f"🎯 Target Table: {Config.TBL_BRONZE_CUSTOMER_CDC}")
        print(f"\n📊 CDC Event Breakdown:")
        print(f"  - INSERT (new customers): {new_count:,}")
        print(f"  - UPDATE (changed customers): {changed_count:,}")
        print("="*70)
        
        # Show operation distribution
        print("\n📈 Operation Distribution:")
        display(df_cdc_events.groupBy("Operation").count().orderBy("Operation"))
        
        print("\n📋 Sample CDC Events (first 10):")
        display(df_cdc_events.orderBy("Operation", "CustomerName").limit(10))
    else:
        print("\n⚠️  No CDC events detected (no new or changed customers)")
        print("   This can happen if:")
        print("   - All incremental customers already exist in historical with same attributes")
        print("   - Incremental data contains only repeat transactions from existing customers")
    
else:
    print("⏭️  Skipping customer CDC detection (mode != incremental)")

# COMMAND ----------

# DBTITLE 1,Validation - Data Quality Checks
def validate_generated_data():
    """
    Run data quality validations on generated tables
    """
    print("="*70)
    print("DATA VALIDATION REPORT")
    print("="*70)
    
    validations = []
    
    # Sales Table Validation
    try:
        df_sales = spark.table(Config.TBL_STG_SALES)
        sales_count = df_sales.count()
        sales_nulls = df_sales.filter(F.col("NetAmount").isNull()).count()
        sales_date_range = df_sales.agg(
            F.min("SaleDate").alias("min_date"),
            F.max("SaleDate").alias("max_date")
        ).first()
        
        validations.append({
            "Table": "stg_sales",
            "Records": sales_count,
            "Null NetAmount": sales_nulls,
            "Date Range": f"{sales_date_range['min_date']} to {sales_date_range['max_date']}",
            "Status": "PASS" if sales_nulls == 0 else "FAIL"
        })
    except Exception as e:
        validations.append({
            "Table": "stg_sales",
            "Records": 0,
            "Null NetAmount": "N/A",
            "Date Range": "N/A",
            "Status": f"ERROR: {str(e)}"
        })
    
    # Purchase Table Validation
    try:
        df_purchase = spark.table(Config.TBL_STG_PURCHASE)
        purchase_count = df_purchase.count()
        purchase_nulls = df_purchase.filter(F.col("NetAmount").isNull()).count()
        
        validations.append({
            "Table": "stg_purchase",
            "Records": purchase_count,
            "Null NetAmount": purchase_nulls,
            "Date Range": "N/A",
            "Status": "PASS" if purchase_nulls == 0 else "FAIL"
        })
    except Exception as e:
        validations.append({
            "Table": "stg_purchase",
            "Records": 0,
            "Null NetAmount": "N/A",
            "Date Range": "N/A",
            "Status": f"ERROR: {str(e)}"
        })
    
    # Stock Table Validation
    try:
        df_stock = spark.table(Config.TBL_STG_STOCK)
        stock_count = df_stock.count()
        
        validations.append({
            "Table": "stg_stock_status",
            "Records": stock_count,
            "Null NetAmount": "N/A",
            "Date Range": "N/A",
            "Status": "PASS"
        })
    except Exception as e:
        validations.append({
            "Table": "stg_stock_status",
            "Records": 0,
            "Null NetAmount": "N/A",
            "Date Range": "N/A",
            "Status": f"ERROR: {str(e)}"
        })
    
    # Business Rule Validation: GST Calculation
    try:
        df_gst_check = spark.sql(f"""
            SELECT 
                COUNT(*) as total_records,
                SUM(CASE WHEN ABS((GrossAmount * 0.18) - GSTAmount) > 0.01 THEN 1 ELSE 0 END) as gst_mismatch
            FROM {Config.TBL_STG_SALES}
        """)
        gst_result = df_gst_check.first()
        
        validations.append({
            "Table": "GST Validation",
            "Records": gst_result["total_records"],
            "Null NetAmount": gst_result["gst_mismatch"],
            "Date Range": "N/A",
            "Status": "PASS" if gst_result["gst_mismatch"] == 0 else "FAIL"
        })
    except Exception as e:
        validations.append({
            "Table": "GST Validation",
            "Records": "N/A",
            "Null NetAmount": "N/A",
            "Date Range": "N/A",
            "Status": f"ERROR: {str(e)}"
        })
    
    # Display validation results
    df_validations = spark.createDataFrame(validations)
    display(df_validations)
    
    print("="*70)
    print("Validation Summary:")
    passed = sum(1 for v in validations if v["Status"] == "PASS")
    print(f"Checks Passed: {passed}/{len(validations)}")
    print("="*70)

# Run validation
validate_generated_data()

# Clean exit for orchestration
dbutils.notebook.exit("SUCCESS")

# COMMAND ----------

# DBTITLE 1,Integration Instructions - Pipeline Setup
# MAGIC %md
# MAGIC ## Integration with Pipeline Notebooks
# MAGIC
# MAGIC **Step 1: Generate Initial Batch Data**
# MAGIC
# MAGIC ```python
# MAGIC # Set widgets:
# MAGIC # - mode: batch
# MAGIC # - batch_days: 90
# MAGIC # - records_per_day: 1000
# MAGIC # - write_mode: overwrite
# MAGIC
# MAGIC # Run all cells in this notebook
# MAGIC ```
# MAGIC
# MAGIC **Step 2: Verify Bronze Tables Created**
# MAGIC
# MAGIC ```sql
# MAGIC SHOW TABLES IN sap_dev.bronze;
# MAGIC
# MAGIC -- Check record counts
# MAGIC SELECT 'stg_sales' as table_name, COUNT(*) as records FROM sap_dev.bronze.stg_sales
# MAGIC UNION ALL
# MAGIC SELECT 'stg_purchase', COUNT(*) FROM sap_dev.bronze.stg_purchase
# MAGIC UNION ALL
# MAGIC SELECT 'stg_stock_status', COUNT(*) FROM sap_dev.bronze.stg_stock_status;
# MAGIC ```
# MAGIC
# MAGIC **Step 3: Run Dimension Load Notebooks**
# MAGIC
# MAGIC Navigate to `../01_DIMENSIONS/` and run in sequence:
# MAGIC
# MAGIC 1. 01_dim_date.ipynb
# MAGIC 2. 02_dim_product_category.ipynb
# MAGIC 3. 03_dim_warehouse.ipynb
# MAGIC 4. 04_dim_retailer.ipynb
# MAGIC 5. 05_dim_agent.ipynb
# MAGIC 6. 06_dim_payment_mode.ipynb
# MAGIC 7. 07_dim_wholesaler.ipynb
# MAGIC 8. 08_dim_customer_full_truncate.ipynb (NEW: Base customer load before SCD)
# MAGIC 9. 09_dim_customer_scd1.ipynb
# MAGIC 10. 10_dim_customer_scd2.ipynb
# MAGIC 11. 11_dim_product.ipynb
# MAGIC
# MAGIC **Step 4: Run Fact Load Notebooks**
# MAGIC
# MAGIC Navigate to `../02_FACTS/` and run:
# MAGIC
# MAGIC 1. 01_fact_sales.ipynb
# MAGIC 2. 02_fact_purchase.ipynb
# MAGIC 3. 03_fact_stock_status.ipynb
# MAGIC
# MAGIC **Step 5: Test Incremental Loads**
# MAGIC
# MAGIC ```python
# MAGIC # Change widgets:
# MAGIC # - mode: incremental
# MAGIC # - incremental_date: 2026-06-02
# MAGIC # - records_per_day: 500
# MAGIC
# MAGIC # Run incremental cell
# MAGIC # Then re-run fact notebooks to process new data
# MAGIC ```
# MAGIC
# MAGIC **Step 6: Test CDC Streaming**
# MAGIC
# MAGIC ```python
# MAGIC # Change widgets:
# MAGIC # - mode: cdc
# MAGIC # - write_mode: append
# MAGIC
# MAGIC # Run CDC cell to generate customer changes
# MAGIC # Then run SCD notebooks to process changes
# MAGIC ```
# MAGIC
# MAGIC **Step 7: Run Gold Layer Aggregations**
# MAGIC
# MAGIC Navigate to `../04_GOLD/` for business marts.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Troubleshooting
# MAGIC
# MAGIC **Issue: Table already exists error**
# MAGIC - Solution: Set write_mode to "overwrite" or manually drop table
# MAGIC
# MAGIC **Issue: Zero records generated**
# MAGIC - Solution: Check widget values (batch_days > 0, records_per_day > 0)
# MAGIC
# MAGIC **Issue: GST validation fails**
# MAGIC - Solution: Regenerate data (GST calculation might have precision issues)
# MAGIC
# MAGIC **Issue: Foreign key violations in facts**
# MAGIC - Solution: Ensure dimensions are loaded before facts
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Advanced Usage
# MAGIC
# MAGIC **Custom Date Range:**
# MAGIC ```python
# MAGIC start_date_str = "2026-01-01"
# MAGIC df_custom = generate_sales_batch(start_date_str, days=30, records_per_day=2000)
# MAGIC df_custom.write.mode("append").saveAsTable(Config.TBL_STG_SALES)
# MAGIC ```
# MAGIC
# MAGIC **Bulk CDC Generation:**
# MAGIC ```python
# MAGIC for i in range(10):  # 10 batches of CDC events
# MAGIC     df_cdc = generate_customer_cdc(num_changes=100)
# MAGIC     df_cdc.write.mode("append").saveAsTable(Config.TBL_BRONZE_CUSTOMER_CDC)
# MAGIC     print(f"Batch {i+1} complete")
# MAGIC ```
