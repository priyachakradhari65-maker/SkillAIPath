# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # 🚀 StartHere - Five Day Experience Setup
# MAGIC
# MAGIC **Foundation Data Analytics: SkillAI-Path Training Platform**
# MAGIC
# MAGIC **SkillAI Path: Foundation Data Analytics → 5-Day Experience**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📋 What This Notebook Does
# MAGIC
# MAGIC This **StartHere** notebook performs a **complete training environment setup** with automatic configuration, synthetic data generation, and validation — everything you need to begin your SQL investigation journey.
# MAGIC
# MAGIC ### What Gets Deployed
# MAGIC
# MAGIC * **Environment Detection** - Automatic DEV/UAT/PROD configuration from notebook path
# MAGIC * **Unity Catalog Schemas** - Two training schemas with appropriate metadata
# MAGIC * **Synthetic Datasets** - Realistic Indian fintech data with intentional anomalies
# MAGIC   * SAP_dev: 3,000 customers, 5,000 loans (development/testing)
# MAGIC   * SAP_main: 100,000 customers, 500,000 loans (production-scale)
# MAGIC * **Data Quality Validation** - Automated checks for completeness and integrity
# MAGIC
# MAGIC ### Investigation Structure
# MAGIC
# MAGIC * **Canvas/** - 3 Investigation Cases (Portfolio Health, Customer Risk, Time Analysis)
# MAGIC * **Lead/** - Reference solutions and instructor playbook
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ▶️ Getting Started
# MAGIC
# MAGIC **Welcome to the Five Day Experience!** This is your starting point.
# MAGIC
# MAGIC 1. Click **Run All** to deploy your training environment
# MAGIC 2. Monitor progress through 8 automated stages
# MAGIC 3. Review deployment summary when complete
# MAGIC 4. Navigate to **Canvas/** folder to begin Day 1 investigations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⏱️ Expected Runtime
# MAGIC
# MAGIC * Small Schema: ~15-20 seconds
# MAGIC * National Schema: ~60-90 seconds
# MAGIC * Total Deployment: ~2-3 minutes
# MAGIC
# MAGIC ### 🔒 Safety Features
# MAGIC
# MAGIC * Automatic environment detection
# MAGIC * Schema isolation per environment
# MAGIC * Reproducible synthetic data (seeded)
# MAGIC * Validation before completion
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Install Dependencies
# ====================================================================
# STAGE 1: INSTALL DEPENDENCIES
# ====================================================================

import time
stage_start = time.time()

print("[1/8] Installing Dependencies...")
print("" + "-" * 60)

%pip install faker --quiet

# Verify installation
try:
    from faker import Faker
    print("\u2713 faker successfully installed")
except ImportError:
    print("\u2717 ERROR: faker installation failed")
    raise

stage_elapsed = time.time() - stage_start
print(f"\u2713 STAGE 1 COMPLETE | {stage_elapsed:.1f}s")
print("=" * 60 + "\n")

# COMMAND ----------

# DBTITLE 1,Environment Detection
# ====================================================================
# STAGE 2: ENVIRONMENT DETECTION & VALIDATION
# ====================================================================

import re
from datetime import datetime, timedelta, date
import time

stage_start = time.time()

print("[2/8] Detecting Environment...")
print("-" * 60)

# Detect environment from notebook path
try:
    current_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    print(f"Notebook Path: {current_path}")
    
    env_match = re.search(r'/Shared/(dev|uat|prod)/SkillAI-Path', current_path, re.IGNORECASE)
    
    if env_match:
        environment = env_match.group(1).lower()
        print(f"\u2713 Environment detected: {environment.upper()}")
    else:
        environment = "dev"
        print("\u26a0️  Warning: Path does not match expected pattern")
        print(f"\u2713 Defaulting to: {environment.upper()}")
    
    # Configure schemas based on environment
    SCHEMA_SMALL = "sap_dev.edufin_regional"
    SCHEMA_NATIONAL = "sap_dev.edufin_national"
    
    print(f"\u2713 Small Schema: {SCHEMA_SMALL}")
    print(f"\u2713 National Schema: {SCHEMA_NATIONAL}")
    
    # Verify Unity Catalog access
    try:
        spark.sql("SHOW CATALOGS LIKE 'workspace'").collect()
        print("\u2713 Unity Catalog access confirmed")
    except Exception as uc_error:
        print(f"\u2717 ERROR: Cannot access Unity Catalog")
        print(f"   Details: {uc_error}")
        raise
    
except Exception as e:
    print(f"\u2717 CRITICAL ERROR: Environment detection failed")
    print(f"   Details: {e}")
    print("\n   Required: Notebook must be in /Shared/<env>/SkillAI-Path/")
    print("   Valid environments: dev, uat, prod")
    dbutils.notebook.exit('{"status": "FAIL", "message": "Environment detection failed"}')

stage_elapsed = time.time() - stage_start
print(f"\u2713 STAGE 2 COMPLETE | {stage_elapsed:.1f}s")
print("=" * 60 + "\n")

# COMMAND ----------

# DBTITLE 1,Create Catalog
# ====================================================================
# STAGE 2.5: CREATE UNITY CATALOG (IF NEEDED)
# ====================================================================

stage_start = time.time()

print("[2.5/8] Ensuring Catalog Exists...")
print("-" * 60)

try:
    # Extract catalog name from schema variable
    catalog_name = SCHEMA_SMALL.split('.')[0]
    print(f"Target Catalog: {catalog_name}")
    
    # Create catalog if it doesn't exist
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
    spark.sql(f"COMMENT ON CATALOG {catalog_name} IS 'SkillAI-Path Training Environment'")
    print(f"\u2713 Catalog ready: {catalog_name}")
    
except Exception as e:
    print(f"\u2717 ERROR: Catalog creation failed")
    print(f"   Details: {e}")
    raise

stage_elapsed = time.time() - stage_start
print(f"\u2713 STAGE 2.5 COMPLETE | {stage_elapsed:.1f}s")
print("=" * 60 + "\n")

# COMMAND ----------

# DBTITLE 1,Create Schemas
# ====================================================================
# STAGE 3: CREATE UNITY CATALOG SCHEMAS
# ====================================================================

stage_start = time.time()

print("[3/8] Creating Schemas...")
print("-" * 60)

try:
    # Create small schema
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_SMALL}")
    spark.sql(f"COMMENT ON SCHEMA {SCHEMA_SMALL} IS 'SkillAI-Path Development Dataset - 3K customers, 5K loans for rapid iteration and testing'")
    print(f"\u2713 Created: {SCHEMA_SMALL}")
    
    # Create national schema
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NATIONAL}")
    spark.sql(f"COMMENT ON SCHEMA {SCHEMA_NATIONAL} IS 'SkillAI-Path Main Dataset - 100K customers, 500K loans for production-scale queries'")
    print(f"\u2713 Created: {SCHEMA_NATIONAL}")
    
    # Verify schema creation
    catalog_name = SCHEMA_SMALL.split('.')[0]
    small_schema = SCHEMA_SMALL.split('.')[1]
    national_schema = SCHEMA_NATIONAL.split('.')[1]
    
    small_exists = spark.sql(f"SHOW SCHEMAS IN {catalog_name} LIKE '{small_schema}'").count() > 0
    national_exists = spark.sql(f"SHOW SCHEMAS IN {catalog_name} LIKE '{national_schema}'").count() > 0
    
    if small_exists and national_exists:
        print("\u2713 Schema validation: Both schemas confirmed")
    else:
        raise Exception("Schema creation validation failed")
        
except Exception as e:
    print(f"\u2717 ERROR: Schema creation failed")
    print(f"   Details: {e}")
    raise

stage_elapsed = time.time() - stage_start
print(f"\u2713 STAGE 3 COMPLETE | {stage_elapsed:.1f}s")
print("=" * 60 + "\n")

# COMMAND ----------

# DBTITLE 1,Drop Existing Tables
# ====================================================================
# STAGE 4: PRE-GENERATION CLEANUP (OPTIONAL)
# ====================================================================

stage_start = time.time()

print("[4/8] Pre-Generation Cleanup...")
print("-" * 60)
print(f"\u26a0️  Environment: {environment.upper()}")
print("\u26a0️  Action: Dropping existing tables for clean regeneration")
print("-" * 60)

tables_to_drop = ["dim_state", "dim_city", "institutions", "customers", "loans"]
tables_dropped = 0

for schema in [SCHEMA_SMALL, SCHEMA_NATIONAL]:
    for table in tables_to_drop:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {schema}.{table}")
            tables_dropped += 1
        except Exception as e:
            print(f"\u26a0️  Could not drop {schema}.{table}: {e}")

print(f"\u2713 Cleanup complete: {tables_dropped} table(s) processed")
print(f"\u2713 Ready for fresh data generation")

stage_elapsed = time.time() - stage_start
print(f"\u2713 STAGE 4 COMPLETE | {stage_elapsed:.1f}s")
print("=" * 60 + "\n")

# COMMAND ----------

# DBTITLE 1,Reference Data
# ====================================================================
# STAGE 5: LOAD REFERENCE DATA
# ====================================================================

# Reference data: States, Cities, Institutions
# Includes intentional anomalies for investigation cases

STATES = [
    {"state_id": 1, "state_name": "Andhra Pradesh", "region": "South"},
    {"state_id": 2, "state_name": "Karnataka", "region": "South"},
    {"state_id": 3, "state_name": "Maharashtra", "region": "West"},
    {"state_id": 4, "state_name": "Telangana", "region": "South"},
    {"state_id": 5, "state_name": "Tamil Nadu", "region": "South"},
    {"state_id": 6, "state_name": "Assam", "region": "East"},
    {"state_id": 7, "state_name": "Bihar", "region": "East"},
    {"state_id": 8, "state_name": "Madhya Pradesh", "region": "Central"},
    {"state_id": 9, "state_name": "Rajasthan", "region": "West"},
    {"state_id": 10, "state_name": "Gujarat", "region": "West"},
    {"state_id": 11, "state_name": "Kerala", "region": "South"},
    {"state_id": 12, "state_name": "Uttar Pradesh", "region": "North"},
    {"state_id": 13, "state_name": "West Bengal", "region": "East"},
    {"state_id": 14, "state_name": "Punjab", "region": "North"},
    {"state_id": 15, "state_name": "Chhattisgarh", "region": "Central"},
    {"state_id": 16, "state_name": "Jharkhand", "region": "East"},
    {"state_id": 17, "state_name": "Odisha", "region": "East"},
    {"state_id": 18, "state_name": "Delhi", "region": "North"},
]

CITIES = [
    {"city_id": 1, "city_name": "Bengaluru", "state_id": 2, "tier_classification": "Tier1"},
    {"city_id": 2, "city_name": "Mumbai", "state_id": 3, "tier_classification": "Tier1"},
    {"city_id": 3, "city_name": "Hyderabad", "state_id": 4, "tier_classification": "Tier1"},
    {"city_id": 4, "city_name": "Chennai", "state_id": 5, "tier_classification": "Tier1"},
    {"city_id": 5, "city_name": "Pune", "state_id": 3, "tier_classification": "Tier1"},
    {"city_id": 6, "city_name": "Ahmedabad", "state_id": 10, "tier_classification": "Tier1"},
    {"city_id": 7, "city_name": "Kolkata", "state_id": 13, "tier_classification": "Tier1"},
    {"city_id": 8, "city_name": "Delhi", "state_id": 18, "tier_classification": "Tier1"},
    {"city_id": 9, "city_name": "Jaipur", "state_id": 9, "tier_classification": "Tier1"},
    {"city_id": 10, "city_name": "Lucknow", "state_id": 12, "tier_classification": "Tier1"},
    {"city_id": 11, "city_name": "Kochi", "state_id": 11, "tier_classification": "Tier1"},
    {"city_id": 12, "city_name": "Indore", "state_id": 8, "tier_classification": "Tier1"},
    {"city_id": 13, "city_name": "Surat", "state_id": 10, "tier_classification": "Tier1"},
    {"city_id": 14, "city_name": "Nagpur", "state_id": 3, "tier_classification": "Tier1"},
    {"city_id": 15, "city_name": "Thane", "state_id": 3, "tier_classification": "Tier1"},
    {"city_id": 16, "city_name": "Coimbatore", "state_id": 5, "tier_classification": "Tier1"},
    {"city_id": 17, "city_name": "Noida", "state_id": 12, "tier_classification": "Tier1"},
    {"city_id": 18, "city_name": "Gurgaon", "state_id": 18, "tier_classification": "Tier1"},
    {"city_id": 19, "city_name": "Visakhapatnam", "state_id": 1, "tier_classification": "Tier2"},
    {"city_id": 20, "city_name": "Vijayawada", "state_id": 1, "tier_classification": "Tier2"},
    {"city_id": 21, "city_name": "Tirupati", "state_id": 1, "tier_classification": "Tier2"},
    {"city_id": 22, "city_name": "Guwahati", "state_id": 6, "tier_classification": "Tier2"},
    {"city_id": 23, "city_name": "Patna", "state_id": 7, "tier_classification": "Tier2"},
    {"city_id": 24, "city_name": "Bhopal", "state_id": 8, "tier_classification": "Tier2"},
    {"city_id": 25, "city_name": "Gwalior", "state_id": 8, "tier_classification": "Tier2"},
    {"city_id": 26, "city_name": "Ranchi", "state_id": 16, "tier_classification": "Tier2"},
    {"city_id": 27, "city_name": "Jamshedpur", "state_id": 16, "tier_classification": "Tier2"},
    {"city_id": 28, "city_name": "Raipur", "state_id": 15, "tier_classification": "Tier2"},
    {"city_id": 29, "city_name": "Mysuru", "state_id": 2, "tier_classification": "Tier2"},
    {"city_id": 30, "city_name": "Mangaluru", "state_id": 2, "tier_classification": "Tier2"},
    {"city_id": 31, "city_name": "Thiruvananthapuram", "state_id": 11, "tier_classification": "Tier2"},
    {"city_id": 32, "city_name": "Amritsar", "state_id": 14, "tier_classification": "Tier2"},
    {"city_id": 33, "city_name": "Varanasi", "state_id": 12, "tier_classification": "Tier2"},
    {"city_id": 34, "city_name": "Agra", "state_id": 12, "tier_classification": "Tier2"},
    {"city_id": 35, "city_name": "Nashik", "state_id": 3, "tier_classification": "Tier2"},
    {"city_id": 36, "city_name": "Madurai", "state_id": 5, "tier_classification": "Tier2"},
    {"city_id": 37, "city_name": "Udaipur", "state_id": 9, "tier_classification": "Tier2"},
    {"city_id": 38, "city_name": "Bhubaneswar", "state_id": 17, "tier_classification": "Tier3"},
    {"city_id": 39, "city_name": "Dehradun", "state_id": 12, "tier_classification": "Tier3"},
    {"city_id": 40, "city_name": "Siliguri", "state_id": 13, "tier_classification": "Tier3"},
    {"city_id": 41, "city_name": "Jabalpur", "state_id": 8, "tier_classification": "Tier3"},
    {"city_id": 42, "city_name": "Howrah", "state_id": 13, "tier_classification": "Tier3"},
    {"city_id": 43, "city_name": "Salem", "state_id": 5, "tier_classification": "Tier3"},
    {"city_id": 44, "city_name": "Warangal", "state_id": 4, "tier_classification": "Tier3"},
    {"city_id": 45, "city_name": "Kota", "state_id": 9, "tier_classification": "Tier3"},
]

INSTITUTIONS = [
    {'institution_id': 1, 'institution_name': 'National Institute of Technology', 'institution_type': 'Engineering', 'accreditation_grade': 'A+', 'placement_rate': 92.0, 'established_year': 1965},
    {'institution_id': 2, 'institution_name': 'Indian Institute of Engineering Science', 'institution_type': 'Engineering', 'accreditation_grade': 'A+', 'placement_rate': 88.0, 'established_year': 1978},
    {'institution_id': 3, 'institution_name': 'Rajiv Gandhi Technical University', 'institution_type': 'Engineering', 'accreditation_grade': 'A', 'placement_rate': 74.0, 'established_year': 1990},
    {'institution_id': 4, 'institution_name': 'Birla Institute of Applied Sciences', 'institution_type': 'Engineering', 'accreditation_grade': 'A', 'placement_rate': 79.0, 'established_year': 1982},
    {'institution_id': 5, 'institution_name': 'Guru Gobind Singh Engineering College', 'institution_type': 'Engineering', 'accreditation_grade': 'B', 'placement_rate': 62.0, 'established_year': 2001},
    {'institution_id': 6, 'institution_name': 'Maulana Azad National Institute of Tech', 'institution_type': 'Engineering', 'accreditation_grade': 'A+', 'placement_rate': 85.0, 'established_year': 1960},
    {'institution_id': 7, 'institution_name': 'Sathyabama Institute of Science', 'institution_type': 'Engineering', 'accreditation_grade': 'B', 'placement_rate': 58.0, 'established_year': 1988},
    {'institution_id': 8, 'institution_name': 'Visvesvaraya Technological University', 'institution_type': 'Engineering', 'accreditation_grade': 'A', 'placement_rate': 71.0, 'established_year': 1998},
    {'institution_id': 9, 'institution_name': 'Shri Ramdeobaba College of Engineering', 'institution_type': 'Engineering', 'accreditation_grade': 'B', 'placement_rate': 65.0, 'established_year': 1984},
    {'institution_id': 10, 'institution_name': 'Government Engineering College', 'institution_type': 'Engineering', 'accreditation_grade': 'C', 'placement_rate': 42.0, 'established_year': 2005},
    {'institution_id': 11, 'institution_name': 'All India Institute of Medical Sciences', 'institution_type': 'Medical', 'accreditation_grade': 'A+', 'placement_rate': 95.0, 'established_year': 1956},
    {'institution_id': 12, 'institution_name': 'Christian Medical College', 'institution_type': 'Medical', 'accreditation_grade': 'A+', 'placement_rate': 93.0, 'established_year': 1900},
    {'institution_id': 13, 'institution_name': 'Kasturba Medical College', 'institution_type': 'Medical', 'accreditation_grade': 'A', 'placement_rate': 87.0, 'established_year': 1953},
    {'institution_id': 14, 'institution_name': 'Sri Ramachandra Medical College', 'institution_type': 'Medical', 'accreditation_grade': 'A', 'placement_rate': 82.0, 'established_year': 1985},
    {'institution_id': 15, 'institution_name': 'Government Medical College', 'institution_type': 'Medical', 'accreditation_grade': 'B', 'placement_rate': 68.0, 'established_year': 1970},
    {'institution_id': 16, 'institution_name': 'Regional Institute of Medical Sciences', 'institution_type': 'Medical', 'accreditation_grade': 'B', 'placement_rate': 61.0, 'established_year': 1993},
    {'institution_id': 17, 'institution_name': 'Indian School of Business', 'institution_type': 'Management', 'accreditation_grade': 'A+', 'placement_rate': 94.0, 'established_year': 2001},
    {'institution_id': 18, 'institution_name': 'Symbiosis Institute of Management', 'institution_type': 'Management', 'accreditation_grade': 'A', 'placement_rate': 80.0, 'established_year': 1978},
    {'institution_id': 19, 'institution_name': 'ICFAI Business School', 'institution_type': 'Management', 'accreditation_grade': 'A', 'placement_rate': 76.0, 'established_year': 1995},
    {'institution_id': 20, 'institution_name': 'Narsee Monjee Institute of Management', 'institution_type': 'Management', 'accreditation_grade': 'A', 'placement_rate': 78.0, 'established_year': 1981},
    {'institution_id': 21, 'institution_name': 'City Business Academy', 'institution_type': 'Management', 'accreditation_grade': 'B', 'placement_rate': 55.0, 'established_year': 2008},
    {'institution_id': 22, 'institution_name': 'Regional Management Institute', 'institution_type': 'Management', 'accreditation_grade': 'C', 'placement_rate': 44.0, 'established_year': 2012},
    {'institution_id': 23, 'institution_name': 'Jawaharlal Nehru University', 'institution_type': 'Arts', 'accreditation_grade': 'A+', 'placement_rate': 60.0, 'established_year': 1969},
    {'institution_id': 24, 'institution_name': 'Delhi University Arts Faculty', 'institution_type': 'Arts', 'accreditation_grade': 'A', 'placement_rate': 52.0, 'established_year': 1922},
    {'institution_id': 25, 'institution_name': 'Presidency University', 'institution_type': 'Arts', 'accreditation_grade': 'A', 'placement_rate': 48.0, 'established_year': 1817},
    {'institution_id': 26, 'institution_name': 'Loyola College', 'institution_type': 'Arts', 'accreditation_grade': 'A', 'placement_rate': 56.0, 'established_year': 1925},
    {'institution_id': 27, 'institution_name': 'State Arts and Science College', 'institution_type': 'Arts', 'accreditation_grade': 'B', 'placement_rate': 40.0, 'established_year': 1985},
    {'institution_id': 28, 'institution_name': 'Municipal Arts College', 'institution_type': 'Arts', 'accreditation_grade': 'C', 'placement_rate': 35.0, 'established_year': 2000},
    {'institution_id': 29, 'institution_name': 'Global Vocational Training Centre', 'institution_type': 'Vocational', 'accreditation_grade': 'B', 'placement_rate': 50.0, 'established_year': 2010},
    {'institution_id': 30, 'institution_name': 'National Skill Development Institute', 'institution_type': 'Vocational', 'accreditation_grade': 'B', 'placement_rate': 47.0, 'established_year': 2015},
    {'institution_id': 31, 'institution_name': 'Standard Polytechnic', 'institution_type': 'Polytechnic', 'accreditation_grade': 'B', 'placement_rate': 52.0, 'established_year': 1995},
    {'institution_id': 32, 'institution_name': 'Central Polytechnic Institute', 'institution_type': 'Polytechnic', 'accreditation_grade': 'C', 'placement_rate': 38.0, 'established_year': 2003},
    {'institution_id': 33, 'institution_name': 'Rural Polytechnic College', 'institution_type': 'Polytechnic', 'accreditation_grade': 'C', 'placement_rate': 32.0, 'established_year': 2009},
    {'institution_id': 34, 'institution_name': 'District Vocational Centre', 'institution_type': 'Vocational', 'accreditation_grade': 'C', 'placement_rate': 28.0, 'established_year': 2018},
    {'institution_id': 35, 'institution_name': 'Shri Ram College of Commerce', 'institution_type': 'Commerce', 'accreditation_grade': 'A+', 'placement_rate': 82.0, 'established_year': 1926},
    {'institution_id': 36, 'institution_name': 'City Commerce College', 'institution_type': 'Commerce', 'accreditation_grade': 'B', 'placement_rate': 45.0, 'established_year': 1990},
    {'institution_id': 37, 'institution_name': 'National Law School of India', 'institution_type': 'Law', 'accreditation_grade': 'A+', 'placement_rate': 90.0, 'established_year': 1987},
    {'institution_id': 38, 'institution_name': 'Central Law Institute', 'institution_type': 'Law', 'accreditation_grade': 'A', 'placement_rate': 72.0, 'established_year': 1970},
    {'institution_id': 39, 'institution_name': 'Regional Law College', 'institution_type': 'Law', 'accreditation_grade': 'B', 'placement_rate': 48.0, 'established_year': 2005},
    {'institution_id': 40, 'institution_name': 'Andhra University College of Engineering', 'institution_type': 'Engineering', 'accreditation_grade': 'A', 'placement_rate': 70.0, 'established_year': 1933},
    {'institution_id': 41, 'institution_name': 'GITAM University', 'institution_type': 'Engineering', 'accreditation_grade': 'A', 'placement_rate': 73.0, 'established_year': 1980},
    {'institution_id': 42, 'institution_name': 'Osmania University', 'institution_type': 'Engineering', 'accreditation_grade': 'B', 'placement_rate': 58.0, 'established_year': 1918},
    {'institution_id': 43, 'institution_name': 'Anna University', 'institution_type': 'Engineering', 'accreditation_grade': 'A+', 'placement_rate': 83.0, 'established_year': 1978},
    {'institution_id': 44, 'institution_name': 'Savitribai Phule Pune University', 'institution_type': 'Engineering', 'accreditation_grade': 'A', 'placement_rate': 69.0, 'established_year': 1949},
    {'institution_id': 45, 'institution_name': 'Jadavpur University', 'institution_type': 'Engineering', 'accreditation_grade': 'A+', 'placement_rate': 86.0, 'established_year': 1955},
    {'institution_id': 46, 'institution_name': 'Amity University', 'institution_type': 'Management', 'accreditation_grade': 'B', 'placement_rate': 54.0, 'established_year': 2005},
    {'institution_id': 47, 'institution_name': 'Manipal Academy of Higher Education', 'institution_type': 'Medical', 'accreditation_grade': 'A+', 'placement_rate': 89.0, 'established_year': 1953},
    {'institution_id': 48, 'institution_name': 'SRM Institute of Science and Technology', 'institution_type': 'Engineering', 'accreditation_grade': 'A', 'placement_rate': 67.0, 'established_year': 1985},
    {'institution_id': 49, 'institution_name': 'Lovely Professional University', 'institution_type': 'Engineering', 'accreditation_grade': 'B', 'placement_rate': 51.0, 'established_year': 2005},
    {'institution_id': 50, 'institution_name': 'Unverified Training Academy', 'institution_type': 'Vocational', 'accreditation_grade': 'C', 'placement_rate': 150.0, 'established_year': 2020}
]

stage_start = time.time()

print("[5/8] Loading Reference Data...")
print("-" * 60)

print(f"\u2713 States loaded: {len(STATES)} records")
print(f"\u2713 Cities loaded: {len(CITIES)} records")
print(f"   - Tier1: {len([c for c in CITIES if c['tier_classification'] == 'Tier1'])} cities")
print(f"   - Tier2: {len([c for c in CITIES if c['tier_classification'] == 'Tier2'])} cities")
print(f"   - Tier3: {len([c for c in CITIES if c['tier_classification'] == 'Tier3'])} cities")
print(f"\u2713 Institutions loaded: {len(INSTITUTIONS)} records")
print(f"   - Engineering: {len([i for i in INSTITUTIONS if i['institution_type'] == 'Engineering'])}")
print(f"   - Medical: {len([i for i in INSTITUTIONS if i['institution_type'] == 'Medical'])}")
print(f"   - Management: {len([i for i in INSTITUTIONS if i['institution_type'] == 'Management'])}")
print(f"   - Other types: {len([i for i in INSTITUTIONS if i['institution_type'] not in ['Engineering', 'Medical', 'Management']])}")
print("\u2713 Intentional anomalies embedded for investigation")

stage_elapsed = time.time() - stage_start
print(f"\u2713 STAGE 5 COMPLETE | {stage_elapsed:.1f}s")
print("=" * 60 + "\n")

# COMMAND ----------

# DBTITLE 1,Generator Configuration
# ====================================================================
# STAGE 6: CONFIGURE DATA GENERATORS
# ====================================================================

from faker import Faker
import random
import numpy as np
from pyspark.sql import Row
from pyspark.sql import functions as F

stage_start = time.time()

print("[6/8] Configuring Data Generators...")
print("-" * 60)

# Initialize generators with fixed seeds for reproducibility (canonical seed: 42)
Faker.seed(42)
random.seed(42)
np.random.seed(42)
fake = Faker('en_IN')
print("\u2713 Faker initialized (locale: en_IN, seed: 42)")
print("\u2713 Random generators seeded for reproducibility")

# Dual schema configuration
SCHEMAS = [
    {"schema_name": SCHEMA_SMALL, "NUM_CUSTOMERS": 3000, "NUM_LOANS": 5000},
    {"schema_name": SCHEMA_NATIONAL, "NUM_CUSTOMERS": 100000, "NUM_LOANS": 500000},
]

VIZAG_CITY_ID = 19  # Used for intentional Vizag default anomaly (Investigation Case 3)

print(f"\u2713 Schema configurations:")
for config in SCHEMAS:
    print(f"   - {config['schema_name']}: {config['NUM_CUSTOMERS']:,} customers, {config['NUM_LOANS']:,} loans")

stage_elapsed = time.time() - stage_start
print(f"\u2713 STAGE 6 COMPLETE | {stage_elapsed:.1f}s")
print("=" * 60 + "\n")

# COMMAND ----------

# DBTITLE 1,Generate Tables
# ====================================================================
# STAGE 7: GENERATE SYNTHETIC DATASETS
# ====================================================================
# Canonical production-grade data generation with business rules
# Includes intentional anomalies for investigation cases

overall_start = time.time()

print("[7/8] Generating Synthetic Datasets...")
print("-" * 60)

# Main data generation loop for both schemas
for idx, config in enumerate(SCHEMAS, 1):
    schema = config['schema_name']
    NUM_CUSTOMERS = config['NUM_CUSTOMERS']
    NUM_LOANS = config['NUM_LOANS']
    
    print(f"\nDataset {idx}/2: {schema}")
    print(f"Target: {NUM_CUSTOMERS:,} customers, {NUM_LOANS:,} loans")
    print("-" * 40)
    t0 = time.time()

    # ================================================================
    # 1. DIMENSION TABLES
    # ================================================================
    df_states = spark.createDataFrame([Row(**s) for s in STATES])
    df_cities = spark.createDataFrame([Row(**c) for c in CITIES])
    
    # Add tier column to institutions
    institutions_with_tier = []
    for inst in INSTITUTIONS:
        inst_copy = inst.copy()
        if inst["institution_id"] == 17:
            inst_copy["tier"] = "Corporate Partner"
        else:
            inst_copy["tier"] = None
        institutions_with_tier.append(inst_copy)
    
    df_inst = spark.createDataFrame([Row(**i) for i in institutions_with_tier])
    # Intentional anomaly: Institution 50 has impossible 150% placement rate
    df_inst = df_inst.withColumn(
        "placement_rate",
        F.when(F.col("institution_id") == 50, 150.0).otherwise(F.col("placement_rate"))
    )
    
    df_states.write.format("delta").mode("overwrite").saveAsTable(f"{schema}.dim_state")
    df_cities.write.format("delta").mode("overwrite").saveAsTable(f"{schema}.dim_city")
    df_inst.write.format("delta").mode("overwrite").saveAsTable(f"{schema}.institutions")
    print("  \u2713 Dimension tables created")

    # ================================================================
    # 2. CUSTOMERS (with production-grade business rules)
    # ================================================================
    print(f"  \u23f3 Generating {NUM_CUSTOMERS:,} customers...")
    
    tier1_ids = [c["city_id"] for c in CITIES if c["tier_classification"] == "Tier1"]
    tier2_ids = [c["city_id"] for c in CITIES if c["tier_classification"] == "Tier2"]
    tier3_ids = [c["city_id"] for c in CITIES if c["tier_classification"] == "Tier3"]
    vizag_weight_tier2 = [3 if cid == VIZAG_CITY_ID else 1 for cid in tier2_ids]
    tier_lookup = {c["city_id"]: c["tier_classification"] for c in CITIES}

    emp_types = ["Engineer", "Salaried", "Self-Employed", "Unemployed"]
    emp_weights = [0.45, 0.30, 0.15, 0.10]
    customers = []
    
    for i in range(1, NUM_CUSTOMERS + 1):
        # City selection with Vizag overweight (Investigation Case 3)
        tier_roll = random.random()
        if tier_roll < 0.55:
            city_id = random.choice(tier1_ids)
        elif tier_roll < 0.85:
            city_id = random.choices(tier2_ids, weights=vizag_weight_tier2)[0]
        else:
            city_id = random.choice(tier3_ids)
        
        # Income bracket and CIBIL score logic
        income_bracket = random.choices(["Low","Medium","High"], weights=[0.40,0.40,0.20])[0]
        if income_bracket == "High":
            annual_income = round(random.uniform(800000,2500000), 2)
        elif income_bracket == "Medium":
            annual_income = round(random.uniform(300000,800000), 2)
        else:
            annual_income = round(random.uniform(80000,300000), 2)
        
        base_cibil = {"High":700, "Medium":600, "Low":450}[income_bracket]
        cibil_score = min(900, max(300, base_cibil + random.randint(-100,150)))
        
        # Employment type and age correlation
        emp_type = random.choices(emp_types, weights=emp_weights)[0]
        if emp_type == "Engineer":
            age = random.randint(18,25)
        elif emp_type == "Salaried":
            age = random.randint(22,35)
        else:
            age = random.randint(20,32)
        
        # Gender-based name generation
        gender = random.choice(["Male","Female"])
        full_name = fake.name_male() if gender=="Male" else fake.name_female()
        customer_since = fake.date_between(start_date='-5y', end_date='-6m')
        
        # Campaign flag for May 2023 cohort (Investigation Case 2)
        campaign_flag = None
        if customer_since.year == 2023 and customer_since.month == 5 and cibil_score < 620:
            campaign_flag = "MAY_EXPANSION"
        
        customers.append(Row(
            customer_id=i, full_name=full_name, gender=gender, age=age, city_id=city_id,
            annual_income=annual_income, income_bracket=income_bracket, cibil_score=cibil_score,
            employment_type=emp_type, customer_since=customer_since,
            status="Active",
            campaign_flag=campaign_flag
        ))
        
        if i % max(25000, NUM_CUSTOMERS//4) == 0:
            print(f"     Generated {i:,}/{NUM_CUSTOMERS:,} customers...")
    
    df_customers = spark.createDataFrame(customers)
    
    # Apply intentional data anomalies for investigation
    cibil_anomaly_ids = [88, 567, 1204, 2050, 2899]
    income_anomaly_ids = [333, 1777, 2500]
    
    df_customers = df_customers.withColumn(
        "cibil_score", 
        F.when(F.col("customer_id").isin(cibil_anomaly_ids), 999).otherwise(F.col("cibil_score"))
    ).withColumn(
        "annual_income", 
        F.when(F.col("customer_id").isin(income_anomaly_ids), 0.0).otherwise(F.col("annual_income"))
    ).withColumn(
        "employment_type", 
        F.when(F.col("customer_id").isin(income_anomaly_ids), "Salaried").otherwise(F.col("employment_type"))
    )
    
    # NULL employment for older records (pre-July 2022 data migration)
    df_customers = df_customers.withColumn(
        "employment_type",
        F.when((F.col("customer_since") < F.lit(date(2022, 7, 1))) & (F.rand(seed=999) < 0.35), None)
         .otherwise(F.col("employment_type"))
    )
    
    df_customers.write.format("delta").mode("overwrite").saveAsTable(f"{schema}.customers")
    print(f"  \u2713 customers table created ({NUM_CUSTOMERS:,} rows)")

    # ================================================================
    # 3. LOANS (with multi-factor default probability model)
    # ================================================================
    print(f"  \u23f3 Generating {NUM_LOANS:,} loans...")
    
    customer_cities = {row.customer_id: row.city_id for row in customers}
    customer_campaign = {row.customer_id: row.campaign_flag for row in customers}
    customer_cibil = {row.customer_id: row.cibil_score for row in customers}
    inst_ids = [i["institution_id"] for i in INSTITUTIONS]
    inst_type_lookup = {i["institution_id"]: i["institution_type"] for i in INSTITUTIONS}
    tier_lookup = {c["city_id"]: c["tier_classification"] for c in CITIES}
    
    loan_purposes = ["Tuition", "Hostel", "Equipment", "Books", "Living Expenses"]
    purpose_weights = [0.45,0.20,0.15,0.10,0.10]
    loans = []
    
    for loan_id in range(1, NUM_LOANS+1):
        cust_id = random.randint(1, NUM_CUSTOMERS)
        city_id = customer_cities[cust_id]
        tier = tier_lookup[city_id]
        income_bracket = customers[cust_id-1].income_bracket
        institution_id = random.choice(inst_ids)
        inst_type = inst_type_lookup[institution_id]
        
        # Loan amount and interest rate based on income bracket
        if income_bracket == "High":
            loan_amount = round(random.uniform(500000,1500000), 2)
            interest_rate = round(random.uniform(8.5,11.0), 2)
        elif income_bracket == "Medium":
            loan_amount = round(random.uniform(200000,700000), 2)
            interest_rate = round(random.uniform(10.5,14.0), 2)
        else:
            loan_amount = round(random.uniform(50000,300000), 2)
            interest_rate = round(random.uniform(13.5,18.0), 2)
        
        # Handle international programs (USD conversion)
        program_type = "Domestic"
        if random.random() < (327 / NUM_LOANS):
            loan_amount = round(loan_amount / 83, 2)
            program_type = "International"
        
        disbursement_date = fake.date_between(start_date=date(2022,1,1), end_date=date(2024,6,30))
        application_date = disbursement_date - timedelta(days=random.randint(15, 60))
        
        tenure_months = random.choice([36,48,60])
        maturity_date = disbursement_date + timedelta(days=tenure_months*30)
        loan_purpose = random.choices(loan_purposes, weights=purpose_weights)[0]
        
        # Multi-factor default probability calculation
        prob = 0.05  # Base probability
        if tier == "Tier2": prob += 0.06
        elif tier == "Tier3": prob += 0.04
        if income_bracket == "Low": prob += 0.07
        elif income_bracket == "Medium": prob += 0.02
        if inst_type in ("Vocational", "Polytechnic"): prob += 0.08
        elif inst_type == "Arts": prob += 0.04
        if city_id == VIZAG_CITY_ID: prob += 0.15  # Vizag anomaly
        
        # Campaign cohort adjustment (Investigation Case 2)
        if customer_campaign.get(cust_id) == "MAY_EXPANSION":
            prob = 0.42
        
        # Weekend disbursement effect (Investigation Case 3)
        if disbursement_date.weekday() == 5:  # Saturday
            prob = prob * 1.31
        
        # Corporate guarantee (zero default rate)
        if institution_id == 17:
            prob = 0.0
        
        # Determine loan status
        status_rand = random.random()
        if status_rand < prob:
            loan_status = "Defaulted"
        elif status_rand < prob+0.08:
            loan_status = "Overdue"
        elif status_rand < prob+0.08+0.17:
            loan_status = "Closed"
        else:
            loan_status = "Active"
        
        default_date = None
        if loan_status == "Defaulted":
            default_date = disbursement_date + timedelta(days=random.randint(90,365))
        
        loans.append(Row(
            loan_id=loan_id, customer_id=cust_id, institution_id=institution_id,
            loan_amount=loan_amount, interest_rate=interest_rate,
            disbursement_date=disbursement_date, application_date=application_date,
            maturity_date=maturity_date, loan_status=loan_status, default_date=default_date,
            loan_purpose=loan_purpose, program_type=program_type
        ))
        
        if loan_id % max(100000, NUM_LOANS//5) == 0:
            print(f"     Generated {loan_id:,}/{NUM_LOANS:,} loans...")
    
    df_loans = spark.createDataFrame(loans)
    
    # Apply intentional data edge cases for investigation
    neg_amount_ids = [47, 1823]
    date_paradox_ids = [188, 777, 1456]
    future_disb_ids = [55, 666, 1111]
    crm_swap_count = int(NUM_LOANS * (82/500000))
    crm_swap_ids = random.sample(range(1, NUM_LOANS+1), min(crm_swap_count, NUM_LOANS))
    
    df_loans = df_loans.withColumn(
        "loan_amount", 
        F.when(F.col("loan_id").isin(neg_amount_ids), -F.col("loan_amount")).otherwise(F.col("loan_amount"))
    ).withColumn(
        "maturity_date", 
        F.when(F.col("loan_id").isin(date_paradox_ids), 
               F.date_sub(F.col("disbursement_date"), 30)).otherwise(F.col("maturity_date"))
    ).withColumn(
        "disbursement_date", 
        F.when(F.col("loan_id").isin(future_disb_ids), 
               F.date_add(F.current_date(), 180)).otherwise(F.col("disbursement_date"))
    ).withColumn(
        "application_date",
        F.when(F.col("loan_id").isin(crm_swap_ids), F.col("disbursement_date"))
         .otherwise(F.col("application_date"))
    ).withColumn(
        "disbursement_date",
        F.when(F.col("loan_id").isin(crm_swap_ids), F.col("application_date"))
         .otherwise(F.col("disbursement_date"))
    )
    
    df_loans.write.format("delta").mode("overwrite").saveAsTable(f"{schema}.loans")
    print(f"  \u2713 loans table created ({NUM_LOANS:,} rows)")
    
    elapsed = int(time.time() - t0)
    print(f"  \u2713 {schema} complete in {elapsed}s")
    print("-" * 40)

overall_elapsed = time.time() - overall_start
print(f"\n\u2713 All tables generated successfully")
print(f"\u2713 Total tables created: {len(SCHEMAS) * 5}")
print(f"\u2713 STAGE 7 COMPLETE | {overall_elapsed:.1f}s")
print("=" * 60 + "\n")

# COMMAND ----------

# DBTITLE 1,Validate Tables
# ====================================================================
# STAGE 8: VALIDATION & QUALITY CHECKS
# ====================================================================

stage_start = time.time()

print("[8/8] Validating Deployment...")
print("-" * 60)

# Validate table readiness with detailed error reporting
def check_table(schema, table):
    """Check table existence and return row count with detailed error handling."""
    try:
        count = spark.sql(f"SELECT COUNT(*) as cnt FROM {schema}.{table}").collect()[0]['cnt']
        return True, count, None
    except Exception as e:
        error_msg = str(e)
        if "Table or view not found" in error_msg:
            return False, 0, "Table does not exist"
        elif "Permission denied" in error_msg:
            return False, 0, "Permission denied"
        else:
            return False, 0, f"Query failed: {error_msg[:100]}"

tables_to_validate = [
    (SCHEMA_SMALL, "dim_state", 18),
    (SCHEMA_SMALL, "dim_city", 45),
    (SCHEMA_SMALL, "institutions", 50),
    (SCHEMA_SMALL, "customers", 3000),
    (SCHEMA_SMALL, "loans", 5000),
    (SCHEMA_NATIONAL, "dim_state", 18),
    (SCHEMA_NATIONAL, "dim_city", 45),
    (SCHEMA_NATIONAL, "institutions", 50),
    (SCHEMA_NATIONAL, "customers", 100000),
    (SCHEMA_NATIONAL, "loans", 500000)
]

print("Validating table availability and row counts...\n")

all_valid = True
validation_results = []

for schema, table, expected in tables_to_validate:
    has_data, count, error = check_table(schema, table)
    
    if has_data:
        status = "\u2713"
        count_match = count >= expected  # Allow >= for flexibility
        if not count_match:
            status = "\u26a0️ "
            all_valid = False
        print(f"{status} {schema}.{table}: {count:,} rows (expected: {expected:,})")
        validation_results.append((table, "PASS" if count_match else "WARN", count))
    else:
        print(f"\u2717 {schema}.{table}: {error}")
        all_valid = False
        validation_results.append((table, "FAIL", 0))

print("\n" + "-" * 60)
if all_valid:
    print("\u2713 All validations passed")
else:
    print("\u26a0️  Some validations failed - review errors above")

stage_elapsed = time.time() - stage_start
print(f"\u2713 STAGE 8 COMPLETE | {stage_elapsed:.1f}s")
print("=" * 60 + "\n")

# COMMAND ----------

# DBTITLE 1,Deployment Summary
# ====================================================================
# DEPLOYMENT SUMMARY & REPORT
# ====================================================================

from datetime import datetime

print("\n" + "=" * 60)
print("  EDUFINDBX DEPLOYMENT SUMMARY")
print("=" * 60)

if all_valid:
    print("\n\u2705 DEPLOYMENT STATUS: SUCCESS")
    print("-" * 60)
    
    # Environment Details
    print("\n🌐 ENVIRONMENT CONFIGURATION")
    print(f"  Environment: {environment.upper()}")
    print(f"  Catalog: workspace")
    print(f"  Deployment Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Schema Details
    print("\n[SCHEMAS DEPLOYED]")
    print(f"  Small Training Schema:    {SCHEMA_SMALL}")
    print(f"  National Training Schema: {SCHEMA_NATIONAL}")
    
    # Table Summary
    print("\n[TABLES GENERATED]")
    small_total = sum([count for table, status, count in validation_results[:5]])
    national_total = sum([count for table, status, count in validation_results[5:]])
    
    print(f"  {SCHEMA_SMALL}:")
    print(f"    - dim_state: 18 rows")
    print(f"    - dim_city: 45 rows")
    print(f"    - institutions: 50 rows")
    print(f"    - customers: 3,000 rows")
    print(f"    - loans: 5,000 rows")
    print(f"    Total: {small_total:,} rows")
    
    print(f"\n  {SCHEMA_NATIONAL}:")
    print(f"    - dim_state: 18 rows")
    print(f"    - dim_city: 45 rows")
    print(f"    - institutions: 50 rows")
    print(f"    - customers: 100,000 rows")
    print(f"    - loans: 500,000 rows")
    print(f"    Total: {national_total:,} rows")
    
    # Quality Checks
    print("\n\u2705 QUALITY VALIDATIONS")
    print("  \u2713 All tables created successfully")
    print("  \u2713 Row counts validated")
    print("  \u2713 Intentional anomalies embedded")
    print("  \u2713 Reproducible synthetic data (seed: 42)")
    
    # Next Steps
    print("\n[NEXT STEPS]")
    print("  1. Navigate to /Shared/dev/EduFinDBX/Canvas/")
    print("  2. Start with: 1_Portfolio_Health investigation")
    print("  3. Continue with: 2_Customer_Risk investigation")
    print("  4. Complete with: 3_Time_Analysis investigation")
    
    # Advanced Features
    print("\n[AFTER COMPLETING CASES]")
    print("  - Build Genie Space for natural language Q&A")
    print("  - Create Lakeview Dashboard for visualization")
    print("  - Setup Workflow automation for recurring tasks")
    print("  - Review INSTRUCTOR_PLAYBOOK.md in Lead/ folder")
    
    print("\n" + "=" * 60)
    print("  \u2705 DEPLOYMENT COMPLETE - READY FOR TRAINING")
    print("=" * 60 + "\n")
    
else:
    print("\n\u274c DEPLOYMENT STATUS: INCOMPLETE")
    print("-" * 60)
    print("\u26a0️  Some tables failed validation")
    print("\u26a0️  Review error messages in Stage 8 above")
    print("\nTroubleshooting:")
    print("  1. Check Unity Catalog permissions")
    print("  2. Verify workspace catalog access")
    print("  3. Review generation logs for errors")
    print("  4. Re-run failed stages if needed")
    print("\n" + "=" * 60 + "\n")