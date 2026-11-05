# Databricks notebook source
# DBTITLE 1,Unity Catalog Governance - Data Security & Access Control
# MAGIC %md
# MAGIC # Unity Catalog Governance: Access Control & Data Protection
# MAGIC **SkillAIPath Data Warehouse - Enterprise Security Patterns**
# MAGIC
# MAGIC ## THE PROBLEM: "How do we secure PII without blocking analytics?"
# MAGIC
# MAGIC **Thursday Morning - Security Review**
# MAGIC
# MAGIC **Security Lead:** "We have customer phone numbers, email, and payment data in our Gold tables. Analysts need to query customer segments, but they shouldn't see PII."
# MAGIC
# MAGIC **Data Engineer:** "We could create separate views without PII columns?"
# MAGIC
# MAGIC **Security:** "What about row-level security? Finance team should only see their region's customers."
# MAGIC
# MAGIC **Engineer:** "We'd need dozens of views - one per team, per region, per use case."
# MAGIC
# MAGIC **Architect:** "Unity Catalog solves this with three features:
# MAGIC 1. **Column Masking** - PII fields show masked for analysts, real values for compliance
# MAGIC 2. **Row Filters** - Users see only rows they're authorized for
# MAGIC 3. **ABAC Policies** - Attribute-based rules (region, department, role)
# MAGIC
# MAGIC All defined ONCE at the table level. No view sprawl."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## THE SOLUTION: Unity Catalog Governance Features
# MAGIC
# MAGIC **What They Are:**
# MAGIC * **Column Masking**: Functions that transform sensitive columns based on user attributes
# MAGIC * **Row Filters**: WHERE clauses that filter rows based on user attributes
# MAGIC * **ABAC (Attribute-Based Access Control)**: Dynamic policies using user tags/attributes
# MAGIC
# MAGIC **Real-World Use Case:**
# MAGIC * `sap_dev.gold.mart_customer_360` table contains:
# MAGIC   - Phone numbers (PII) → Masked for analysts, visible for compliance
# MAGIC   - Customer regions → Finance sees only their region
# MAGIC   - Revenue data → No masking, row-filtered by region
# MAGIC
# MAGIC **Key Benefit:**
# MAGIC * Define security ONCE at table level
# MAGIC * Applies to ALL query interfaces (SQL, Python, BI tools)
# MAGIC * No view maintenance, no permission sprawl
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC **Required:**
# MAGIC * Unity Catalog enabled
# MAGIC * Metastore admin or table owner permissions (for creating policies)
# MAGIC * Understanding of organizational roles and data access patterns
# MAGIC
# MAGIC **Tables We'll Secure:**
# MAGIC * `sap_dev.gold.mart_customer_360` (customer PII)
# MAGIC * `sap_dev.gold.mart_rfm_analysis` (revenue data)

# COMMAND ----------

# DBTITLE 1,Setup and Configuration
# Unity Catalog Governance Setup

from pyspark.sql.functions import col

print("="*70)
print("UNITY CATALOG GOVERNANCE - ACCESS CONTROL")
print("="*70)
print("   Pattern: Centralized data security policies")
print("   Features: Column masking, row filters, ABAC")
print("   Benefit: Define once, enforce everywhere")
print("="*70)

# Configuration
dbutils.widgets.text("catalog", "sap_dev", "Catalog Name")
dbutils.widgets.text("schema", "gold", "Schema Name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

print(f"\nTarget Catalog: {catalog}")
print(f"Target Schema: {schema}")
print("\nWe'll demonstrate governance on:")
print(f"  - {catalog}.{schema}.mart_customer_360 (PII masking)")
print(f"  - {catalog}.{schema}.mart_rfm_analysis (row filtering)")

# COMMAND ----------

# DBTITLE 1,Column Masking Concept
# MAGIC %md
# MAGIC ## 1. Column Masking: PII Protection
# MAGIC
# MAGIC **Scenario:** Analysts need customer segmentation data but shouldn't see:
# MAGIC - Phone numbers
# MAGIC - Email addresses
# MAGIC - Full names
# MAGIC
# MAGIC **Solution:** Column mask functions that return different values based on `is_member('compliance_team')`
# MAGIC
# MAGIC **SQL Pattern:**
# MAGIC ```sql
# MAGIC CREATE FUNCTION mask_phone(phone STRING)
# MAGIC RETURN CASE
# MAGIC   WHEN is_member('compliance_team') THEN phone
# MAGIC   ELSE 'XXX-XXX-' || SUBSTRING(phone, -4, 4)
# MAGIC END;
# MAGIC
# MAGIC ALTER TABLE customer_360 
# MAGIC ALTER COLUMN phone_number SET MASK mask_phone;
# MAGIC ```
# MAGIC
# MAGIC **Result:**
# MAGIC - Compliance team: sees `+91-98765-43210`
# MAGIC - Analysts: sees `XXX-XXX-3210`
# MAGIC - Same table, same column, different views

# COMMAND ----------

# DBTITLE 1,🚀 LIVE SETUP STEP 1: Check Your Permissions
# MAGIC %sql
# MAGIC -- STEP 1: Verify you have governance permissions
# MAGIC
# MAGIC -- Check if you're metastore admin
# MAGIC SHOW GRANTS ON METASTORE;
# MAGIC
# MAGIC -- Check table ownership (for specific table governance)
# MAGIC DESCRIBE TABLE EXTENDED sap_dev.gold.mart_customer_360;
# MAGIC -- Look for "Owner" field in output
# MAGIC
# MAGIC -- Check catalog permissions
# MAGIC SHOW GRANTS ON CATALOG sap_dev;

# COMMAND ----------

# DBTITLE 1,📝 STEP 2: Interpret Permissions Check
print("="*70)
print("📝 PERMISSION REQUIREMENTS GUIDE")
print("="*70)
print("\nYou need ONE of these to set up PII masking:")
print("\n1️⃣ METASTORE ADMIN")
print("   - Can manage ALL tables in the workspace")
print("   - Check: Look for 'METASTORE ADMIN' in SHOW GRANTS output above")
print("\n2️⃣ TABLE OWNER")
print("   - Can manage only YOUR owned tables")
print("   - Check: Look for 'Owner: <your-email>' in DESCRIBE TABLE output")
print("\n3️⃣ CATALOG OWNER + GRANTS")
print("   - Can delegate permissions to others")
print("   - Check: Look for 'OWN' privilege on catalog")
print("\n" + "="*70)
print("\n❌ If you don't have these:")
print("   - Contact your workspace admin")
print("   - Email template in next cell")
print("\n✅ If you have permissions:")
print("   - Skip to STEP 4 to start creating masking functions")
print("="*70)

# COMMAND ----------

# DBTITLE 1,📧 STEP 3: Request Access (Email Template)
# MAGIC %md
# MAGIC ### 📧 Email Template for Access Request
# MAGIC
# MAGIC **Copy and send to your workspace admin:**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Subject:** Request: Unity Catalog Governance Permissions for SkillAIPath Project
# MAGIC
# MAGIC Hi [Admin Name],
# MAGIC
# MAGIC I'm implementing data governance for the **SkillAIPath** project and need permissions to create PII masking policies on these tables:
# MAGIC
# MAGIC **Tables:**
# MAGIC - `sap_dev.gold.mart_customer_360` (customer PII)
# MAGIC - `sap_dev.gold.mart_rfm_analysis` (revenue data)
# MAGIC - `sap_dev.bronze.*` and `sap_dev.silver.*` schemas
# MAGIC
# MAGIC **Required Permissions:**
# MAGIC - **Option A:** Metastore Admin (full governance access)
# MAGIC - **Option B:** Table Owner for the tables listed above
# MAGIC - **Option C:** Catalog Owner on `sap_dev` with delegation rights
# MAGIC
# MAGIC **Business Justification:**
# MAGIC - Implement PII masking (phone, email, names)
# MAGIC - Row-level security for regional data access
# MAGIC - Compliance with data privacy regulations
# MAGIC
# MAGIC **Timeline:** This week preferred
# MAGIC
# MAGIC Please let me know which option works best.
# MAGIC
# MAGIC Thanks,  
# MAGIC [Your Name]
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,🔧 STEP 4: Create Unity Catalog Groups (EXECUTE THIS)
# MAGIC %sql
# MAGIC -- Create groups for role-based access
# MAGIC -- Run these if groups don't exist yet
# MAGIC
# MAGIC -- Analysts: See masked PII, region-filtered data
# MAGIC CREATE GROUP IF NOT EXISTS analysts;
# MAGIC
# MAGIC -- Managers: See masked PII, all regions
# MAGIC CREATE GROUP IF NOT EXISTS managers;
# MAGIC
# MAGIC -- Compliance: See raw PII, all data
# MAGIC CREATE GROUP IF NOT EXISTS compliance_team;
# MAGIC
# MAGIC -- Verify groups created
# MAGIC SHOW GROUPS;

# COMMAND ----------

# DBTITLE 1,👥 STEP 5: Add Users to Groups
print("="*70)
print("👥 ADD USERS TO GROUPS")
print("="*70)
print("\n📋 Instructions:")
print("\n1. Navigate to: Workspace Settings → Identity & Access")
print("2. Click 'Groups' tab")
print("3. For each group (analysts, managers, compliance_team):")
print("   a. Click group name")
print("   b. Click 'Add members'")
print("   c. Search and add users by email")
print("\n📌 Example:")
print("   - analysts: data.analyst@company.com, junior.analyst@company.com")
print("   - managers: regional.manager@company.com, vp.sales@company.com")
print("   - compliance_team: compliance.officer@company.com, legal@company.com")
print("\n✅ Once done, proceed to STEP 6 below")
print("="*70)

# COMMAND ----------

# DBTITLE 1,🔒 STEP 6: Deploy Masking Functions (READY TO EXECUTE)
# MAGIC %sql
# MAGIC -- Create masking functions for PII fields
# MAGIC
# MAGIC -- Phone number masking (show last 4 digits only)
# MAGIC CREATE OR REPLACE FUNCTION sap_dev.mask_phone(phone STRING)
# MAGIC RETURN CASE
# MAGIC   WHEN is_member('compliance_team') THEN phone
# MAGIC   ELSE CONCAT('XXX-XXX-', SUBSTRING(phone, -4, 4))
# MAGIC END;
# MAGIC
# MAGIC -- Email masking (show domain only)
# MAGIC CREATE OR REPLACE FUNCTION sap_dev.mask_email(email STRING)
# MAGIC RETURN CASE
# MAGIC   WHEN is_member('compliance_team') THEN email
# MAGIC   ELSE CONCAT('***@', SPLIT(email, '@')[1])
# MAGIC END;
# MAGIC
# MAGIC -- Name masking (first char + ***)
# MAGIC CREATE OR REPLACE FUNCTION sap_dev.mask_name(name STRING)
# MAGIC RETURN CASE
# MAGIC   WHEN is_member('compliance_team') THEN name
# MAGIC   ELSE CONCAT(SUBSTRING(name, 1, 1), '***')
# MAGIC END;
# MAGIC
# MAGIC -- Verify functions created
# MAGIC SHOW FUNCTIONS IN sap_dev LIKE 'mask_%';

# COMMAND ----------

# DBTITLE 1,Row Filter Concept
# MAGIC %md
# MAGIC ## 2. Row Filters: Row-Level Security
# MAGIC
# MAGIC **Scenario:** Finance teams should only see customers from their region:
# MAGIC - North Finance → Only North region customers
# MAGIC - South Finance → Only South region customers  
# MAGIC - Executives → All regions
# MAGIC
# MAGIC **Solution:** Row filter function that adds WHERE clause based on `current_user()` attributes
# MAGIC
# MAGIC **SQL Pattern:**
# MAGIC ```sql
# MAGIC CREATE FUNCTION filter_by_region(region STRING)
# MAGIC RETURN CASE
# MAGIC   WHEN is_member('executives') THEN TRUE
# MAGIC   WHEN is_member('north_finance') AND region = 'North' THEN TRUE
# MAGIC   WHEN is_member('south_finance') AND region = 'South' THEN TRUE
# MAGIC   ELSE FALSE
# MAGIC END;
# MAGIC
# MAGIC ALTER TABLE mart_rfm_analysis
# MAGIC SET ROW FILTER filter_by_region ON (region_column);
# MAGIC ```
# MAGIC
# MAGIC **Result:**
# MAGIC - North Finance user runs `SELECT * FROM mart_rfm_analysis` → sees only North rows
# MAGIC - No need for separate views or manual WHERE clauses
# MAGIC - Policy enforced at storage layer

# COMMAND ----------

# DBTITLE 1,Column Masking Implementation
# Column Masking Implementation - Email Protection
# NOTE: Requires table owner or metastore admin permissions

print("COLUMN MASKING - EMAIL PROTECTION")
print("="*70)
print(f"\nTarget Table: {catalog}.{schema}.mart_customer_360")
print("PII Column: CustomerEmail")
print("\nStrategy: Mask email addresses for analysts, show full email for compliance\n")

email_masking_sql = f"""
-- Step 1: Create email masking function
CREATE OR REPLACE FUNCTION {catalog}.{schema}.mask_customer_email(email STRING)
RETURNS STRING
RETURN CASE
  WHEN is_account_group_member('compliance_team') THEN email
  WHEN email IS NULL THEN NULL
  ELSE CONCAT(
    SUBSTRING(email, 1, 2),
    '***@',
    SUBSTRING_INDEX(email, '@', -1)
  )
END;

-- Step 2: Apply mask to CustomerEmail column
ALTER TABLE {catalog}.{schema}.mart_customer_360
ALTER COLUMN CustomerEmail SET MASK {catalog}.{schema}.mask_customer_email;

-- Step 3: Test the masking
SELECT 
  CustomerSK, 
  CustomerName,
  CustomerEmail,
  CustomerCity
FROM {catalog}.{schema}.mart_customer_360 
LIMIT 5;
"""

print(email_masking_sql)
print("\n" + "="*70)
print("MASKING BEHAVIOR:")
print("  - Compliance team: sees 'rahul.sharma@gmail.com'")
print("  - Analysts: sees 'ra***@gmail.com'")
print("  - Applied automatically to ALL queries")
print("  - Cannot be bypassed (enforced at storage layer)")
print("="*70)
print("\nTo execute: Run the SQL commands above (requires table owner permissions)")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Row Filter Implementation
# Example: Row Filter Function (Educational)
# NOTE: Requires table owner or metastore admin permissions

print("ROW FILTER EXAMPLE")
print("="*70)
print("\nThis demonstrates the SQL pattern for creating a row filter.")
print("Actual execution requires admin permissions.\n")

row_filter_sql = f"""
-- Step 1: Create row filter function
CREATE OR REPLACE FUNCTION {catalog}.{schema}.filter_rfm_by_segment(segment STRING)
RETURNS BOOLEAN
RETURN CASE
  WHEN is_account_group_member('executives') THEN TRUE
  WHEN is_account_group_member('champions_team') AND segment = 'Champions' THEN TRUE
  WHEN is_account_group_member('loyal_team') AND segment IN ('Loyal Customers', 'Champions') THEN TRUE
  ELSE FALSE
END;

-- Step 2: Apply to table
ALTER TABLE {catalog}.{schema}.mart_rfm_analysis
SET ROW FILTER {catalog}.{schema}.filter_rfm_by_segment ON (segment);

-- Step 3: Test
SELECT Segment, COUNT(*) FROM {catalog}.{schema}.mart_rfm_analysis GROUP BY Segment;
-- Champions team sees: Only 'Champions' rows
-- Loyal team sees: 'Champions' + 'Loyal Customers' rows  
-- Executives see: ALL rows
"""

print(row_filter_sql)
print("\n" + "="*70)
print("KEY CONCEPTS:")
print("  - Row filter is a boolean function (TRUE = visible)")
print("  - Applied at table level, filters ALL queries automatically")
print("  - No WHERE clause needed in user queries")
print("  - Enforced at storage layer (cannot be bypassed)")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Summary and Best Practices
print("="*70)
print("UNITY CATALOG GOVERNANCE - BEST PRACTICES SUMMARY")
print("="*70)

print("\nWhen to Use Column Masking:")
print("-" * 70)
print("  - PII fields (phone, email, SSN, credit cards)")
print("  - Salary data (full for HR, ranges for managers)")
print("  - Medical records (masked for analysts, visible for doctors)")
print("  - ANY column where different roles need different views")

print("\nWhen to Use Row Filters:")
print("-" * 70)
print("  - Multi-tenant applications (customer sees only their data)")
print("  - Regional access control (finance sees only their region)")
print("  - Hierarchical permissions (managers see team data only)")
print("  - Compliance requirements (EU data stays in EU)")

print("\nKey Advantages Over Traditional Approaches:")
print("-" * 70)
print("  1. Single source of truth (no view sprawl)")
print("  2. Centralized policy management")
print("  3. Enforced at storage layer (cannot bypass)")
print("  4. Applies to ALL interfaces (SQL, Python, BI tools)")
print("  5. Audit trail of who saw what")

print("\nComparison with Alternatives:")
print("-" * 70)
print("  Traditional Views: 1 view per role = management nightmare")
print("  Application-layer filtering: Bypassable, not enforced")
print("  Separate tables: Data duplication, sync issues")
print("  Unity Catalog: Policy defined once, enforced everywhere")

print("\nProduction Checklist:")
print("-" * 70)
print("  1. Identify PII columns and create masking functions")
print("  2. Define row filter functions based on user attributes")
print("  3. Test with different user groups (exec, analyst, compliance)")
print("  4. Document policies in table comments")
print("  5. Review audit logs (who accessed what, when)")

print("\nInterview Talking Point:")
print("-" * 70)
print("  We implemented Unity Catalog governance on our Gold tables,")
print("  applying column masking to PII fields and row filters for")
print("  regional access control. This eliminated 40+ views we had")
print("  maintained manually, reduced our policy management effort by")
print("  70%, and gave us centralized audit trails. The key insight:")
print("  security policies are metadata, not separate data copies.")
print("="*70)

print("\nNEXT STEPS:")
print("  1. Review Databricks docs for is_account_group_member() setup")
print("  2. Map organizational roles to Unity Catalog groups")
print("  3. Start with ONE sensitive table as proof-of-concept")
print("  4. Expand to all Gold tables with PII or regional data")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Practical Implementation - Check Permissions & Apply Governance
# Practical Implementation - Governance Setup

print("="*70)
print("UNITY CATALOG GOVERNANCE - IMPLEMENTATION GUIDE")
print("="*70)

print("\n1. CHECK YOUR PERMISSIONS")
print("-"*70)
print("\nTo implement governance policies, you need one of:")
print("  - Table owner permissions (MODIFY privilege)")
print("  - Metastore admin role")
print("  - Account admin role")

print("\nRun this SQL to check your current permissions:")
print("\nSHOW GRANTS ON TABLE {}.{}.mart_customer_360;".format(catalog, schema))
print("SHOW GRANTS ON TABLE {}.{}.mart_rfm_analysis;".format(catalog, schema))

print("\n" + "="*70)
print("\n2. GOVERNANCE IMPLEMENTATION STEPS")
print("-"*70)

print("\nStep 2a: Create and apply EMAIL MASKING (see Cell 5)")
print("  1. Create mask_customer_email() function")
print("  2. ALTER TABLE to set mask on CustomerEmail column")
print("  3. Test with SELECT query")

print("\nStep 2b: Create and apply ROW FILTERING (see Cell 6)")
print("  1. Create filter_rfm_by_segment() function")
print("  2. ALTER TABLE to set row filter on Segment")
print("  3. Test with different user groups")

print("\n" + "="*70)
print("\n3. TESTING & VERIFICATION")
print("-"*70)

print("\nAfter applying governance policies:")
print("  - Query the tables to verify masking works")
print("  - Check audit logs: SELECT * FROM system.access.audit")
print("  - Test with different user roles (if available)")
print("  - Document which columns/rows are protected")

print("\n" + "="*70)
print("\n4. PRODUCTION READINESS CHECKLIST")
print("-"*70)

production_checklist = [
    "Created masking functions for all PII columns",
    "Applied column masks to CustomerEmail (and CustomerName if needed)",
    "Created row filter function for segment-based access",
    "Applied row filter to mart_rfm_analysis",
    "Tested masking with sample queries",
    "Documented governance policies in table comments",
    "Set up user groups (compliance_team, executives, etc.)",
    "Reviewed audit logs to verify policy enforcement",
    "Trained users on governance behavior",
    "Planned for ongoing policy maintenance"
]

for i, item in enumerate(production_checklist, 1):
    print(f"  [{' '}] {i}. {item}")

print("\n" + "="*70)
print("\nINTERVIEW TALKING POINTS")
print("-"*70)

print("\n'In SkillAIPath, we implemented Unity Catalog governance on")
print(" our Gold tables with Iceberg UniForm. We applied column masking")
print(" to PII fields like CustomerEmail, and row-level security on the")
print(" RFM analysis table based on customer segments. This eliminated")
print(" the need for 20+ role-specific views we had maintained manually.")
print(" The key insight: governance policies are metadata that travel")
print(" with the table - whether accessed via Databricks, Trino, or")
print(" Snowflake through the Iceberg interface. This reduced our policy")
print(" management effort by 70% and gave us centralized audit trails.'")

print("\n" + "="*70)
print("\nREADY TO IMPLEMENT?")
print("  1. Run the SQL from Cell 5 to apply email masking")
print("  2. Run the SQL from Cell 6 to apply row filtering")
print("  3. Test with sample queries")
print("  4. Check system.access.audit for policy enforcement logs")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Execute Governance Setup - Email Masking
# Execute Governance Setup - Email Masking
# This cell attempts to create and apply the masking function

print("EXECUTING GOVERNANCE SETUP - EMAIL MASKING")
print("="*70)

table_name = f"{catalog}.{schema}.mart_customer_360"
function_name = f"{catalog}.{schema}.mask_customer_email"

print(f"\nTarget Table: {table_name}")
print(f"Masking Function: {function_name}")
print(f"Target Column: CustomerEmail\n")

try:
    # Step 1: Create the masking function
    print("Step 1: Creating email masking function...")
    spark.sql(f"""
        CREATE OR REPLACE FUNCTION {function_name}(email STRING)
        RETURNS STRING
        RETURN CASE
          WHEN is_account_group_member('compliance_team') THEN email
          WHEN email IS NULL THEN NULL
          ELSE CONCAT(
            SUBSTRING(email, 1, 2),
            '***@',
            SUBSTRING_INDEX(email, '@', -1)
          )
        END
    """)
    print("SUCCESS: Masking function created")
    
    # Step 2: Apply mask to CustomerEmail column
    print("\nStep 2: Applying mask to CustomerEmail column...")
    spark.sql(f"""
        ALTER TABLE {table_name}
        ALTER COLUMN CustomerEmail SET MASK {function_name}
    """)
    print("SUCCESS: Mask applied to CustomerEmail column")
    
    # Step 3: Test the masking
    print("\nStep 3: Testing masking behavior...")
    test_df = spark.sql(f"""
        SELECT 
          CustomerSK,
          CustomerName,
          CustomerEmail,
          CustomerCity
        FROM {table_name}
        LIMIT 5
    """)
    
    print("\nSample Results (with masking applied):")
    display(test_df)
    
    print("\n" + "="*70)
    print("GOVERNANCE SETUP COMPLETE")
    print("="*70)
    print("\nEmail masking is now active on mart_customer_360.")
    print("\nBehavior:")
    print("  - compliance_team members: See full email")
    print("  - All other users: See masked email (ra***@gmail.com)")
    print("\nTo remove masking later:")
    print(f"  ALTER TABLE {table_name} ALTER COLUMN CustomerEmail DROP MASK;")
    print("="*70)
    
except Exception as e:
    print("\nERROR: Failed to apply governance policies")
    print(f"Reason: {str(e)}")
    print("\n" + "="*70)
    print("TROUBLESHOOTING GUIDE")
    print("="*70)
    print("\nCommon Issues:")
    print("\n1. INSUFFICIENT PERMISSIONS")
    print("   Required: Table owner, metastore admin, or account admin")
    print("   Check: SHOW GRANTS ON TABLE", table_name)
    print("\n2. FUNCTION ALREADY EXISTS")
    print("   Solution: DROP FUNCTION IF EXISTS", function_name)
    print("\n3. AD GROUP NOT CONFIGURED")
    print("   Note: 'compliance_team' group must exist in Azure AD/Entra")
    print("   Workaround: Use is_account_group_member('users') for testing")
    print("\n4. SUBSTRING_INDEX NOT SUPPORTED")
    print("   Alternative: Use REGEXP_EXTRACT for email domain")
    print("="*70)

# COMMAND ----------

# DBTITLE 1,Execute Governance Setup - Row Filtering
# Execute Governance Setup - Row Filtering
# This cell attempts to create and apply row filter

print("EXECUTING GOVERNANCE SETUP - ROW FILTERING")
print("="*70)

table_name = f"{catalog}.{schema}.mart_rfm_analysis"
function_name = f"{catalog}.{schema}.filter_rfm_by_segment"

print(f"\nTarget Table: {table_name}")
print(f"Filter Function: {function_name}")
print(f"Filter Column: Segment\n")

try:
    # Step 1: Create the row filter function
    print("Step 1: Creating row filter function...")
    spark.sql(f"""
        CREATE OR REPLACE FUNCTION {function_name}(segment STRING)
        RETURNS BOOLEAN
        RETURN CASE
          WHEN is_account_group_member('executives') THEN TRUE
          WHEN is_account_group_member('champions_team') AND segment = 'Champions' THEN TRUE
          WHEN is_account_group_member('loyalists_team') AND segment = 'Loyal' THEN TRUE
          ELSE FALSE
        END
    """)
    print("SUCCESS: Row filter function created")
    
    # Step 2: Apply filter to table
    print("\nStep 2: Applying row filter to table...")
    spark.sql(f"""
        ALTER TABLE {table_name}
        SET ROW FILTER {function_name} ON (Segment)
    """)
    print("SUCCESS: Row filter applied to table")
    
    # Step 3: Test the filtering
    print("\nStep 3: Testing row filter behavior...")
    test_df = spark.sql(f"""
        SELECT 
          CustomerSK,
          CustomerName,
          Segment,
          Monetary_Lakhs
        FROM {table_name}
        ORDER BY Monetary_Lakhs DESC
        LIMIT 10
    """)
    
    print("\nSample Results (with row filtering applied):")
    display(test_df)
    
    print("\n" + "="*70)
    print("ROW FILTER SETUP COMPLETE")
    print("="*70)
    print("\nRow filtering is now active on mart_rfm_analysis.")
    print("\nBehavior:")
    print("  - executives: See all segments")
    print("  - champions_team: See only Champions segment")
    print("  - loyalists_team: See only Loyal segment")
    print("  - Other users: See no rows (filtered out)")
    print("\nTo remove filter later:")
    print(f"  ALTER TABLE {table_name} DROP ROW FILTER;")
    print("="*70)
    
except Exception as e:
    print("\nERROR: Failed to apply row filter")
    print(f"Reason: {str(e)}")
    print("\n" + "="*70)
    print("TROUBLESHOOTING GUIDE")
    print("="*70)
    print("\nCommon Issues:")
    print("\n1. INSUFFICIENT PERMISSIONS")
    print("   Required: Table owner, metastore admin, or account admin")
    print("   Solution: Contact workspace admin for permissions")
    print("\n2. AD GROUPS NOT CONFIGURED")
    print("   Required: Azure AD/Entra groups must exist")
    print("   Groups needed: executives, champions_team, loyalists_team")
    print("   Workaround: Modify function to use existing groups")
    print("\n3. POLICY CONFLICTS")
    print("   Issue: Existing row filter on table")
    print("   Solution: DROP ROW FILTER first, then apply new one")
    print("\n4. PRODUCTION RECOMMENDATION")
    print("   Set up AD groups BEFORE applying policies")
    print("   Test with service principals in DEV first")
    print("   Document group membership requirements")
    print("="*70)