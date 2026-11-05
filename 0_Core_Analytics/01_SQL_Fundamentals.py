# Databricks notebook source
# DBTITLE 0,SQL Entry Qualifier for EduFin
# MAGIC %md
# MAGIC # SQL for Financial Analysis
# MAGIC
# MAGIC > **Transform from SQL beginner to confident analyst in 3-4 hours**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Who This Is For
# MAGIC
# MAGIC * New to SQL and want practical query skills
# MAGIC * Need SQL for data analysis roles
# MAGIC * Want to work with real business data
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What You'll Build
# MAGIC
# MAGIC * **CTEs** - Break complex logic into readable steps
# MAGIC * **ROW_NUMBER** - Rank data within categories (top N per group)
# MAGIC * **SUM OVER** - Calculate running totals for trend analysis
# MAGIC * **LAG** - Compare periods (month-over-month growth)
# MAGIC
# MAGIC **Real dataset:** 5,000 loan records with disbursements, defaults, customer profiles, and geographic data.
# MAGIC
# MAGIC **Time investment:** 3-4 hours (skip SQL Basics section if you know SELECT, WHERE, JOIN, GROUP BY)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What's Inside
# MAGIC
# MAGIC **SQL Basics Review (15 minutes)** - Essential commands: SELECT, WHERE, JOIN, GROUP BY  
# MAGIC **Core Techniques (90 minutes)** - CTEs, ROW_NUMBER, SUM OVER, LAG with working examples  
# MAGIC **Practice Queries (90 minutes)** - 10 progressively challenging business scenarios  
# MAGIC **Activity Tracking (5 minutes)** - Log your progress and completion  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Ready? Run Cell 5 below to generate the training dataset, then continue to Cell 7 for environment validation.**

# COMMAND ----------

# DBTITLE 1,SQL Entry Qualifier for EduFin
# MAGIC %md
# MAGIC # Terms of Use
# MAGIC © 2026 SkillAI Path · Free Educational Material
# MAGIC
# MAGIC **You CAN:**
# MAGIC * Use for learning and practice (unlimited access)
# MAGIC * Share the download link with others learning SQL
# MAGIC * Include completion in portfolio/resume ("Completed SQL for Financial Analysis via SkillAI Path")
# MAGIC * Reference techniques in interviews
# MAGIC * Use code patterns in personal/commercial projects
# MAGIC
# MAGIC **You CANNOT:**
# MAGIC * Repackage and sell as your own course
# MAGIC * Remove attribution/copyright notices
# MAGIC * Use SkillAI Path branding without permission
# MAGIC * Claim authorship of this content
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Your Progress:**
# MAGIC Complete all 10 practice queries in Part 3 to assess your SQL skills. Your final score (Cell 77) shows how you performed.
# MAGIC
# MAGIC **Questions or Support:**
# MAGIC Email: tech@skillaipath.com
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **By continuing, you acknowledge these terms.**

# COMMAND ----------

# DBTITLE 1,⚙️ One-Time Data Setup
# MAGIC %md
# MAGIC # ⚙️ One-Time Setup: Generate Training Dataset
# MAGIC
# MAGIC **ACTION REQUIRED: Run Cell 5 below ONCE before proceeding**
# MAGIC
# MAGIC This creates:
# MAGIC * 5,000 loan records (realistic patterns: Active, Defaulted, Overdue, Closed)
# MAGIC * 5,000 customer profiles (names, employment, CIBIL scores)
# MAGIC * Geographic reference tables (cities and states)
# MAGIC
# MAGIC **Time:** ~60 seconds  
# MAGIC **Storage:** workspace.sap_fin.*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ⚠️ **If you've already run this before, skip to Cell 7 (Environment Validation)**

# COMMAND ----------

# DBTITLE 1,Initialize Session Variables
from datetime import datetime

if "session_start" not in globals():
    session_start = datetime.now()

if "total_score" not in globals():
    total_score = 0

if "queries_completed" not in globals():
    queries_completed = 0

# COMMAND ----------

# DBTITLE 1,⚙️ Run This First - Data Setup
# MAGIC %md
# MAGIC # Run This to Get Data to Work On
# MAGIC
# MAGIC **ACTION REQUIRED:** Run the cell below ONCE before starting the exercises.
# MAGIC
# MAGIC **What it does:**
# MAGIC * Creates sample financial dataset (5,000 loans, 5,000 customers, 50 cities, 28 states)
# MAGIC * Takes ~60 seconds to complete
# MAGIC * Stores data in `workspace.sap_fin.*`
# MAGIC
# MAGIC **After running:** Continue to Cell 8 (Environment Validation) to verify everything is ready.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Generate Sample Data (Run Once)
# DATA GENERATION - Run this cell once to create the sample dataset
# This creates all tables needed for the SQL exercises

import random
from datetime import datetime, timedelta

# Set random seed for consistent results across runs
random.seed(42)

print("[INFO] Starting data generation...\n")

# Create catalog and schema
spark.sql("CREATE CATALOG IF NOT EXISTS workspace")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.sap_fin")
print("[✓] Catalog and schema ready")

# Drop existing tables to handle schema conflicts
spark.sql("DROP TABLE IF EXISTS workspace.sap_fin.dim_state")
spark.sql("DROP TABLE IF EXISTS workspace.sap_fin.dim_city")
spark.sql("DROP TABLE IF EXISTS workspace.sap_fin.customers")
spark.sql("DROP TABLE IF EXISTS workspace.sap_fin.loans")
print("[✓] Cleaned up existing tables")

# Generate dim_state (28 Indian states)
states = [
    (1, 'Maharashtra'), (2, 'Karnataka'), (3, 'Tamil Nadu'), (4, 'Gujarat'),
    (5, 'Rajasthan'), (6, 'Uttar Pradesh'), (7, 'West Bengal'), (8, 'Madhya Pradesh'),
    (9, 'Andhra Pradesh'), (10, 'Telangana'), (11, 'Kerala'), (12, 'Punjab'),
    (13, 'Haryana'), (14, 'Delhi'), (15, 'Jharkhand'), (16, 'Odisha'),
    (17, 'Chhattisgarh'), (18, 'Uttarakhand'), (19, 'Himachal Pradesh'), (20, 'Goa'),
    (21, 'Assam'), (22, 'Bihar'), (23, 'Jammu and Kashmir'), (24, 'Manipur'),
    (25, 'Meghalaya'), (26, 'Nagaland'), (27, 'Sikkim'), (28, 'Tripura')
]
df_state = spark.createDataFrame(states, ['state_id', 'state_name'])
df_state.write.mode('overwrite').saveAsTable('workspace.sap_fin.dim_state')
print("[✓] Generated 28 states")

# Generate dim_city (50 cities)
cities = [
    (1, 'Mumbai', 1), (2, 'Pune', 1), (3, 'Bangalore', 2), (4, 'Mysore', 2),
    (5, 'Chennai', 3), (6, 'Coimbatore', 3), (7, 'Ahmedabad', 4), (8, 'Surat', 4),
    (9, 'Jaipur', 5), (10, 'Udaipur', 5), (11, 'Lucknow', 6), (12, 'Kanpur', 6),
    (13, 'Kolkata', 7), (14, 'Howrah', 7), (15, 'Indore', 8), (16, 'Bhopal', 8),
    (17, 'Visakhapatnam', 9), (18, 'Vijayawada', 9), (19, 'Hyderabad', 10), (20, 'Warangal', 10),
    (21, 'Kochi', 11), (22, 'Thiruvananthapuram', 11), (23, 'Amritsar', 12), (24, 'Ludhiana', 12),
    (25, 'Gurgaon', 13), (26, 'Faridabad', 13), (27, 'New Delhi', 14), (28, 'Noida', 14),
    (29, 'Ranchi', 15), (30, 'Jamshedpur', 15), (31, 'Bhubaneswar', 16), (32, 'Cuttack', 16),
    (33, 'Raipur', 17), (34, 'Bilaspur', 17), (35, 'Dehradun', 18), (36, 'Haridwar', 18),
    (37, 'Shimla', 19), (38, 'Manali', 19), (39, 'Panaji', 20), (40, 'Margao', 20),
    (41, 'Guwahati', 21), (42, 'Dibrugarh', 21), (43, 'Patna', 22), (44, 'Gaya', 22),
    (45, 'Srinagar', 23), (46, 'Jammu', 23), (47, 'Imphal', 24), (48, 'Shillong', 25),
    (49, 'Kohima', 26), (50, 'Gangtok', 27)
]
df_city = spark.createDataFrame(cities, ['city_id', 'city_name', 'state_id'])
df_city.write.mode('overwrite').saveAsTable('workspace.sap_fin.dim_city')
print("[✓] Generated 50 cities")

# Generate customers (5,000)
first_names = ['Raj', 'Priya', 'Amit', 'Sneha', 'Vikram', 'Ananya', 'Rahul', 'Pooja', 'Arjun', 'Kavya']
last_names = ['Sharma', 'Patel', 'Singh', 'Kumar', 'Reddy', 'Iyer', 'Gupta', 'Nair', 'Mehta', 'Verma']
employment_types = ['Salaried', 'Self-Employed', 'Business Owner', 'Professional']

customer_data = []
for i in range(1, 5001):
    customer_data.append((
        i,
        f"{random.choice(first_names)} {random.choice(last_names)}",
        random.choice(['Male', 'Female']),
        random.randint(22, 65),
        random.randint(1, 50),
        random.randint(200000, 5000000),
        'High' if random.random() > 0.7 else ('Medium' if random.random() > 0.4 else 'Low'),
        random.randint(550, 850),
        random.choice(employment_types),
        (datetime.now() - timedelta(days=random.randint(180, 1825))).strftime('%Y-%m-%d'),
        'Active' if random.random() > 0.1 else 'Inactive',
        random.choice([True, False])
    ))

df_customers = spark.createDataFrame(customer_data, [
    'customer_id', 'full_name', 'gender', 'age', 'city_id', 'annual_income',
    'income_bracket', 'cibil_score', 'employment_type', 'customer_since', 'status', 'campaign_flag'
])
df_customers.write.mode('overwrite').saveAsTable('workspace.sap_fin.customers')
print("[✓] Generated 5,000 customers")

# Generate loans (5,000)
loan_types = ['Personal', 'Home', 'Auto', 'Education', 'Business']
loan_statuses = ['Active', 'Closed', 'Defaulted', 'Overdue']
source_channels = ['Online', 'Branch', 'Partner', 'Direct Sales']

loan_data = []
for i in range(1, 5001):
    customer_id = random.randint(1, 5000)
    loan_amount = random.randint(50000, 2000000)
    tenure = random.choice([12, 24, 36, 48, 60])
    monthly_emi = round(loan_amount / tenure, 2)
    disbursement_date = (datetime.now() - timedelta(days=random.randint(30, 1095))).strftime('%Y-%m-%d')
    
    # Status distribution: 45% Active, 25% Closed, 20% Defaulted, 10% Overdue
    rand = random.random()
    if rand < 0.45:
        status = 'Active'
        default_date = None
    elif rand < 0.70:
        status = 'Closed'
        default_date = None
    elif rand < 0.90:
        status = 'Defaulted'
        disb_dt = datetime.strptime(disbursement_date, '%Y-%m-%d')
        default_date = (disb_dt + timedelta(days=random.randint(90, 365))).strftime('%Y-%m-%d')
    else:
        status = 'Overdue'
        default_date = None
    
    loan_data.append((
        i,
        customer_id,
        loan_amount,
        random.choice(loan_types),
        tenure,
        monthly_emi,
        disbursement_date,
        status,
        default_date,
        random.choice(source_channels)
    ))

df_loans = spark.createDataFrame(loan_data, [
    'loan_id', 'customer_id', 'loan_amount', 'loan_type', 'tenure_months',
    'monthly_emi', 'disbursement_date', 'loan_status', 'default_date', 'source_channel'
])
df_loans.write.mode('overwrite').saveAsTable('workspace.sap_fin.loans')
print("[✓] Generated 5,000 loans")

# Validation
validation = spark.sql("""
    SELECT 'loans' AS table_name, COUNT(*) AS row_count FROM workspace.sap_fin.loans
    UNION ALL
    SELECT 'customers', COUNT(*) FROM workspace.sap_fin.customers
    UNION ALL
    SELECT 'dim_city', COUNT(*) FROM workspace.sap_fin.dim_city
    UNION ALL
    SELECT 'dim_state', COUNT(*) FROM workspace.sap_fin.dim_state
""").collect()

print("\n" + "="*50)
print("[SUCCESS] Data generation complete!")
print("="*50)
for row in validation:
    print(f"  {row.table_name}: {row.row_count:,} rows")
print("\nNext Step: Continue to Cell 10 (Environment Validation)")

# COMMAND ----------

# DBTITLE 1,Environment Validation
# ACTION REQUIRED: Run this cell to:
# 1. Validate dataset (5,000 loans, 5,000 customers)
# 2. Create temp views (loans, customers, dim_city, dim_state)
#
# This takes ~5 seconds. You'll see confirmation messages below.

df_loans = spark.table('workspace.sap_fin.loans')
df_customers = spark.table('workspace.sap_fin.customers')
df_city = spark.table('workspace.sap_fin.dim_city')
df_state = spark.table('workspace.sap_fin.dim_state')

# Validate counts
loan_count = df_loans.count()
customer_count = df_customers.count()

assert loan_count == 5000, f"Expected 5,000 loans, found {loan_count}"
assert customer_count == 5000, f"Expected 5,000 customers, found {customer_count}"

print(f"[PASS] Dataset validated ({loan_count:,} loans, {customer_count:,} customers)")

# Create temp views for SQL queries
df_loans.createOrReplaceTempView('loans')
df_customers.createOrReplaceTempView('customers')
df_city.createOrReplaceTempView('dim_city')
df_state.createOrReplaceTempView('dim_state')

print("[PASS] Temp views created: loans, customers, dim_city, dim_state")

print("\n" + "="*50)
print("[PASS] ENVIRONMENT READY")
print("="*50)
print("\nNext Step: Continue to Cell 9 (SQL Basics Review)\n")

# COMMAND ----------

# DBTITLE 1,Prerequisites Check
# MAGIC %md
# MAGIC # SQL Basics Review
# MAGIC
# MAGIC **New to SQL?** This section covers the fundamentals in 15 minutes.
# MAGIC
# MAGIC **Already know SQL?** Skip to Cell 14 (Core Techniques)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What is SQL?
# MAGIC
# MAGIC SQL (Structured Query Language) lets you ask questions about data stored in tables.
# MAGIC
# MAGIC **Think of a table like an Excel spreadsheet:**
# MAGIC - Rows = individual records (like one loan)
# MAGIC - Columns = attributes (like loan_amount, customer_name)
# MAGIC
# MAGIC **Core idea:** Write queries that tell the database what you want, and it gives you back the matching rows.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## The 4 Essential SQL Commands
# MAGIC
# MAGIC Everything else builds on these:
# MAGIC
# MAGIC 1. **SELECT** - Choose which columns to see
# MAGIC 2. **WHERE** - Filter which rows to include
# MAGIC 3. **GROUP BY** - Summarize data (counts, averages, sums)
# MAGIC 4. **JOIN** - Combine data from multiple tables
# MAGIC
# MAGIC **Run the 4 examples below to see how they work.**

# COMMAND ----------

# DBTITLE 1,Example 1: Simple SELECT
# MAGIC %sql
# MAGIC -- See the first 10 loans in the dataset
# MAGIC -- This shows you what columns exist and what the data looks like
# MAGIC
# MAGIC SELECT 
# MAGIC   loan_id,
# MAGIC   customer_id,
# MAGIC   loan_amount,
# MAGIC   loan_status,
# MAGIC   disbursement_date
# MAGIC FROM workspace.sap_fin.loans
# MAGIC LIMIT 10;
# MAGIC
# MAGIC -- Try: Change LIMIT 10 to LIMIT 5

# COMMAND ----------

# DBTITLE 1,Example 2: Filter with WHERE
# MAGIC %sql
# MAGIC -- Show only Defaulted loans
# MAGIC -- WHERE filters rows to match your condition
# MAGIC
# MAGIC SELECT 
# MAGIC   loan_id,
# MAGIC   customer_id,
# MAGIC   loan_amount,
# MAGIC   loan_status,
# MAGIC   default_date
# MAGIC FROM workspace.sap_fin.loans
# MAGIC WHERE loan_status = 'Defaulted'
# MAGIC LIMIT 10;
# MAGIC
# MAGIC -- Try: Change 'Defaulted' to 'Active' and re-run

# COMMAND ----------

# DBTITLE 1,Example 3: Aggregate with GROUP BY
# MAGIC %sql
# MAGIC -- Count how many loans exist in each status category
# MAGIC -- GROUP BY + COUNT(*) gives you summary statistics
# MAGIC
# MAGIC SELECT 
# MAGIC   loan_status,
# MAGIC   COUNT(*) AS loan_count,
# MAGIC   SUM(loan_amount) AS total_amount,
# MAGIC   ROUND(AVG(loan_amount), 2) AS avg_amount
# MAGIC FROM workspace.sap_fin.loans
# MAGIC GROUP BY loan_status
# MAGIC ORDER BY loan_count DESC;
# MAGIC
# MAGIC -- This shows distribution across Active, Defaulted, Overdue, Closed
# MAGIC -- Try: Remove ORDER BY and see what changes

# COMMAND ----------

# DBTITLE 1,Example 4: JOIN Two Tables
# MAGIC %sql
# MAGIC -- Combine customer names with their loan details
# MAGIC -- JOIN connects tables using a common column (customer_id)
# MAGIC
# MAGIC SELECT 
# MAGIC   c.full_name,
# MAGIC   c.employment_type,
# MAGIC   l.loan_amount,
# MAGIC   l.loan_status
# MAGIC FROM workspace.sap_fin.customers c
# MAGIC INNER JOIN workspace.sap_fin.loans l 
# MAGIC   ON c.customer_id = l.customer_id
# MAGIC WHERE l.loan_status = 'Defaulted'
# MAGIC ORDER BY l.loan_amount DESC
# MAGIC LIMIT 10;
# MAGIC
# MAGIC -- This shows defaulted loans with customer context
# MAGIC -- The aliases 'c' and 'l' are shortcuts (customers = c, loans = l)
# MAGIC -- Try: Change WHERE to filter 'Active' instead

# COMMAND ----------

# DBTITLE 1,✅ Part 0 Complete
# MAGIC %md
# MAGIC # SQL Basics Complete
# MAGIC
# MAGIC If you understood the 4 examples above, you're ready for advanced techniques.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What You Just Learned
# MAGIC
# MAGIC * **SELECT** - Choose columns
# MAGIC * **WHERE** - Filter rows
# MAGIC * **GROUP BY + aggregates** - Summarize data
# MAGIC * **JOIN** - Combine tables
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What's Next
# MAGIC
# MAGIC The **Core Techniques** section introduces 4 advanced SQL patterns used daily by data analysts:
# MAGIC
# MAGIC 1. **CTEs (WITH clause)** - Break complex queries into readable steps
# MAGIC 2. **ROW_NUMBER()** - Rank data and find top N per group
# MAGIC 3. **SUM() OVER()** - Calculate running totals
# MAGIC 4. **LAG()** - Compare current vs previous period
# MAGIC
# MAGIC These patterns power executive dashboards, business reports, and financial analysis.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Continue to Cell 14 when ready.**

# COMMAND ----------

# DBTITLE 1,Part 2: Teaching Modules
# MAGIC %md
# MAGIC # Core SQL Techniques
# MAGIC
# MAGIC > **Learning structure:** Concept → Working example → Your turn
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What You'll Master (4 Techniques)
# MAGIC
# MAGIC ### Module 1: CTEs (WITH Clause)
# MAGIC Break complex analysis into readable, testable steps. Foundation for all advanced SQL.
# MAGIC
# MAGIC ### Module 2: ROW_NUMBER (Ranking)
# MAGIC Rank data within categories. Essential for "top N per group" questions.
# MAGIC
# MAGIC ### Module 3: SUM OVER (Running Totals)
# MAGIC Calculate cumulative metrics for trend analysis and forecasting.
# MAGIC
# MAGIC ### Module 4: LAG (Time Comparisons)
# MAGIC Compare current vs previous period (month-over-month, year-over-year growth).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Each module:**
# MAGIC 1. Concept explanation
# MAGIC 2. Working example you can run
# MAGIC 3. Exercise for you to practice
# MAGIC 4. Common mistakes to avoid
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Start with Cell 15 (Module 1: CTEs)**

# COMMAND ----------

# DBTITLE 1,Module 1: CTEs (WITH Clause)
# MAGIC %md
# MAGIC ## Module 1: CTEs (WITH Clause)
# MAGIC
# MAGIC **What it does:** Break complex queries into named steps.
# MAGIC
# MAGIC **Syntax:**
# MAGIC ```sql
# MAGIC WITH step1 AS (
# MAGIC   SELECT ... -- First calculation
# MAGIC ),
# MAGIC step2 AS (
# MAGIC   SELECT ... FROM step1 -- Use result from step1
# MAGIC )
# MAGIC SELECT ... FROM step2;
# MAGIC ```
# MAGIC
# MAGIC **Example below:** Find customers with above-average total loan amounts.
# MAGIC
# MAGIC **After running it, try:**
# MAGIC * Change LIMIT 10 to LIMIT 5
# MAGIC * Remove the WHERE filter to see all customers
# MAGIC * Add a third column showing the average itself

# COMMAND ----------

# DBTITLE 1,CTE Example: Above Average Loans
# MAGIC %sql
# MAGIC -- Step 1: Calculate average loan amount per customer
# MAGIC -- Step 2: Filter to customers above overall average
# MAGIC
# MAGIC WITH customer_totals AS (
# MAGIC   SELECT 
# MAGIC     c.customer_id,
# MAGIC     c.full_name,
# MAGIC     SUM(l.loan_amount) AS total_loan_amount
# MAGIC   FROM customers c
# MAGIC   INNER JOIN loans l ON c.customer_id = l.customer_id
# MAGIC   GROUP BY c.customer_id, c.full_name
# MAGIC )
# MAGIC SELECT 
# MAGIC   customer_id,
# MAGIC   full_name,
# MAGIC   total_loan_amount,
# MAGIC   ROUND(total_loan_amount / 10000000, 2) AS total_amount_cr
# MAGIC FROM customer_totals
# MAGIC WHERE total_loan_amount > (SELECT AVG(total_loan_amount) FROM customer_totals)
# MAGIC ORDER BY total_loan_amount DESC
# MAGIC LIMIT 10;
# MAGIC
# MAGIC -- This shows top 10 customers with above-average total loans

# COMMAND ----------

# DBTITLE 1,Common Mistakes: CTEs
# MAGIC %md
# MAGIC ### Common Mistakes with CTEs
# MAGIC
# MAGIC **Mistake 1: Using CTE name before defining it**
# MAGIC ```
# MAGIC WITH step2 AS (SELECT * FROM step1),  -- ERROR
# MAGIC      step1 AS (SELECT * FROM loans)   -- step1 not defined yet
# MAGIC ```
# MAGIC Fix: Define CTEs in order (step1 first, then step2)
# MAGIC
# MAGIC **Mistake 2: Not using the CTE in final query**
# MAGIC ```
# MAGIC WITH customer_totals AS (...)
# MAGIC SELECT * FROM customers;  -- ERROR: Ignoring the CTE
# MAGIC ```
# MAGIC Fix: SELECT FROM customer_totals (reference the CTE)
# MAGIC
# MAGIC **Mistake 3: Comma placement**
# MAGIC ```
# MAGIC WITH step1 AS (...),  -- Comma here
# MAGIC      step2 AS (...)   -- NO comma here (last CTE)
# MAGIC SELECT ...            -- Main query starts
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Your Turn: Add Second CTE
# MAGIC %md
# MAGIC ### Your Turn: Add Second CTE
# MAGIC
# MAGIC Modify the query above:
# MAGIC 1. Keep first CTE (customer_totals)
# MAGIC 2. Add second CTE (defaulted_customers) - filters to customers with ≥ 1 Defaulted loan
# MAGIC 3. Show final result
# MAGIC
# MAGIC Join loans again in CTE 2 to check loan_status.
# MAGIC
# MAGIC **Columns**: customer_id, full_name, total_loan_amount, total_amount_cr

# COMMAND ----------

# DBTITLE 1,EXERCISE: Two CTEs
# MAGIC %sql
# MAGIC -- Write your query here
# MAGIC -- CTE 1: customer_totals (already given above)
# MAGIC -- CTE 2: defaulted_customers (you add this)
# MAGIC -- Final SELECT from defaulted_customers
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Module 1 Exercise Check
# Check Module 1 Exercise
try:
    ex1_result = _sqldf
    
    if ex1_result is None:
        print("⚠️ Run the SQL cell above first")
    elif ex1_result.count() == 0:
        print("⚠️ Query returned no rows - check your filters")
    else:
        expected_cols = ['customer_id', 'full_name', 'total_loan_amount', 'total_amount_cr']
        actual_cols = ex1_result.columns
        
        if actual_cols != expected_cols:
            print("⚠️ Column names don't match")
            print(f"Expected: {expected_cols}")
            print(f"Got: {actual_cols}")
        else:
            row_count = ex1_result.count()
            if row_count < 10 or row_count > 100:
                print("⚠️ Row count unusual - expected 10-100 customers")
            else:
                print(f"✅ Correct. {row_count} customers with defaults + above-average loans.")
except Exception as e:
    print(f"⚠️ Error: {str(e)}")
    print("Hint: Two CTEs - customer_totals and defaulted_customers")

# COMMAND ----------

# DBTITLE 1,Module 2: ROW_NUMBER (Ranking)
# MAGIC %md
# MAGIC ## Module 2: ROW_NUMBER (Ranking)
# MAGIC
# MAGIC **What it does:** Assign unique ranks to rows.
# MAGIC
# MAGIC **Key parts:**
# MAGIC - `OVER (ORDER BY ...)` → What to rank by
# MAGIC - `PARTITION BY ...` → Create separate rankings per group
# MAGIC
# MAGIC **Syntax:**
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   column1,
# MAGIC   ROW_NUMBER() OVER (ORDER BY column2 DESC) AS rank
# MAGIC FROM table;
# MAGIC ```
# MAGIC
# MAGIC **Example below:** Rank customers by total loan amount.
# MAGIC
# MAGIC **After running it, try:**
# MAGIC * Change ORDER BY to ASC instead of DESC
# MAGIC * Remove LIMIT 10 to see all ranks
# MAGIC * What happens if you add PARTITION BY employment_type?

# COMMAND ----------

# DBTITLE 1,Module 2 Visual: How PARTITION BY Works
# MAGIC %md
# MAGIC ### 📊 Visual: How PARTITION BY Works
# MAGIC
# MAGIC Imagine ranking customers by loan amount:
# MAGIC
# MAGIC **WITHOUT PARTITION BY** (all customers together):
# MAGIC ```
# MAGIC Customer        Loan Amount    ROW_NUMBER
# MAGIC ─────────────────────────────────────────
# MAGIC Raj Sharma      ₹25,00,000     1  ← Highest overall
# MAGIC Priya Patel     ₹22,00,000     2
# MAGIC Amit Singh      ₹18,00,000     3
# MAGIC Sneha Kumar     ₹15,00,000     4
# MAGIC ```
# MAGIC
# MAGIC **WITH PARTITION BY employment_type** (separate rankings per type):
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────┐
# MAGIC │ PARTITION: Salaried                      │
# MAGIC ├──────────────────────────────────────────┤
# MAGIC │ Raj Sharma     ₹25,00,000   ROW_NUMBER=1 │ ← Highest in Salaried
# MAGIC │ Priya Patel    ₹22,00,000   ROW_NUMBER=2 │
# MAGIC │ Amit Singh     ₹18,00,000   ROW_NUMBER=3 │
# MAGIC └──────────────────────────────────────────┘
# MAGIC
# MAGIC ┌──────────────────────────────────────────┐
# MAGIC │ PARTITION: Self-Employed                 │
# MAGIC ├──────────────────────────────────────────┤
# MAGIC │ Sneha Kumar    ₹28,00,000   ROW_NUMBER=1 │ ← Highest in Self-Employed
# MAGIC │ Vikram Reddy   ₹20,00,000   ROW_NUMBER=2 │   (ranking RESTARTS!)
# MAGIC │ Ananya Iyer    ₹15,00,000   ROW_NUMBER=3 │
# MAGIC └──────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC **Key Insight**: Each partition gets its own independent ranking. ROW_NUMBER restarts at 1 for each partition.
# MAGIC
# MAGIC **Real use case**: "Show me the top 3 loan customers in EACH state" → `PARTITION BY state_name`

# COMMAND ----------

# DBTITLE 1,ROW_NUMBER Example: Rank Customers
# MAGIC %sql
# MAGIC -- Rank all customers by total loan amount
# MAGIC
# MAGIC SELECT 
# MAGIC   c.customer_id,
# MAGIC   c.full_name,
# MAGIC   SUM(l.loan_amount) AS total_loan_amount,
# MAGIC   ROW_NUMBER() OVER (ORDER BY SUM(l.loan_amount) DESC) AS rank_position
# MAGIC FROM customers c
# MAGIC INNER JOIN loans l ON c.customer_id = l.customer_id
# MAGIC GROUP BY c.customer_id, c.full_name
# MAGIC ORDER BY rank_position
# MAGIC LIMIT 10;
# MAGIC
# MAGIC -- Shows top 10 customers by total loan amount with their rank

# COMMAND ----------

# DBTITLE 1,Common Mistakes: ROW_NUMBER
# MAGIC %md
# MAGIC ### ⚠️ Common Mistakes with ROW_NUMBER
# MAGIC
# MAGIC **Mistake 1: Filtering ranks directly**
# MAGIC ```
# MAGIC SELECT ..., ROW_NUMBER() OVER (...) AS rank
# MAGIC FROM customers
# MAGIC WHERE rank <= 3;  -- ❌ Error: Cannot filter window function in WHERE
# MAGIC ```
# MAGIC ✅ Fix: Use subquery or CTE
# MAGIC ```
# MAGIC WITH ranked AS (
# MAGIC   SELECT ..., ROW_NUMBER() OVER (...) AS rank
# MAGIC   FROM customers
# MAGIC )
# MAGIC SELECT * FROM ranked WHERE rank <= 3;
# MAGIC ```
# MAGIC
# MAGIC **Mistake 2: Forgetting ORDER BY in OVER clause**
# MAGIC ```
# MAGIC ROW_NUMBER() OVER (PARTITION BY state)  -- ❌ No ORDER BY!
# MAGIC ```
# MAGIC ✅ Fix: Always specify ORDER BY
# MAGIC ```
# MAGIC ROW_NUMBER() OVER (PARTITION BY state ORDER BY loan_amount DESC)
# MAGIC ```
# MAGIC
# MAGIC **Mistake 3: Misunderstanding PARTITION BY**
# MAGIC - WITHOUT PARTITION BY → Ranks ALL rows together (1, 2, 3, 4...)
# MAGIC - WITH PARTITION BY state → Separate rankings PER state (1, 2, 3 in State A, then 1, 2, 3 in State B)

# COMMAND ----------

# DBTITLE 1,Your Turn: Add PARTITION BY
# MAGIC %md
# MAGIC ### Your Turn: Rank Within Groups
# MAGIC
# MAGIC Modify the query to rank WITHIN each employment_type:
# MAGIC 1. Add `PARTITION BY employment_type` in OVER clause
# MAGIC 2. Show top 3 from EACH type
# MAGIC 3. Include employment_type column
# MAGIC
# MAGIC Filter WHERE rank_position <= 3 (need subquery or CTE).
# MAGIC
# MAGIC **Columns**: employment_type, customer_id, full_name, total_loan_amount, rank_position

# COMMAND ----------

# DBTITLE 1,EXERCISE: PARTITION BY employment_type
# MAGIC %sql
# MAGIC -- Write your query here
# MAGIC -- Use ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)
# MAGIC -- Filter to top 3 per employment_type
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Module 2 Exercise Check
# Check Module 2 Exercise
try:
    ex2_result = _sqldf
    
    if ex2_result is None:
        print("⚠️ Run the SQL cell above first")
    elif ex2_result.count() == 0:
        print("⚠️ Query returned no rows")
    else:
        expected_cols = ['employment_type', 'customer_id', 'full_name', 'total_loan_amount', 'rank_position']
        actual_cols = ex2_result.columns
        
        if actual_cols != expected_cols:
            print("⚠️ Column names don't match")
            print(f"Expected: {expected_cols}")
            print(f"Got: {actual_cols}")
        else:
            row_count = ex2_result.count()
            if row_count < 8 or row_count > 15:
                print(f"⚠️ Expected ~12 rows (3 per employment type), got {row_count}")
            else:
                print(f"✅ Correct. Top 3 per employment type ({row_count} rows).")
except Exception as e:
    print(f"⚠️ Error: {str(e)}")
    print("Hint: ROW_NUMBER() OVER (PARTITION BY employment_type ORDER BY ...)")

# COMMAND ----------

# DBTITLE 1,Module 3: SUM OVER (Cumulative Totals)
# MAGIC %md
# MAGIC ## Module 3: SUM OVER (Cumulative Totals)
# MAGIC
# MAGIC **What it does:** Calculate running totals.
# MAGIC
# MAGIC **Key part:**
# MAGIC - `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` - Sum from start to current row
# MAGIC
# MAGIC **Syntax:**
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   month,
# MAGIC   monthly_amount,
# MAGIC   SUM(monthly_amount) OVER (
# MAGIC     ORDER BY month
# MAGIC     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC   ) AS cumulative
# MAGIC FROM table;
# MAGIC ```
# MAGIC
# MAGIC **Example below:** Cumulative defaulted amount by month.
# MAGIC
# MAGIC **After running it, try:**
# MAGIC * Which month crossed Rs 1 Crore cumulative?
# MAGIC * Change ORDER BY to disbursement_date for a different view
# MAGIC * Calculate cumulative for 'Active' loans instead

# COMMAND ----------

# DBTITLE 1,Module 3 Visual: How SUM OVER Works
# MAGIC %md
# MAGIC ### 📊 Visual: How Cumulative Totals Work
# MAGIC
# MAGIC Imagine tracking monthly defaulted amounts:
# MAGIC
# MAGIC ```
# MAGIC Month        Monthly Defaults    Cumulative Total    How It's Calculated
# MAGIC ──────────────────────────────────────────────────────────────
# MAGIC Jan 2024     ₹45L                ₹45L              45
# MAGIC Feb 2024     ₹52L                ₹97L              45 + 52
# MAGIC Mar 2024     ₹48L                ₹1.45Cr           97 + 48
# MAGIC Apr 2024     ₹63L                ₹2.08Cr           145 + 63
# MAGIC May 2024     ₹55L                ₹2.63Cr           208 + 55
# MAGIC              │                   │
# MAGIC              │                   └─────── Always increasing
# MAGIC              │                           (or staying same)
# MAGIC              └───────────── This month only
# MAGIC ```
# MAGIC
# MAGIC **Visual Flow** (ORDER BY month):
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────┐
# MAGIC │ ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW   │
# MAGIC │ (Include ALL rows from start to current)           │
# MAGIC └─────────────────────────────────────────────────────┘
# MAGIC
# MAGIC Row 1 (Jan):  [45]                    → Sum = 45
# MAGIC Row 2 (Feb):  [45, 52]                → Sum = 97
# MAGIC Row 3 (Mar):  [45, 52, 48]            → Sum = 145
# MAGIC Row 4 (Apr):  [45, 52, 48, 63]        → Sum = 208
# MAGIC               ↑──────────────────────↑
# MAGIC               Window expands as you    Current row
# MAGIC               move down the table
# MAGIC ```
# MAGIC
# MAGIC **Key Insight**: Each row's cumulative value includes ALL previous rows plus itself. The window "grows" as you move through ordered data.
# MAGIC
# MAGIC **Real use case**: "When did our cumulative losses reach ₹10 Crores?" → Find the first row where cumulative total >= 10

# COMMAND ----------

# DBTITLE 1,SUM OVER Example: Cumulative Defaults
# MAGIC %sql
# MAGIC -- Show monthly defaulted amount and cumulative total
# MAGIC
# MAGIC WITH monthly_defaults AS (
# MAGIC   SELECT 
# MAGIC     DATE_FORMAT(default_date, 'yyyy-MM') AS default_month,
# MAGIC     SUM(loan_amount) AS monthly_defaulted_amount
# MAGIC   FROM loans
# MAGIC   WHERE loan_status = 'Defaulted' AND default_date IS NOT NULL
# MAGIC   GROUP BY DATE_FORMAT(default_date, 'yyyy-MM')
# MAGIC )
# MAGIC SELECT 
# MAGIC   default_month,
# MAGIC   ROUND(monthly_defaulted_amount / 10000000, 2) AS monthly_defaulted_cr,
# MAGIC   ROUND(
# MAGIC     SUM(monthly_defaulted_amount) OVER (
# MAGIC       ORDER BY default_month 
# MAGIC       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC     ) / 10000000, 2
# MAGIC   ) AS cumulative_defaulted_cr
# MAGIC FROM monthly_defaults
# MAGIC ORDER BY default_month;
# MAGIC
# MAGIC -- Shows month-by-month defaults and running total

# COMMAND ----------

# DBTITLE 1,Common Mistakes: SUM OVER
# MAGIC %md
# MAGIC ### Common Mistakes with SUM OVER
# MAGIC
# MAGIC **Mistake 1: Missing ROWS BETWEEN clause**
# MAGIC ```
# MAGIC SUM(amount) OVER (ORDER BY month)  -- Works but implicit behavior
# MAGIC ```
# MAGIC Best practice: Be explicit
# MAGIC ```
# MAGIC SUM(amount) OVER (
# MAGIC   ORDER BY month 
# MAGIC   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Mistake 2: Forgetting ORDER BY**
# MAGIC ```
# MAGIC SUM(amount) OVER ()  -- ERROR: Returns same total for every row (no ordering)
# MAGIC ```
# MAGIC Fix: Always specify ORDER BY
# MAGIC ```
# MAGIC SUM(amount) OVER (ORDER BY month ROWS BETWEEN...)
# MAGIC ```
# MAGIC
# MAGIC **Mistake 3: Wrong window frame**
# MAGIC ```
# MAGIC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING  -- Forward-looking sum (incorrect for cumulative)
# MAGIC ```
# MAGIC Fix: Use UNBOUNDED PRECEDING for cumulative totals

# COMMAND ----------

# DBTITLE 1,Your Turn: Cumulative Loan Count
# MAGIC %md
# MAGIC ### Your Turn: Cumulative Loan Count
# MAGIC
# MAGIC Modify to show cumulative loan COUNT (not amount):
# MAGIC 1. Use disbursement_date (all loans, not just defaults)
# MAGIC 2. Group by month
# MAGIC 3. Apply same SUM() OVER() pattern for running total
# MAGIC
# MAGIC Change SUM(loan_amount) to COUNT(*), remove WHERE filter.
# MAGIC
# MAGIC **Columns**: disbursement_month, monthly_loan_count, cumulative_loan_count

# COMMAND ----------

# DBTITLE 1,EXERCISE: Cumulative Loan Count
# MAGIC %sql
# MAGIC -- Write your query here
# MAGIC -- CTE: Group by disbursement month, COUNT(*)
# MAGIC -- Main query: Use SUM(count) OVER() for running total
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Module 3 Exercise Check
# Check Module 3 Exercise
try:
    ex3_result = _sqldf
    
    if ex3_result is None:
        print("⚠️ Run the SQL cell above first")
    elif ex3_result.count() == 0:
        print("⚠️ Query returned no rows")
    else:
        expected_cols = ['disbursement_month', 'monthly_loan_count', 'cumulative_loan_count']
        actual_cols = ex3_result.columns
        
        if actual_cols != expected_cols:
            print("⚠️ Column names don't match")
            print(f"Expected: {expected_cols}")
            print(f"Got: {actual_cols}")
        else:
            rows = ex3_result.collect()
            cumulative_vals = [row['cumulative_loan_count'] for row in rows]
            is_increasing = all(cumulative_vals[i] <= cumulative_vals[i+1] for i in range(len(cumulative_vals)-1))
            
            if not is_increasing:
                print("⚠️ Cumulative count should always increase")
            else:
                print(f"✅ Correct. {len(rows)} months tracked.")
except Exception as e:
    print(f"Error: {e}")
    
    print("Hint: SUM(count) OVER (ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)")

# COMMAND ----------

# DBTITLE 1,Module 4: LAG (Time Comparisons)
# MAGIC %md
# MAGIC ## Module 4: LAG (Period Comparisons)
# MAGIC
# MAGIC **What it does:** Access data from previous row.
# MAGIC
# MAGIC **Syntax:**
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   month,
# MAGIC   current_value,
# MAGIC   LAG(current_value, 1) OVER (ORDER BY month) AS prev_value,
# MAGIC   current_value - LAG(current_value, 1) OVER (ORDER BY month) AS change
# MAGIC FROM table;
# MAGIC ```
# MAGIC
# MAGIC **Example below:** Month-over-month change in disbursements.
# MAGIC
# MAGIC **After running it, try:**
# MAGIC * Calculate percentage change instead of absolute change
# MAGIC * Use LAG(value, 2) to compare with 2 months ago
# MAGIC * Use LEAD() to compare with next month

# COMMAND ----------

# DBTITLE 1,Module 4 Visual: How LAG Works
# MAGIC %md
# MAGIC ### 📊 Visual: How LAG Works (Previous Row Access)
# MAGIC
# MAGIC Imagine comparing current month to previous month:
# MAGIC
# MAGIC ```
# MAGIC Month        Loans    LAG(loans, 1)    Comparison
# MAGIC              (Current) (Previous)
# MAGIC ────────────────────────────────────────────────────
# MAGIC Jan 2024     150      NULL             (no prior month)
# MAGIC Feb 2024     180      150         ←    Feb vs Jan: +30
# MAGIC Mar 2024     165      180         ←    Mar vs Feb: -15
# MAGIC Apr 2024     195      165         ←    Apr vs Mar: +30
# MAGIC              │        │
# MAGIC              │        └─────────── Pulled from ROW ABOVE
# MAGIC              └───────────────── Current row value
# MAGIC ```
# MAGIC
# MAGIC **Visual Flow** (ORDER BY month):
# MAGIC ```
# MAGIC               LAG looks UP to previous row
# MAGIC                        ↑
# MAGIC                        │
# MAGIC Row 1 (Jan):  150      │  ← NULL (no row before this)
# MAGIC                        │
# MAGIC Row 2 (Feb):  180  ────┘  ← LAG gets 150 from Jan
# MAGIC                        │
# MAGIC Row 3 (Mar):  165  ────┘  ← LAG gets 180 from Feb
# MAGIC                        │
# MAGIC Row 4 (Apr):  195  ────┘  ← LAG gets 165 from Mar
# MAGIC ```
# MAGIC
# MAGIC **Calculate Change**:
# MAGIC ```
# MAGIC Current - LAG(current, 1) = Month-over-Month Change
# MAGIC
# MAGIC Feb: 180 - 150 = +30  (↑ Growth)
# MAGIC Mar: 165 - 180 = -15  (↓ Decline)
# MAGIC Apr: 195 - 165 = +30  (↑ Growth)
# MAGIC ```
# MAGIC
# MAGIC **Percentage Change**:
# MAGIC ```
# MAGIC (Current - Previous) / Previous * 100
# MAGIC
# MAGIC Feb: (180 - 150) / 150 * 100 = +20.0%
# MAGIC Mar: (165 - 180) / 180 * 100 = -8.3%
# MAGIC Apr: (195 - 165) / 165 * 100 = +18.2%
# MAGIC ```
# MAGIC
# MAGIC **Key Insight**: LAG lets each row "look back" to compare with previous values. Perfect for trends, changes, and growth rates.
# MAGIC
# MAGIC **Real use case**: "Are our monthly disbursements increasing or decreasing?" → Use LAG to compare each month to the prior month

# COMMAND ----------

# DBTITLE 1,LAG Example: MoM Disbursement Change
# MAGIC %sql
# MAGIC -- Show month-over-month change in loan disbursements
# MAGIC
# MAGIC WITH monthly_stats AS (
# MAGIC   SELECT 
# MAGIC     DATE_FORMAT(disbursement_date, 'yyyy-MM') AS disbursement_month,
# MAGIC     COUNT(*) AS loans_disbursed,
# MAGIC     SUM(loan_amount) AS total_amount
# MAGIC   FROM loans
# MAGIC   GROUP BY DATE_FORMAT(disbursement_date, 'yyyy-MM')
# MAGIC )
# MAGIC SELECT 
# MAGIC   disbursement_month,
# MAGIC   loans_disbursed,
# MAGIC   LAG(loans_disbursed, 1) OVER (ORDER BY disbursement_month) AS prev_month_loans,
# MAGIC   loans_disbursed - LAG(loans_disbursed, 1) OVER (ORDER BY disbursement_month) AS mom_change
# MAGIC FROM monthly_stats
# MAGIC ORDER BY disbursement_month;

# COMMAND ----------

# DBTITLE 1,Common Mistakes: LAG
# MAGIC %md
# MAGIC ### ⚠️ Common Mistakes with LAG
# MAGIC
# MAGIC **Mistake 1: Not handling NULL for first row**
# MAGIC ```
# MAGIC current_value - LAG(current_value, 1) OVER (...)  -- ❌ First row = NULL - value
# MAGIC ```
# MAGIC ✅ Fix: Use COALESCE or filter NULLs
# MAGIC ```
# MAGIC current_value - COALESCE(LAG(current_value, 1) OVER (...), 0)
# MAGIC -- OR --
# MAGIC WHERE LAG(current_value, 1) OVER (...) IS NOT NULL
# MAGIC ```
# MAGIC
# MAGIC **Mistake 2: Wrong ORDER BY**
# MAGIC ```
# MAGIC LAG(amount, 1) OVER (ORDER BY customer_id)  -- ❌ Comparing wrong rows!
# MAGIC ```
# MAGIC For time series: ORDER BY date/month (chronological)
# MAGIC For rankings: ORDER BY the metric you're analyzing
# MAGIC
# MAGIC **Mistake 3: Percentage change formula**
# MAGIC ```
# MAGIC (current - prev) * 100  -- ❌ Missing division!
# MAGIC ```
# MAGIC ✅ Correct formula:
# MAGIC ```
# MAGIC ((current - prev) / prev) * 100
# MAGIC -- Example: (180 - 150) / 150 * 100 = 20%
# MAGIC ```
# MAGIC
# MAGIC **Quick check:** First row's LAG value should always be NULL (no previous row exists).

# COMMAND ----------

# DBTITLE 1,Your Turn: Percentage Change
# MAGIC %md
# MAGIC ### Your Turn: Percentage Change
# MAGIC
# MAGIC Modify to show PERCENTAGE change (not absolute):
# MAGIC 1. Calculate: ((current - previous) / previous) * 100
# MAGIC 2. Round to 2 decimals
# MAGIC 3. Handle NULL for first month
# MAGIC
# MAGIC **Hint**: Divide difference by LAG result, multiply by 100
# MAGIC
# MAGIC **Columns**: disbursement_month, loans_disbursed, prev_month_loans, mom_change_pct

# COMMAND ----------

# DBTITLE 1,EXERCISE: MoM Percentage Change
# MAGIC %sql
# MAGIC -- Write your query here
# MAGIC -- Use LAG to get previous month
# MAGIC -- Calculate (current - prev) / prev * 100
# MAGIC -- Round to 2 decimals
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Module 4 Exercise Check
# Check Module 4 Exercise
try:
    ex4_result = _sqldf
    
    if ex4_result is None:
        print("⚠️ Run the SQL cell above first")
    elif ex4_result.count() == 0:
        print("⚠️ Query returned no rows")
    else:
        expected_cols = ['disbursement_month', 'loans_disbursed', 'prev_month_loans', 'mom_change_pct']
        actual_cols = ex4_result.columns
        
        if actual_cols != expected_cols:
            print("⚠️ Column names don't match")
            print(f"Expected: {expected_cols}")
            print(f"Got: {actual_cols}")
        else:
            first_row = ex4_result.collect()[0]
            if first_row['prev_month_loans'] is not None:
                print("⚠️ First month should have NULL for prev_month_loans")
            else:
                print(f"✅ Correct. {ex4_result.count()} months with MoM % change.")
except Exception as e:
    print(f"⚠️ Error: {str(e)}")
    print("Hint: LAG() for prev month, then (current - prev) / prev * 100")

# COMMAND ----------

# DBTITLE 1,Bringing It All Together
# MAGIC %md
# MAGIC ## Bringing It All Together
# MAGIC
# MAGIC **You've learned 4 techniques. Now see them work together in one query.**
# MAGIC
# MAGIC This example combines:
# MAGIC 1. **CTE** (WITH clause) - Break into steps
# MAGIC 2. **ROW_NUMBER** - Rank months
# MAGIC 3. **SUM OVER** - Running total
# MAGIC 4. **LAG** - Month-over-month comparison
# MAGIC
# MAGIC **Business question:** "Show monthly loan disbursements with rankings, running totals, and MoM growth. Identify which months rank in top 5."
# MAGIC
# MAGIC **This is what Practice Queries test:** Your ability to combine techniques for real business analysis.
# MAGIC
# MAGIC **After running the example below:**
# MAGIC - Notice how each technique solves one piece of the puzzle
# MAGIC - See how CTEs make complex queries readable
# MAGIC - Understand the output before moving to Practice Queries

# COMMAND ----------

# DBTITLE 1,Combined Example: All 4 Techniques
# MAGIC %sql
# MAGIC -- Real analyst query: Combine all 4 techniques
# MAGIC
# MAGIC -- STEP 1 (CTE): Calculate monthly metrics
# MAGIC WITH monthly_stats AS (
# MAGIC   SELECT 
# MAGIC     DATE_FORMAT(disbursement_date, 'yyyy-MM') AS month,
# MAGIC     COUNT(*) AS loans_disbursed,
# MAGIC     SUM(loan_amount) AS total_amount
# MAGIC   FROM loans
# MAGIC   GROUP BY DATE_FORMAT(disbursement_date, 'yyyy-MM')
# MAGIC ),
# MAGIC
# MAGIC -- STEP 2 (CTE + Window Functions): Add rankings and comparisons
# MAGIC ranked_months AS (
# MAGIC   SELECT 
# MAGIC     month,
# MAGIC     loans_disbursed,
# MAGIC     
# MAGIC     -- ROW_NUMBER: Rank months by volume
# MAGIC     ROW_NUMBER() OVER (ORDER BY loans_disbursed DESC) AS volume_rank,
# MAGIC     
# MAGIC     -- SUM OVER: Running total of loans
# MAGIC     SUM(loans_disbursed) OVER (
# MAGIC       ORDER BY month
# MAGIC       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC     ) AS cumulative_loans,
# MAGIC     
# MAGIC     -- LAG: Previous month for comparison
# MAGIC     LAG(loans_disbursed, 1) OVER (ORDER BY month) AS prev_month_loans
# MAGIC   FROM monthly_stats
# MAGIC )
# MAGIC
# MAGIC -- STEP 3: Final output with calculated fields
# MAGIC SELECT 
# MAGIC   month,
# MAGIC   loans_disbursed,
# MAGIC   volume_rank,
# MAGIC   cumulative_loans,
# MAGIC   prev_month_loans,
# MAGIC   CASE 
# MAGIC     WHEN prev_month_loans IS NULL THEN NULL
# MAGIC     ELSE ROUND((loans_disbursed - prev_month_loans) * 100.0 / prev_month_loans, 2)
# MAGIC   END AS mom_growth_pct,
# MAGIC   CASE WHEN volume_rank <= 5 THEN 'Top 5' ELSE '' END AS highlight
# MAGIC FROM ranked_months
# MAGIC ORDER BY month;
# MAGIC
# MAGIC -- This shows the power of combining techniques:
# MAGIC -- - CTE breaks complexity into steps
# MAGIC -- - ROW_NUMBER identifies best months
# MAGIC -- - SUM OVER tracks cumulative progress
# MAGIC -- - LAG calculates growth rates
# MAGIC --
# MAGIC -- Practice queries follow this same pattern.

# COMMAND ----------

# DBTITLE 1,Part 3: Qualifier Test
# MAGIC %md
# MAGIC # Practice Queries (10)
# MAGIC
# MAGIC **Run queries sequentially:** Q1 → Q2 → Q3... → Q10
# MAGIC
# MAGIC Autograder checks the most recent SQL result. Running out of order breaks feedback.
# MAGIC
# MAGIC **Workflow:**
# MAGIC 1. Read query prompt
# MAGIC 2. Write SQL
# MAGIC 3. Run SQL cell
# MAGIC 4. Run autograder cell below
# MAGIC 5. Fix if needed, re-run both
# MAGIC 6. Move to next query
# MAGIC
# MAGIC **Learning approach:** Don't rush. If stuck, review the relevant module (CTE, ROW_NUMBER, SUM OVER, or LAG).

# COMMAND ----------

# DBTITLE 1,Debug Helper
# MAGIC %md
# MAGIC ### Debug Helper
# MAGIC
# MAGIC If autograder fails, run this code in a new cell to inspect your result:
# MAGIC
# MAGIC ```python
# MAGIC print("Your Query Results:")
# MAGIC print("Columns:", _sqldf.columns)
# MAGIC print("Rows:", _sqldf.count())
# MAGIC display(_sqldf.limit(5))
# MAGIC ```
# MAGIC
# MAGIC **Tip**: Autograders use `check_query()` function (Cell 45). Each "Q#: Check" cell is a one-line call.

# COMMAND ----------

# DBTITLE 1,Initialize Test Variables
# Initialize test variables

for i in range(1, 11):
    globals()[f'q{i}_done'] = False

print("[PASS] Ready")

# COMMAND ----------

# my code 
def check_query(q_num, expected_cols, row_check, success_msg="Correct"):
    try:
        result = _sqldf
        
        if result is None:
            print("[PENDING] Run the SQL cell above first")
            return
        
        if result.count() == 0:
            print("[PENDING] Query returned no rows")
            return
        
        actual_cols = result.columns
        if actual_cols != expected_cols:
            print("[PENDING] Column names don't match")
            print(f"Expected: {expected_cols}")
            print(f"Got: {actual_cols}")
            return
        
        row_count = result.count()
        
        if isinstance(row_check, tuple):
            min_rows, max_rows = row_check
            if row_count < min_rows or row_count > max_rows:
                print(f"[PENDING] Row count seems wrong: {row_count}")
                return
        elif row_count != row_check:
            print(f"[PENDING] Expected {row_check} rows, got {row_count}")
            return
        
        # SUCCESS LOGIC (AUTO TRACKING)
        global total_score, queries_completed
        
        if not globals().get(f'q{q_num}_done', False):
            globals()[f'q{q_num}_done'] = True
            
            total_score += 10
            queries_completed += 1
        
        print(f"[PASS] {success_msg}")
        print(f"Score: {total_score}")
        print(f"Queries Completed: {queries_completed}")
        
    except Exception as e:
        print("Error:", str(e))

# COMMAND ----------

# DBTITLE 1,Query 1: Defaulted Loans with Customer Names
# MAGIC %md
# MAGIC ## Query 1: List Defaulted Loans with Customer Names
# MAGIC
# MAGIC **Business Question**: Which customers have defaulted loans? Show loan details with customer names.
# MAGIC
# MAGIC **Requirements**:
# MAGIC - JOIN loans and customers tables
# MAGIC - Filter to loan_status = 'Defaulted'
# MAGIC - Show: loan_id, customer_id, full_name, loan_amount, disbursement_date
# MAGIC - Order by loan_amount DESC
# MAGIC
# MAGIC **Expected columns** (exact spelling): `loan_id`, `customer_id`, `full_name`, `loan_amount`, `disbursement_date`
# MAGIC
# MAGIC Write your SQL below:

# COMMAND ----------

# DBTITLE 1,Q1: Your SQL
# MAGIC %sql
# MAGIC -- Write your query here
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Q1: Autograder
check_query(1, ['loan_id', 'customer_id', 'full_name', 'loan_amount', 'disbursement_date'], (500, 700), "Correct. ~600 defaulted loans.")

# COMMAND ----------

# DBTITLE 1,Query 2: Loan Count by Employment Type
# MAGIC %md
# MAGIC ## Query 2: Count Loans by Employment Type
# MAGIC
# MAGIC **Business Question**: How many loans does each employment type have?
# MAGIC
# MAGIC **Requirements**:
# MAGIC - JOIN loans and customers
# MAGIC - GROUP BY employment_type
# MAGIC - COUNT total loans per type
# MAGIC - Show: employment_type, loan_count
# MAGIC - Order by loan_count DESC
# MAGIC
# MAGIC **Expected columns**: `employment_type`, `loan_count`
# MAGIC
# MAGIC Write your SQL below:

# COMMAND ----------

# DBTITLE 1,Q2: Your SQL
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   c.employment_type,
# MAGIC   COUNT(*) AS loan_count
# MAGIC FROM loans l
# MAGIC INNER JOIN customers c ON l.customer_id = c.customer_id
# MAGIC GROUP BY c.employment_type
# MAGIC ORDER BY loan_count DESC

# COMMAND ----------

# DBTITLE 1,Q2: Autograder
check_query(2, ['employment_type', 'loan_count'], 4, "Correct. 4 employment types.")

# COMMAND ----------

# DBTITLE 1,Query 3: Average Loan Amount by Status
# MAGIC %md
# MAGIC ## Query 3: Average Loan Amount by Loan Status
# MAGIC
# MAGIC **Business Question**: What's the average loan amount for each loan status?
# MAGIC
# MAGIC **Requirements**:
# MAGIC - Use loans table only
# MAGIC - GROUP BY loan_status
# MAGIC - Calculate average loan_amount
# MAGIC - Convert to Lakhs (divide by 100000, round to 2 decimals)
# MAGIC - Show: loan_status, avg_loan_amount_lakhs
# MAGIC - Order by avg_loan_amount_lakhs DESC
# MAGIC
# MAGIC **Expected columns**: `loan_status`, `avg_loan_amount_lakhs`
# MAGIC
# MAGIC Write your SQL below:

# COMMAND ----------

# DBTITLE 1,Q3: Your SQL
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   loan_status,
# MAGIC   ROUND(AVG(loan_amount) / 100000, 2) AS avg_loan_amount_lakhs
# MAGIC FROM loans
# MAGIC GROUP BY loan_status
# MAGIC ORDER BY avg_loan_amount_lakhs DESC

# COMMAND ----------

# DBTITLE 1,Q3: Autograder
check_query(3, ['loan_status', 'avg_loan_amount_lakhs'], 4, "Correct. 4 loan statuses.")

# COMMAND ----------

# DBTITLE 1,Query 4: CTE - Above Average Customers
# MAGIC %md
# MAGIC ## Query 4: Customers with Above-Average Loan Amounts
# MAGIC
# MAGIC **Business Question**: Which customers have total loan amounts above the overall average?
# MAGIC
# MAGIC **Requirements**:
# MAGIC - Use a CTE to calculate total loan amount per customer
# MAGIC - Filter to customers above the average of those totals
# MAGIC - Show: customer_id, customer_name, total_loan_amount
# MAGIC - Convert total to Crores (divide by 10000000, round to 2 decimals) as total_amount_cr
# MAGIC - Order by total_amount_cr DESC
# MAGIC - LIMIT 20
# MAGIC
# MAGIC **Expected columns**: `customer_id`, `customer_name`, `total_loan_amount`, `total_amount_cr`
# MAGIC
# MAGIC **Must use**: WITH clause (CTE)
# MAGIC
# MAGIC Write your SQL below:

# COMMAND ----------

# DBTITLE 1,Q4: Your SQL
# MAGIC %sql
# MAGIC WITH customer_totals AS (
# MAGIC   SELECT 
# MAGIC     c.customer_id,
# MAGIC     c.full_name,
# MAGIC     SUM(l.loan_amount) AS total_loan_amount
# MAGIC   FROM customers c
# MAGIC   INNER JOIN loans l ON c.customer_id = l.customer_id
# MAGIC   GROUP BY c.customer_id, c.full_name
# MAGIC )
# MAGIC SELECT 
# MAGIC   customer_id,
# MAGIC   full_name,
# MAGIC   total_loan_amount,
# MAGIC   ROUND(total_loan_amount / 10000000, 2) AS total_amount_cr
# MAGIC FROM customer_totals
# MAGIC WHERE total_loan_amount > (SELECT AVG(total_loan_amount) FROM customer_totals)
# MAGIC ORDER BY total_amount_cr DESC
# MAGIC LIMIT 20

# COMMAND ----------

# DBTITLE 1,Q4: Autograder
check_query(4, ['customer_id', 'full_name', 'total_loan_amount', 'total_amount_cr'], 20, "Correct. 20 above-average customers.")

# COMMAND ----------

# DBTITLE 1,Query 5: CTE - Monthly Disbursements Above Average
# MAGIC %md
# MAGIC ## Query 5: Months with Above-Average Disbursements
# MAGIC
# MAGIC **Business Question**: Which months had loan disbursements above the monthly average?
# MAGIC
# MAGIC **Requirements**:
# MAGIC - Use CTEs to: (1) calculate monthly disbursement counts, (2) filter to above average
# MAGIC - Group by month (format: yyyy-MM)
# MAGIC - Count loans per month
# MAGIC - Show months where count > average monthly count
# MAGIC - Show: disbursement_month, loan_count
# MAGIC - Order by disbursement_month
# MAGIC
# MAGIC **Expected columns**: `disbursement_month`, `loan_count`
# MAGIC
# MAGIC **Must use**: WITH clause (CTE)
# MAGIC
# MAGIC Write your SQL below:

# COMMAND ----------

# DBTITLE 1,Q5: Your SQL
# MAGIC %sql
# MAGIC WITH monthly_counts AS (
# MAGIC   SELECT 
# MAGIC     DATE_FORMAT(disbursement_date, 'yyyy-MM') AS disbursement_month,
# MAGIC     COUNT(*) AS loan_count
# MAGIC   FROM loans
# MAGIC   GROUP BY DATE_FORMAT(disbursement_date, 'yyyy-MM')
# MAGIC )
# MAGIC SELECT 
# MAGIC   disbursement_month,
# MAGIC   loan_count
# MAGIC FROM monthly_counts
# MAGIC WHERE loan_count > (SELECT AVG(loan_count) FROM monthly_counts)
# MAGIC ORDER BY disbursement_month

# COMMAND ----------

# DBTITLE 1,Q5: Autograder
check_query(5, ['disbursement_month', 'loan_count'], (10, 15), "Correct. Months above average disbursements.")

# COMMAND ----------

# DBTITLE 1,Query 6: ROW_NUMBER - Rank Customers
# MAGIC %md
# MAGIC ## Query 6: Rank Customers by Total Loan Amount
# MAGIC
# MAGIC **Business Question**: Rank all customers by their total loan amount (highest first).
# MAGIC
# MAGIC **Requirements**:
# MAGIC - JOIN loans and customers
# MAGIC - Calculate total loan amount per customer
# MAGIC - Use ROW_NUMBER() to assign ranks (1 = highest)
# MAGIC - Show: customer_id, full_name, total_loan_amount, rank_position
# MAGIC - Order by rank_position
# MAGIC - LIMIT 15
# MAGIC
# MAGIC **Expected columns**: `customer_id`, `full_name`, `total_loan_amount`, `rank_position`
# MAGIC
# MAGIC **Must use**: ROW_NUMBER() OVER ()
# MAGIC
# MAGIC Write your SQL below:

# COMMAND ----------

# DBTITLE 1,Q6: Your SQL
# MAGIC %sql
# MAGIC WITH ranked_customers AS (
# MAGIC   SELECT 
# MAGIC     c.customer_id,
# MAGIC     c.full_name,
# MAGIC     SUM(l.loan_amount) AS total_loan_amount,
# MAGIC     ROW_NUMBER() OVER (ORDER BY SUM(l.loan_amount) DESC) AS rank_position
# MAGIC   FROM customers c
# MAGIC   INNER JOIN loans l ON c.customer_id = l.customer_id
# MAGIC   GROUP BY c.customer_id, c.full_name
# MAGIC )
# MAGIC SELECT 
# MAGIC   customer_id,
# MAGIC   full_name,
# MAGIC   total_loan_amount,
# MAGIC   rank_position
# MAGIC FROM ranked_customers
# MAGIC ORDER BY rank_position
# MAGIC LIMIT 15

# COMMAND ----------

# DBTITLE 1,Q6: Autograder
check_query(6, ['customer_id', 'full_name', 'total_loan_amount', 'rank_position'], 15, "Correct. Top 15 customers ranked.")

# COMMAND ----------

# DBTITLE 1,Query 7: PARTITION BY - Top 2 Per State
# MAGIC %md
# MAGIC ## Query 7: Top 2 Defaulted Customers Per State
# MAGIC
# MAGIC **Business Question**: Who are the top 2 customers with highest defaulted amounts in each state?
# MAGIC
# MAGIC **Requirements**:
# MAGIC - JOIN loans, customers, dim_city, dim_state (3-table join)
# MAGIC - Filter to loan_status = 'Defaulted'
# MAGIC - Calculate total defaulted amount per customer per state
# MAGIC - Use ROW_NUMBER() OVER (PARTITION BY state_name ORDER BY total DESC)
# MAGIC - Filter to state_rank <= 2
# MAGIC - Show: state_name, customer_id, full_name, total_defaulted, state_rank
# MAGIC - Order by state_name, state_rank
# MAGIC
# MAGIC **Expected columns**: `state_name`, `customer_id`, `full_name`, `total_defaulted`, `state_rank`
# MAGIC
# MAGIC **Must use**: PARTITION BY in ROW_NUMBER()
# MAGIC
# MAGIC Write your SQL below:

# COMMAND ----------

# DBTITLE 1,Q7: Your SQL
# MAGIC %sql
# MAGIC WITH state_defaulters AS (
# MAGIC   SELECT 
# MAGIC     s.state_name,
# MAGIC     c.customer_id,
# MAGIC     c.full_name,
# MAGIC     SUM(l.loan_amount) AS total_defaulted,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY s.state_name ORDER BY SUM(l.loan_amount) DESC) AS state_rank
# MAGIC   FROM loans l
# MAGIC   INNER JOIN customers c ON l.customer_id = c.customer_id
# MAGIC   INNER JOIN dim_city ci ON c.city_id = ci.city_id
# MAGIC   INNER JOIN dim_state s ON ci.state_id = s.state_id
# MAGIC   WHERE l.loan_status = 'Defaulted'
# MAGIC   GROUP BY s.state_name, c.customer_id, c.full_name
# MAGIC )
# MAGIC SELECT 
# MAGIC   state_name,
# MAGIC   customer_id,
# MAGIC   full_name,
# MAGIC   total_defaulted,
# MAGIC   state_rank
# MAGIC FROM state_defaulters
# MAGIC WHERE state_rank <= 2
# MAGIC ORDER BY state_name, state_rank

# COMMAND ----------

# DBTITLE 1,Q7: Autograder
check_query(7, ['state_name', 'customer_id', 'full_name', 'total_defaulted', 'state_rank'], (10, 40), "Correct. Top 2 per state.")

# COMMAND ----------

# DBTITLE 1,Query 8: SUM OVER - Cumulative Defaults
# MAGIC %md
# MAGIC ## Query 8: Cumulative Defaulted Amount by Month
# MAGIC
# MAGIC **Business Question**: Show the running total of defaulted amounts month by month.
# MAGIC
# MAGIC **Requirements**:
# MAGIC - Use CTE to calculate monthly defaulted amounts
# MAGIC - Use SUM() OVER() with ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC - Group by default month (format: yyyy-MM)
# MAGIC - Convert to Crores (divide by 10000000, round to 2 decimals)
# MAGIC - Show: default_month, monthly_defaulted_cr, cumulative_defaulted_cr
# MAGIC - Order by default_month
# MAGIC
# MAGIC **Expected columns**: `default_month`, `monthly_defaulted_cr`, `cumulative_defaulted_cr`
# MAGIC
# MAGIC **Must use**: SUM() OVER (ORDER BY ... ROWS BETWEEN ...)
# MAGIC
# MAGIC Write your SQL below:

# COMMAND ----------

# DBTITLE 1,Q8: Your SQL
# MAGIC %sql
# MAGIC WITH monthly_defaults AS (
# MAGIC   SELECT 
# MAGIC     DATE_FORMAT(default_date, 'yyyy-MM') AS default_month,
# MAGIC     SUM(loan_amount) AS monthly_defaulted
# MAGIC   FROM loans
# MAGIC   WHERE loan_status = 'Defaulted' AND default_date IS NOT NULL
# MAGIC   GROUP BY DATE_FORMAT(default_date, 'yyyy-MM')
# MAGIC )
# MAGIC SELECT 
# MAGIC   default_month,
# MAGIC   ROUND(monthly_defaulted / 10000000, 2) AS monthly_defaulted_cr,
# MAGIC   ROUND(SUM(monthly_defaulted) OVER (ORDER BY default_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / 10000000, 2) AS cumulative_defaulted_cr
# MAGIC FROM monthly_defaults
# MAGIC ORDER BY default_month

# COMMAND ----------

# DBTITLE 1,Q8: Autograder
check_query(8, ['default_month', 'monthly_defaulted_cr', 'cumulative_defaulted_cr'], (15, 25), "Correct. Cumulative defaults by month.")

# COMMAND ----------

# DBTITLE 1,Query 9: LAG - Month-over-Month Change
# MAGIC %md
# MAGIC ## Query 9: Month-over-Month Change in Disbursements
# MAGIC
# MAGIC **Business Question**: Calculate the month-over-month change in loan disbursement volume.
# MAGIC
# MAGIC **Requirements**:
# MAGIC - Use CTE to calculate monthly loan counts
# MAGIC - Use LAG() to get previous month's count
# MAGIC - Calculate absolute change (current - previous)
# MAGIC - Show: disbursement_month, loans_disbursed, prev_month_loans, mom_change
# MAGIC - Order by disbursement_month
# MAGIC
# MAGIC **Expected columns**: `disbursement_month`, `loans_disbursed`, `prev_month_loans`, `mom_change`
# MAGIC
# MAGIC **Must use**: LAG() OVER (ORDER BY ...)
# MAGIC
# MAGIC Write your SQL below:

# COMMAND ----------

# DBTITLE 1,Q9: Your SQL
# MAGIC %sql
# MAGIC WITH monthly_counts AS (
# MAGIC   SELECT 
# MAGIC     DATE_FORMAT(disbursement_date, 'yyyy-MM') AS disbursement_month,
# MAGIC     COUNT(*) AS loans_disbursed
# MAGIC   FROM loans
# MAGIC   GROUP BY DATE_FORMAT(disbursement_date, 'yyyy-MM')
# MAGIC )
# MAGIC SELECT 
# MAGIC   disbursement_month,
# MAGIC   loans_disbursed,
# MAGIC   LAG(loans_disbursed) OVER (ORDER BY disbursement_month) AS prev_month_loans,
# MAGIC   loans_disbursed - LAG(loans_disbursed) OVER (ORDER BY disbursement_month) AS mom_change
# MAGIC FROM monthly_counts
# MAGIC ORDER BY disbursement_month

# COMMAND ----------

# DBTITLE 1,Q9: Autograder
check_query(9, ['disbursement_month', 'loans_disbursed', 'prev_month_loans', 'mom_change'], (15, 25), "Correct. Month-over-month analysis.")

# COMMAND ----------

# DBTITLE 1,Query 10: CTE + Window Function Combined
# MAGIC %md
# MAGIC ## Query 10: Monthly Metrics with Rankings
# MAGIC
# MAGIC **Business Question**: Show monthly loan metrics and rank months by default rate.
# MAGIC
# MAGIC **Requirements**:
# MAGIC - Use CTE to calculate monthly metrics:
# MAGIC   - loans_disbursed (count)
# MAGIC   - defaulted_loans (count where status = 'Defaulted')
# MAGIC   - default_rate_pct (defaulted / total * 100, rounded to 2 decimals)
# MAGIC - Use ROW_NUMBER() to rank months by default_rate_pct DESC
# MAGIC - Show: disbursement_month, loans_disbursed, defaulted_loans, default_rate_pct, rank_by_default_rate
# MAGIC - Order by rank_by_default_rate
# MAGIC - LIMIT 10 (worst 10 months)
# MAGIC
# MAGIC **Expected columns**: `disbursement_month`, `loans_disbursed`, `defaulted_loans`, `default_rate_pct`, `rank_by_default_rate`
# MAGIC
# MAGIC **Must use**: CTE + ROW_NUMBER() OVER ()
# MAGIC
# MAGIC Write your SQL below:

# COMMAND ----------

# DBTITLE 1,Q10: Your SQL
# MAGIC %sql
# MAGIC WITH monthly_metrics AS (
# MAGIC   SELECT 
# MAGIC     DATE_FORMAT(disbursement_date, 'yyyy-MM') AS disbursement_month,
# MAGIC     COUNT(*) AS loans_disbursed,
# MAGIC     SUM(CASE WHEN loan_status = 'Defaulted' THEN 1 ELSE 0 END) AS defaulted_loans,
# MAGIC     ROUND(SUM(CASE WHEN loan_status = 'Defaulted' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS default_rate_pct
# MAGIC   FROM loans
# MAGIC   GROUP BY DATE_FORMAT(disbursement_date, 'yyyy-MM')
# MAGIC )
# MAGIC SELECT 
# MAGIC   disbursement_month,
# MAGIC   loans_disbursed,
# MAGIC   defaulted_loans,
# MAGIC   default_rate_pct,
# MAGIC   ROW_NUMBER() OVER (ORDER BY default_rate_pct DESC) AS rank_by_default_rate
# MAGIC FROM monthly_metrics
# MAGIC ORDER BY rank_by_default_rate
# MAGIC LIMIT 10

# COMMAND ----------

# DBTITLE 1,Q10: Autograder
check_query(10, ['disbursement_month', 'loans_disbursed', 'defaulted_loans', 'default_rate_pct', 'rank_by_default_rate'], 10, "Correct. Worst 10 months by default rate.")

# COMMAND ----------

# DBTITLE 1,Part 4: Your Final Score
# MAGIC %md
# MAGIC # Results Summary
# MAGIC
# MAGIC ▶️ **Run the cell below to see your final score and next steps.**

# COMMAND ----------

# DBTITLE 1,Final Score Calculation
# Calculate final score
passed_queries = []

for i in range(1, 11):
    if globals().get(f'q{i}_done', False):
        passed_queries.append(i)

score = len(passed_queries)

print("="*60)
print("YOUR SQL SKILLS PROGRESS")
print("="*60)
print(f"\nCompleted: {score}/10 queries")
print(f"Passed: {passed_queries if passed_queries else 'None'}")

if score >= 8:
    print("\n" + "="*60)
    print("STRONG SQL FOUNDATION")
    print("="*60)
    print("\nYou've demonstrated:")
    print("  * CTEs for multi-step logic")
    print("  * ROW_NUMBER + PARTITION BY for ranking")
    print("  * SUM OVER for running totals")
    print("  * LAG for month-over-month analysis")
    print("\nYou're ready for: Complex business queries, executive dashboards, business reporting")

elif score >= 6:
    print("\nSOLID PROGRESS - REFINE & ADVANCE")
    failed_queries = [i for i in range(1, 11) if i not in passed_queries]
    print(f"\nFocus areas: Queries {failed_queries}")

    if any(q in failed_queries for q in [4, 5]):
        print("\n  * Revisit: Module 1 (CTEs)")
    if any(q in failed_queries for q in [6, 7]):
        print("  * Revisit: Module 2 (ROW_NUMBER + PARTITION BY)")
    if 8 in failed_queries:
        print("  * Revisit: Module 3 (SUM OVER for running totals)")
    if 9 in failed_queries:
        print("  * Revisit: Module 4 (LAG for time comparisons)")

    print("\nNext step: Review modules above, retry failed queries")
    print("Target: 8/10 demonstrates readiness for production work")

elif score >= 4:
    print("\nBUILDING FOUNDATION - PRACTICE MORE")
    print("\nRecommended path:")
    print("  1. Work through Module 1-4 teaching examples slowly")
    print("  2. Complete each module exercise with autograder")
    print("  3. Understand WHY each query works (not just memorize)")
    print("  4. Retry test queries after completing exercises")
    print("\nFocus: Understanding window functions and CTEs - core analyst skills")

else:
    print("\nSTART WITH FUNDAMENTALS")

    print("\nThis notebook assumes you can:")
    print("  * Join tables (INNER JOIN)")
    print("  * Filter rows (WHERE)")
    print("  * Aggregate data (GROUP BY, COUNT, SUM, AVG)")

    print("\nFree resources to build these skills:")
    print("  * Mode Analytics SQL Tutorial (interactive)")
    print("  * Khan Academy Intro to SQL")
    print("  * SQLZoo (practice exercises)")

    print("\nLearning path:")
    print("  1. Master basics (joins, aggregations, filtering)")
    print("  2. Return here and complete Module 1-4 exercises")
    print("  3. Practice the 10 queries")
    print("  4. Build to 8/10 for production work")

    print("\nGrowth mindset: Every expert was once a beginner. Start with fundamentals.")

print("\n" + "="*60)

# COMMAND ----------

# DBTITLE 1,What You Built
# MAGIC %md
# MAGIC # What You Built
# MAGIC
# MAGIC You now have 4 powerful SQL patterns in your toolkit. Here's what you can do:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Pattern 1: CTEs (Multi-Step Analysis)
# MAGIC
# MAGIC **What you learned:**
# MAGIC ```sql
# MAGIC WITH step1 AS (...),
# MAGIC      step2 AS (...)
# MAGIC SELECT ... FROM step2;
# MAGIC ```
# MAGIC
# MAGIC **Real queries you can write:**
# MAGIC - "Calculate total value per customer, then find the top 10%"
# MAGIC - "Identify key segments, then drill into individual customers"
# MAGIC - "Build monthly aggregates, then compare to rolling averages"
# MAGIC
# MAGIC **Why it matters:** CTEs break 50-line monsters into readable 3-step workflows. Code reviews love them.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Pattern 2: ROW_NUMBER + PARTITION BY (Top N per Group)
# MAGIC
# MAGIC **What you learned:**
# MAGIC ```sql
# MAGIC ROW_NUMBER() OVER (
# MAGIC   PARTITION BY category 
# MAGIC   ORDER BY metric DESC
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Real queries you can write:**
# MAGIC - "Top 3 customers per state for targeted outreach"
# MAGIC - "Best and worst performing months per product line"
# MAGIC - "Highest value transactions per customer segment"
# MAGIC
# MAGIC **Why it matters:** Answers "show me the top X in EACH group" without writing 10 separate queries.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Pattern 3: SUM OVER (Running Totals)
# MAGIC
# MAGIC **What you learned:**
# MAGIC ```sql
# MAGIC SUM(amount) OVER (
# MAGIC   ORDER BY date
# MAGIC   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Real queries you can write:**
# MAGIC - "When did cumulative total hit the ₹10 Cr threshold?"
# MAGIC - "Track year-to-date revenue against targets"
# MAGIC - "Cumulative customer acquisition over time"
# MAGIC
# MAGIC **Why it matters:** No self-joins needed. Trend analysis in one query.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Pattern 4: LAG (Period Comparisons)
# MAGIC
# MAGIC **What you learned:**
# MAGIC ```sql
# MAGIC LAG(value, 1) OVER (ORDER BY month)
# MAGIC ```
# MAGIC
# MAGIC **Real queries you can write:**
# MAGIC - "Month-over-month growth (absolute and %)"
# MAGIC - "Detect declining metrics (when current < previous)"
# MAGIC - "Compare this quarter to last quarter"
# MAGIC
# MAGIC **Why it matters:** Every business review asks "how does this compare to last period?" You can answer in SQL.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Combined Power
# MAGIC
# MAGIC Query 10 showed you combining all 4:
# MAGIC - CTE for monthly aggregates
# MAGIC - Window functions for calculations
# MAGIC - ROW_NUMBER for ranking
# MAGIC - Complex business logic in readable steps
# MAGIC
# MAGIC This is **production-level SQL**. You're writing queries that analysts with 2+ years experience write daily.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## How This Translates to Work
# MAGIC
# MAGIC **Data Analyst roles:** You can now handle 80% of daily queries
# MAGIC **Business Intelligence:** You understand dashboards that use these patterns
# MAGIC **Business Analysis:** Reporting, metrics tracking, trend analysis covered
# MAGIC
# MAGIC **Interview ready:** If asked "write a query to find top N per category" or "calculate running totals", you can.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What's Different Now
# MAGIC
# MAGIC **Before this notebook:**
# MAGIC - Window functions felt mysterious
# MAGIC - Complex queries required help
# MAGIC - "Top N per group" = confusion
# MAGIC
# MAGIC **After this notebook:**
# MAGIC - You recognize window function patterns instantly
# MAGIC - You can structure multi-step queries independently
# MAGIC - You debug queries using column names + row counts
# MAGIC
# MAGIC **The gap closed:** From "I need help" to "I can figure this out" for advanced SQL.

# COMMAND ----------

# DBTITLE 1,Where to Go Next
# MAGIC %md
# MAGIC # Where to Go Next
# MAGIC
# MAGIC ## Practice More SQL (Free Resources)
# MAGIC
# MAGIC ### Interactive Platforms
# MAGIC 1. **LeetCode SQL** (leetcode.com/problemset/database)
# MAGIC    - 200+ SQL problems, easy → hard
# MAGIC    - Focus on Medium problems (match this notebook's difficulty)
# MAGIC    - Great for interview prep
# MAGIC
# MAGIC 2. **HackerRank SQL** (hackerrank.com/domains/sql)
# MAGIC    - Structured learning path
# MAGIC    - Certificates for completed tracks
# MAGIC    - Window functions section covers ROW_NUMBER, LAG, LEAD
# MAGIC
# MAGIC 3. **DataLemur SQL** (datalemur.com)
# MAGIC    - Real interview questions from Meta, Google, Amazon
# MAGIC    - Focuses on business analytics (like this notebook)
# MAGIC    - Free tier covers most problems
# MAGIC
# MAGIC ### Dataset Exploration
# MAGIC 4. **Kaggle Datasets** (kaggle.com/datasets)
# MAGIC    - Download CSV → Import to Databricks → Practice
# MAGIC    - Recommended: E-commerce, sales, operations datasets
# MAGIC    - Try replicating this notebook's patterns on new data
# MAGIC
# MAGIC 5. **Google BigQuery Public Datasets**
# MAGIC    - Free tier includes real-world datasets
# MAGIC    - Practice same queries at scale
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Domain-Specific Applications
# MAGIC
# MAGIC Apply these techniques to your industry:
# MAGIC
# MAGIC **E-commerce & Retail:**
# MAGIC - Top products per category (ROW_NUMBER + PARTITION BY)
# MAGIC - Sales trends (LAG for month-over-month)
# MAGIC - Inventory cumulative tracking (SUM OVER)
# MAGIC
# MAGIC **SaaS & Subscription:**
# MAGIC - Churn analysis (LAG for subscription changes)
# MAGIC - MRR/ARR tracking (cumulative revenue)
# MAGIC - Feature adoption by segment (ROW_NUMBER)
# MAGIC
# MAGIC **Healthcare & Operations:**
# MAGIC - Patient volume trends (LAG)
# MAGIC - Cumulative case tracking (SUM OVER)
# MAGIC - Top performers per department (PARTITION BY)
# MAGIC
# MAGIC **Marketing & Analytics:**
# MAGIC - Campaign performance by channel (PARTITION BY)
# MAGIC - Cumulative conversions (SUM OVER)
# MAGIC - Period-over-period engagement (LAG)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Advanced SQL Topics (After Mastering This)
# MAGIC
# MAGIC Once comfortable with CTEs + window functions:
# MAGIC
# MAGIC 1. **More Window Functions**
# MAGIC    - LEAD (opposite of LAG - look forward)
# MAGIC    - RANK / DENSE_RANK (handle ties)
# MAGIC    - NTILE (divide into buckets)
# MAGIC    - FIRST_VALUE / LAST_VALUE
# MAGIC
# MAGIC 2. **Advanced Patterns**
# MAGIC    - Recursive CTEs (hierarchical data)
# MAGIC    - Window frames (RANGE vs ROWS)
# MAGIC    - Multiple PARTITION BY levels
# MAGIC
# MAGIC 3. **Performance Tuning**
# MAGIC    - Index strategies for window functions
# MAGIC    - Partitioning large tables
# MAGIC    - Query optimization
# MAGIC
# MAGIC 4. **Databricks-Specific**
# MAGIC    - Delta Lake time travel
# MAGIC    - Spark SQL optimizations
# MAGIC    - Streaming analytics
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Your Skill Validation
# MAGIC
# MAGIC **Can you answer these with SQL now?**
# MAGIC - "Show me top 5 customers per state" → ROW_NUMBER + PARTITION BY
# MAGIC - "What's our year-to-date revenue?" → SUM OVER
# MAGIC - "How much did sales change last month?" → LAG
# MAGIC - "Find customers above average spending" → CTE
# MAGIC
# MAGIC If you answered yes to all 4, you've leveled up.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Community & Help
# MAGIC
# MAGIC **Where to get help:**
# MAGIC - **Stack Overflow** (tag: [sql] [window-functions])
# MAGIC - **Databricks Community** (community.databricks.com)
# MAGIC - **SQL subreddit** (r/SQL - 500K members)
# MAGIC
# MAGIC **Pro tip:** When asking for help, share:
# MAGIC 1. Sample data (3-5 rows)
# MAGIC 2. Expected output
# MAGIC 3. What you've tried
# MAGIC
# MAGIC You'll get better answers faster.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Final Thought
# MAGIC
# MAGIC You just completed a notebook that covers techniques most SQL courses skip or rush through. You practiced on **real business data** with **instant feedback** and **business context**.
# MAGIC
# MAGIC These 4 patterns (CTEs, ROW_NUMBER, SUM OVER, LAG) appear in:
# MAGIC - 90% of data analyst job descriptions
# MAGIC - Every BI dashboard worth building
# MAGIC - Most SQL technical interviews
# MAGIC
# MAGIC **You're not a beginner anymore.** Go build something.

# COMMAND ----------

# DBTITLE 1,Quick Reference Card
# MAGIC %md
# MAGIC # Quick Reference Card
# MAGIC
# MAGIC **Screenshot this for your SQL toolkit** → Use anytime you need these patterns.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Pattern 1: CTEs (Multi-Step Queries)
# MAGIC
# MAGIC **When to use:** Break complex logic into readable steps
# MAGIC
# MAGIC ```sql
# MAGIC WITH step1 AS (
# MAGIC   SELECT customer_id, SUM(loan_amount) AS total
# MAGIC   FROM loans
# MAGIC   GROUP BY customer_id
# MAGIC ),
# MAGIC step2 AS (
# MAGIC   SELECT * FROM step1 WHERE total > 100000
# MAGIC )
# MAGIC SELECT * FROM step2;
# MAGIC ```
# MAGIC
# MAGIC **Key rules:**
# MAGIC - Define in order (step1 before step2)
# MAGIC - Reference CTE in final SELECT
# MAGIC - Comma BETWEEN CTEs, NOT after last one
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Pattern 2: ROW_NUMBER (Ranking)
# MAGIC
# MAGIC **When to use:** Top N overall OR top N per group
# MAGIC
# MAGIC **All rows together:**
# MAGIC ```sql
# MAGIC ROW_NUMBER() OVER (ORDER BY amount DESC)
# MAGIC ```
# MAGIC
# MAGIC **Separate ranks per group:**
# MAGIC ```sql
# MAGIC ROW_NUMBER() OVER (
# MAGIC   PARTITION BY state 
# MAGIC   ORDER BY amount DESC
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Filter ranks:** Use CTE or subquery
# MAGIC ```sql
# MAGIC WITH ranked AS (
# MAGIC   SELECT *, ROW_NUMBER() OVER (...) AS rn
# MAGIC   FROM table
# MAGIC )
# MAGIC SELECT * FROM ranked WHERE rn <= 3;
# MAGIC ```
# MAGIC
# MAGIC **Key rules:**
# MAGIC - Always include ORDER BY in OVER()
# MAGIC - Can't filter window functions in WHERE directly
# MAGIC - PARTITION BY = separate rankings per group
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Pattern 3: SUM OVER (Running Totals)
# MAGIC
# MAGIC **When to use:** Cumulative totals, year-to-date metrics
# MAGIC
# MAGIC ```sql
# MAGIC SUM(amount) OVER (
# MAGIC   ORDER BY date
# MAGIC   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Key rules:**
# MAGIC - ORDER BY required (defines accumulation order)
# MAGIC - ROWS BETWEEN makes it explicit
# MAGIC - Cumulative values should NEVER decrease
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Pattern 4: LAG (Period Comparisons)
# MAGIC
# MAGIC **When to use:** Month-over-month, compare to previous row
# MAGIC
# MAGIC **Access previous row:**
# MAGIC ```sql
# MAGIC LAG(value, 1) OVER (ORDER BY date)
# MAGIC ```
# MAGIC
# MAGIC **Calculate change:**
# MAGIC ```sql
# MAGIC value - LAG(value, 1) OVER (ORDER BY date)
# MAGIC ```
# MAGIC
# MAGIC **Percentage change:**
# MAGIC ```sql
# MAGIC ((current - prev) / prev) * 100
# MAGIC -- Remember: Division before multiplication!
# MAGIC ```
# MAGIC
# MAGIC **Key rules:**
# MAGIC - First row's LAG = NULL (no previous row)
# MAGIC - Use COALESCE to handle NULLs
# MAGIC - ORDER BY determines which row is "previous"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Combining Patterns
# MAGIC
# MAGIC **Real-world query structure:**
# MAGIC ```sql
# MAGIC WITH monthly_metrics AS (
# MAGIC   -- CTE for aggregation
# MAGIC   SELECT month, COUNT(*) AS count, SUM(amount) AS total
# MAGIC   FROM loans
# MAGIC   GROUP BY month
# MAGIC ),
# MAGIC with_growth AS (
# MAGIC   -- Add LAG for MoM comparison
# MAGIC   SELECT *,
# MAGIC     LAG(count, 1) OVER (ORDER BY month) AS prev_count
# MAGIC   FROM monthly_metrics
# MAGIC ),
# MAGIC with_cumulative AS (
# MAGIC   -- Add running total
# MAGIC   SELECT *,
# MAGIC     SUM(total) OVER (
# MAGIC       ORDER BY month
# MAGIC       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC     ) AS cumulative_total
# MAGIC   FROM with_growth
# MAGIC ),
# MAGIC ranked AS (
# MAGIC   -- Rank by growth rate
# MAGIC   SELECT *,
# MAGIC     ROW_NUMBER() OVER (ORDER BY (count - prev_count) DESC) AS rank
# MAGIC   FROM with_cumulative
# MAGIC )
# MAGIC SELECT * FROM ranked WHERE rank <= 10;
# MAGIC ```
# MAGIC
# MAGIC **You just learned this. Seriously.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Debugging Tips
# MAGIC
# MAGIC **Column name errors:**
# MAGIC ```python
# MAGIC print(_sqldf.columns)  # See what you actually returned
# MAGIC ```
# MAGIC
# MAGIC **Row count checks:**
# MAGIC ```python
# MAGIC _sqldf.count()  # How many rows?
# MAGIC ```
# MAGIC
# MAGIC **Inspect data:**
# MAGIC ```python
# MAGIC display(_sqldf.limit(5))  # See first 5 rows
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Copy-Paste Templates
# MAGIC
# MAGIC **Top N per group:**
# MAGIC ```sql
# MAGIC WITH ranked AS (
# MAGIC   SELECT *, 
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY [group_column]
# MAGIC       ORDER BY [metric] DESC
# MAGIC     ) AS rn
# MAGIC   FROM [table]
# MAGIC )
# MAGIC SELECT * FROM ranked WHERE rn <= [N];
# MAGIC ```
# MAGIC
# MAGIC **Month-over-month %:**
# MAGIC ```sql
# MAGIC WITH monthly AS (
# MAGIC   SELECT 
# MAGIC     DATE_FORMAT([date_col], 'yyyy-MM') AS month,
# MAGIC     [aggregation] AS value
# MAGIC   FROM [table]
# MAGIC   GROUP BY DATE_FORMAT([date_col], 'yyyy-MM')
# MAGIC )
# MAGIC SELECT 
# MAGIC   month,
# MAGIC   value,
# MAGIC   LAG(value, 1) OVER (ORDER BY month) AS prev_value,
# MAGIC   ROUND(
# MAGIC     ((value - LAG(value, 1) OVER (ORDER BY month)) / 
# MAGIC      LAG(value, 1) OVER (ORDER BY month)) * 100, 2
# MAGIC   ) AS pct_change
# MAGIC FROM monthly;
# MAGIC ```
# MAGIC
# MAGIC **Cumulative total:**
# MAGIC ```sql
# MAGIC WITH time_series AS (
# MAGIC   SELECT [date_col], SUM([amount]) AS period_total
# MAGIC   FROM [table]
# MAGIC   GROUP BY [date_col]
# MAGIC )
# MAGIC SELECT 
# MAGIC   [date_col],
# MAGIC   period_total,
# MAGIC   SUM(period_total) OVER (
# MAGIC     ORDER BY [date_col]
# MAGIC     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC   ) AS cumulative_total
# MAGIC FROM time_series;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Save this reference.** You'll use these patterns every week as a data analyst.

# COMMAND ----------

# DBTITLE 1,You Made It 🎉
# MAGIC %md
# MAGIC # You Made It 🎉
# MAGIC
# MAGIC You finished. You practiced CTEs, ROW_NUMBER, SUM OVER, and LAG. These 4 patterns appear in 90% of analyst job descriptions and technical interviews.
# MAGIC
# MAGIC **You have the foundation now.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Here's the Gap
# MAGIC
# MAGIC This notebook taught you **one-time project building**: 10 queries, isolated problems, clear requirements, done.
# MAGIC
# MAGIC Real analyst work is **working on the same problem over multiple days** with evolving requirements. Monday's question leads to Tuesday's deeper investigation. You adjust your analysis as the business learns more.
# MAGIC
# MAGIC **That's what EduFin teaches you to investigate.**
# MAGIC
# MAGIC Mon-Fri loan default crisis workflow. 500K+ records (100x larger). Same business problem, incremental requirements each day. Business decisions under pressure. Messy, incomplete data.
# MAGIC
# MAGIC You built the foundation here. EduFin teaches you to use it like an analyst.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Get Support
# MAGIC
# MAGIC **Questions or progress updates?**  
# MAGIC Reach out to community community:  
# MAGIC https://veereshgendle.graphy.com/t/u/community-chat?entityExternalId=68da52346013d27142f309b6&channel=question-answers
# MAGIC
# MAGIC **Ready for EduFin?**  
# MAGIC Reach out on team chat:  
# MAGIC https://skillaipath.com/edufin-sql
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC Most people download free resources. Few finish them. You just built something real.
# MAGIC
# MAGIC **Now go build more.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC *SQL for Financial Analysis · Built by SkillAI Path*