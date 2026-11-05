# Databricks notebook source
# DBTITLE 1,Interview Prep - Technical/Coding Questions
# MAGIC %md
# MAGIC # Interview Questions - Technical & Coding
# MAGIC
# MAGIC ## Purpose
# MAGIC 20 technical questions requiring:
# MAGIC * SQL queries (window functions, CTEs, joins)
# MAGIC * PySpark code (transformations, optimizations)
# MAGIC * Architecture design
# MAGIC * Debugging scenarios
# MAGIC
# MAGIC ## Format
# MAGIC * Question in markdown
# MAGIC * Code solution in next cell (Python/SQL)
# MAGIC * Run cells to see output
# MAGIC * Practice explaining your approach
# MAGIC
# MAGIC ## Skills Tested
# MAGIC * SQL complexity (window functions, CTEs, aggregations)
# MAGIC * PySpark DataFrame API
# MAGIC * Delta Lake operations
# MAGIC * SCD Type 2 implementation
# MAGIC * Performance optimization
# MAGIC * Troubleshooting

# COMMAND ----------

# DBTITLE 1,Q1: Top N Per Group
# MAGIC %md
# MAGIC ## Question 1: Top N Products Per Category
# MAGIC
# MAGIC **Interviewer asks:** "Write a SQL query to find the top 3 best-selling products in each product category."
# MAGIC
# MAGIC **Tables:**
# MAGIC * `fact_sales` (product_key, quantity_sold, sales_amount)
# MAGIC * `dim_product` (product_key, product_name, category_id)
# MAGIC * `dim_product_category` (category_id, category_name)
# MAGIC
# MAGIC **Expected output:**
# MAGIC | category_name | product_name | total_quantity | rank |
# MAGIC |---------------|--------------|----------------|------|
# MAGIC | Electronics   | Laptop Pro   | 5000           | 1    |
# MAGIC | Electronics   | Phone X      | 4500           | 2    |
# MAGIC | Electronics   | Tablet Y     | 4000           | 3    |
# MAGIC | Groceries     | Rice 10kg    | 8000           | 1    |
# MAGIC | ...           | ...          | ...            | ...  |

# COMMAND ----------

# DBTITLE 1,Solution Q1
# MAGIC %sql
# MAGIC -- Solution: Window Function with ROW_NUMBER()
# MAGIC
# MAGIC WITH product_sales AS (
# MAGIC   SELECT
# MAGIC     p.product_key,
# MAGIC     p.product_name,
# MAGIC     pc.category_name,
# MAGIC     SUM(fs.quantity_sold) AS total_quantity
# MAGIC   FROM fact_sales fs
# MAGIC   INNER JOIN dim_product p ON fs.product_key = p.product_key
# MAGIC   INNER JOIN dim_product_category pc ON p.category_id = pc.category_id
# MAGIC   GROUP BY p.product_key, p.product_name, pc.category_name
# MAGIC ),
# MAGIC ranked_products AS (
# MAGIC   SELECT
# MAGIC     category_name,
# MAGIC     product_name,
# MAGIC     total_quantity,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY category_name
# MAGIC       ORDER BY total_quantity DESC
# MAGIC     ) AS rank
# MAGIC   FROM product_sales
# MAGIC )
# MAGIC SELECT
# MAGIC   category_name,
# MAGIC   product_name,
# MAGIC   total_quantity,
# MAGIC   rank
# MAGIC FROM ranked_products
# MAGIC WHERE rank <= 3
# MAGIC ORDER BY category_name, rank;
# MAGIC
# MAGIC -- Interview talking points:
# MAGIC -- 1. "ROW_NUMBER() for deterministic ranking (no ties)"
# MAGIC -- 2. "Could use RANK() if ties should share same rank"
# MAGIC -- 3. "CTE for readability over subquery"
# MAGIC -- 4. "PARTITION BY category resets counter per group"

# COMMAND ----------

# DBTITLE 1,Q2: Running Total
# MAGIC %md
# MAGIC ## Question 2: Running Total Sales by Month
# MAGIC
# MAGIC **Interviewer asks:** "Calculate cumulative revenue by month. Show month, monthly revenue, and year-to-date total."
# MAGIC
# MAGIC **Expected output:**
# MAGIC | year | month | monthly_revenue | ytd_revenue |
# MAGIC |------|-------|-----------------|-------------|
# MAGIC | 2024 | Jan   | 1000000         | 1000000     |
# MAGIC | 2024 | Feb   | 1200000         | 2200000     |
# MAGIC | 2024 | Mar   | 1100000         | 3300000     |
# MAGIC | ...  | ...   | ...             | ...         |

# COMMAND ----------

# DBTITLE 1,Solution Q2
# MAGIC %sql
# MAGIC -- Solution: Window Function with SUM() OVER()
# MAGIC
# MAGIC WITH monthly_sales AS (
# MAGIC   SELECT
# MAGIC     YEAR(d.full_date) AS year,
# MAGIC     MONTH(d.full_date) AS month,
# MAGIC     DATE_TRUNC('month', d.full_date) AS month_start,
# MAGIC     SUM(fs.sales_amount) AS monthly_revenue
# MAGIC   FROM fact_sales fs
# MAGIC   INNER JOIN dim_date d ON fs.date_key = d.date_key
# MAGIC   GROUP BY YEAR(d.full_date), MONTH(d.full_date), DATE_TRUNC('month', d.full_date)
# MAGIC )
# MAGIC SELECT
# MAGIC   year,
# MAGIC   DATE_FORMAT(month_start, 'MMM') AS month,
# MAGIC   ROUND(monthly_revenue, 2) AS monthly_revenue,
# MAGIC   ROUND(SUM(monthly_revenue) OVER (
# MAGIC     PARTITION BY year
# MAGIC     ORDER BY month_start
# MAGIC     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC   ), 2) AS ytd_revenue
# MAGIC FROM monthly_sales
# MAGIC ORDER BY year, month_start;
# MAGIC
# MAGIC -- Interview talking points:
# MAGIC -- 1. "PARTITION BY year to reset cumulative total each year"
# MAGIC -- 2. "ROWS BETWEEN for explicit frame (could also use RANGE)"
# MAGIC -- 3. "DATE_TRUNC for consistent monthly grouping"
# MAGIC -- 4. "Useful for YTD metrics in dashboards"

# COMMAND ----------

# DBTITLE 1,Q3-6: More SQL Questions
# MAGIC %md
# MAGIC ## Question 3: Cohort Analysis
# MAGIC
# MAGIC **Q:** "Find customer retention rate: Of customers who made their first purchase in Jan 2024, how many purchased again in Feb?"
# MAGIC
# MAGIC **Hint:** Self-join fact_sales, use MIN(date) for first purchase
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Question 4: Gap Detection
# MAGIC
# MAGIC **Q:** "Identify inventory records where stock_quantity dropped by >50% compared to previous day for same product."
# MAGIC
# MAGIC **Hint:** LAG() window function
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Question 5: SCD Type 2 Query
# MAGIC
# MAGIC **Q:** "Given dim_customer with SCD Type 2, write a query to show ALL historical addresses for customer 'C001'."
# MAGIC
# MAGIC **Hint:** Filter by natural_key (customer_id), order by effective_date
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Question 6: Slowly Changing Aggregates
# MAGIC
# MAGIC **Q:** "Calculate total sales by customer's city AS OF each transaction date (not current city)."
# MAGIC
# MAGIC **Hint:** Join fact_sales with dim_customer using BETWEEN effective_date AND end_date

# COMMAND ----------

# DBTITLE 1,Q7: Recursive CTE - Org Hierarchy
# MAGIC %md
# MAGIC ## Question 7: Recursive CTE for Organizational Hierarchy (ADVANCED)
# MAGIC
# MAGIC **Interviewer asks:** "We have an employees table with manager_id. Write a query to show the full reporting chain for employee 'E001' up to the CEO."
# MAGIC
# MAGIC **Table:**
# MAGIC ```
# MAGIC employees (employee_id, employee_name, manager_id, job_title)
# MAGIC ```
# MAGIC
# MAGIC **Example data:**
# MAGIC | employee_id | employee_name | manager_id | job_title |
# MAGIC |-------------|---------------|------------|-------------|
# MAGIC | E001        | Alice         | E002       | Analyst     |
# MAGIC | E002        | Bob           | E003       | Manager     |
# MAGIC | E003        | Carol         | NULL       | VP          |
# MAGIC
# MAGIC **Expected output:**
# MAGIC | employee_id | employee_name | job_title | level | reporting_chain |
# MAGIC |-------------|---------------|-----------|-------|------------------|
# MAGIC | E001        | Alice         | Analyst   | 1     | Alice           |
# MAGIC | E002        | Bob           | Manager   | 2     | Alice → Bob     |
# MAGIC | E003        | Carol         | VP        | 3     | Alice → Bob → Carol |
# MAGIC
# MAGIC **Why this is advanced:** Recursive CTEs test understanding of hierarchical data traversal (common in org charts, product categories, bill-of-materials).

# COMMAND ----------

# DBTITLE 1,Solution Q7
# MAGIC %sql
# MAGIC -- Solution: Recursive CTE
# MAGIC
# MAGIC WITH RECURSIVE reporting_chain AS (
# MAGIC   -- Base case: Start with the target employee
# MAGIC   SELECT
# MAGIC     employee_id,
# MAGIC     employee_name,
# MAGIC     manager_id,
# MAGIC     job_title,
# MAGIC     1 AS level,
# MAGIC     employee_name AS reporting_chain
# MAGIC   FROM employees
# MAGIC   WHERE employee_id = 'E001'
# MAGIC
# MAGIC   UNION ALL
# MAGIC
# MAGIC   -- Recursive case: Walk up the management chain
# MAGIC   SELECT
# MAGIC     e.employee_id,
# MAGIC     e.employee_name,
# MAGIC     e.manager_id,
# MAGIC     e.job_title,
# MAGIC     rc.level + 1 AS level,
# MAGIC     CONCAT(rc.reporting_chain, ' → ', e.employee_name) AS reporting_chain
# MAGIC   FROM employees e
# MAGIC   INNER JOIN reporting_chain rc ON e.employee_id = rc.manager_id
# MAGIC )
# MAGIC SELECT
# MAGIC   employee_id,
# MAGIC   employee_name,
# MAGIC   job_title,
# MAGIC   level,
# MAGIC   reporting_chain
# MAGIC FROM reporting_chain
# MAGIC ORDER BY level;
# MAGIC
# MAGIC -- Interview talking points:
# MAGIC -- 1. "Base case selects starting employee (anchor)"
# MAGIC -- 2. "Recursive case joins to manager_id (walks up hierarchy)"
# MAGIC -- 3. "Terminates when manager_id is NULL (CEO has no manager)"
# MAGIC -- 4. "Could reverse to show all direct/indirect reports (walk down)"
# MAGIC -- 5. "Production use: Add cycle detection (max recursion depth)"
# MAGIC -- 6. "Alternative: Use CONNECT BY in Oracle, or graph traversal in Neo4j"
# MAGIC
# MAGIC -- Variations interviewer might ask:
# MAGIC -- • "Show ALL employees under manager 'E003'" → Reverse join direction
# MAGIC -- • "Calculate total team size at each level" → Add COUNT() aggregation
# MAGIC -- • "Find employees with no direct reports" → LEFT JOIN WHERE manager_id IS NULL

# COMMAND ----------

# DBTITLE 1,Q8: Multi-Table Join with Filtering
# MAGIC %md
# MAGIC ## Question 8: Complex Multi-Table Join with Business Logic (ADVANCED)
# MAGIC
# MAGIC **Interviewer asks:** "Find customers who placed orders in BOTH Jan 2024 AND Feb 2024, calculate their total spend in each month, and show only those who spent MORE in Feb than Jan. Include customer segment from customer_360 mart."
# MAGIC
# MAGIC **Tables:**
# MAGIC * `fact_sales` (sales_key, customer_key, date_key, sales_amount)
# MAGIC * `dim_customer` (customer_key, customer_name, email)
# MAGIC * `dim_date` (date_key, full_date, year, month)
# MAGIC * `mart_customer_360` (customer_key, customer_segment, lifetime_value)
# MAGIC
# MAGIC **Expected output:**
# MAGIC | customer_name | jan_spend | feb_spend | spend_increase | customer_segment |
# MAGIC |---------------|-----------|-----------|----------------|------------------|
# MAGIC | John Doe      | 5000      | 7000      | 2000           | VIP Customer     |
# MAGIC | Jane Smith    | 3000      | 4500      | 1500           | Returning        |
# MAGIC
# MAGIC **Why this is advanced:** Tests multiple CTEs, conditional aggregation, HAVING clause, and business logic filtering.

# COMMAND ----------

# DBTITLE 1,Solution Q8
# MAGIC %sql
# MAGIC -- Solution: Multi-CTE approach with filtering
# MAGIC
# MAGIC WITH jan_sales AS (
# MAGIC   SELECT
# MAGIC     fs.customer_key,
# MAGIC     SUM(fs.sales_amount) AS jan_spend
# MAGIC   FROM fact_sales fs
# MAGIC   INNER JOIN dim_date d ON fs.date_key = d.date_key
# MAGIC   WHERE d.year = 2024 AND d.month = 1
# MAGIC   GROUP BY fs.customer_key
# MAGIC ),
# MAGIC feb_sales AS (
# MAGIC   SELECT
# MAGIC     fs.customer_key,
# MAGIC     SUM(fs.sales_amount) AS feb_spend
# MAGIC   FROM fact_sales fs
# MAGIC   INNER JOIN dim_date d ON fs.date_key = d.date_key
# MAGIC   WHERE d.year = 2024 AND d.month = 2
# MAGIC   GROUP BY fs.customer_key
# MAGIC ),
# MAGIC monthly_comparison AS (
# MAGIC   SELECT
# MAGIC     COALESCE(jan.customer_key, feb.customer_key) AS customer_key,
# MAGIC     COALESCE(jan.jan_spend, 0) AS jan_spend,
# MAGIC     COALESCE(feb.feb_spend, 0) AS feb_spend,
# MAGIC     COALESCE(feb.feb_spend, 0) - COALESCE(jan.jan_spend, 0) AS spend_increase
# MAGIC   FROM jan_sales jan
# MAGIC   FULL OUTER JOIN feb_sales feb ON jan.customer_key = feb.customer_key
# MAGIC   WHERE jan.customer_key IS NOT NULL  -- Purchased in Jan
# MAGIC     AND feb.customer_key IS NOT NULL  -- Purchased in Feb
# MAGIC     AND feb.feb_spend > jan.jan_spend -- Spent more in Feb
# MAGIC )
# MAGIC SELECT
# MAGIC   dc.customer_name,
# MAGIC   ROUND(mc.jan_spend, 2) AS jan_spend,
# MAGIC   ROUND(mc.feb_spend, 2) AS feb_spend,
# MAGIC   ROUND(mc.spend_increase, 2) AS spend_increase,
# MAGIC   m360.customer_segment
# MAGIC FROM monthly_comparison mc
# MAGIC INNER JOIN dim_customer dc ON mc.customer_key = dc.customer_key
# MAGIC INNER JOIN mart_customer_360 m360 ON mc.customer_key = m360.customer_key
# MAGIC ORDER BY mc.spend_increase DESC;
# MAGIC
# MAGIC -- Interview talking points:
# MAGIC -- 1. "Separate CTEs for each month (clean, readable)"
# MAGIC -- 2. "FULL OUTER JOIN to catch customers in either month"
# MAGIC -- 3. "COALESCE handles customers with sales in only one month"
# MAGIC -- 4. "Filter in WHERE (both months) not HAVING (avoid confusion)"
# MAGIC -- 5. "Final join to enrich with customer details and segment"
# MAGIC
# MAGIC -- Alternative approach (single CTE with CASE):
# MAGIC /*
# MAGIC WITH monthly_sales AS (
# MAGIC   SELECT
# MAGIC     fs.customer_key,
# MAGIC     SUM(CASE WHEN d.month = 1 THEN fs.sales_amount ELSE 0 END) AS jan_spend,
# MAGIC     SUM(CASE WHEN d.month = 2 THEN fs.sales_amount ELSE 0 END) AS feb_spend
# MAGIC   FROM fact_sales fs
# MAGIC   INNER JOIN dim_date d ON fs.date_key = d.date_key
# MAGIC   WHERE d.year = 2024 AND d.month IN (1, 2)
# MAGIC   GROUP BY fs.customer_key
# MAGIC   HAVING jan_spend > 0 AND feb_spend > 0 AND feb_spend > jan_spend
# MAGIC )
# MAGIC */
# MAGIC
# MAGIC -- Performance notes:
# MAGIC -- • Add indexes on date_key (fact_sales), customer_key (all tables)
# MAGIC -- • If data volume large, filter on date_key range before join
# MAGIC -- • Consider materializing monthly aggregates in gold layer

# COMMAND ----------

# DBTITLE 1,Q9: Window Functions - Percent Rank
# MAGIC %md
# MAGIC ## Question 9: Window Functions with Conditional Aggregation (ADVANCED)
# MAGIC
# MAGIC **Interviewer asks:** "For each product category, calculate:
# MAGIC 1. Each product's revenue
# MAGIC 2. Its percentile rank within the category (PERCENT_RANK)
# MAGIC 3. Whether it's in the top 20% of the category
# MAGIC 4. The revenue gap to the #1 product in that category"
# MAGIC
# MAGIC **Tables:**
# MAGIC * `fact_sales` (product_key, sales_amount)
# MAGIC * `dim_product` (product_key, product_name, category_id)
# MAGIC * `dim_product_category` (category_id, category_name)
# MAGIC
# MAGIC **Expected output:**
# MAGIC | category_name | product_name | revenue | percentile | top_20_pct | gap_to_top |
# MAGIC |---------------|--------------|---------|------------|------------|------------|
# MAGIC | Electronics   | Laptop Pro   | 500000  | 1.00       | Yes        | 0          |
# MAGIC | Electronics   | Phone X      | 450000  | 0.67       | Yes        | 50000      |
# MAGIC | Electronics   | Tablet Y     | 300000  | 0.33       | No         | 200000     |
# MAGIC
# MAGIC **Why this is advanced:** Combines multiple window functions (PERCENT_RANK, FIRST_VALUE, MAX), conditional logic, and business metrics.

# COMMAND ----------

# DBTITLE 1,Solution Q9
# MAGIC %sql
# MAGIC -- Solution: Multiple window functions in single query
# MAGIC
# MAGIC WITH product_revenue AS (
# MAGIC   SELECT
# MAGIC     p.product_key,
# MAGIC     p.product_name,
# MAGIC     pc.category_id,
# MAGIC     pc.category_name,
# MAGIC     SUM(fs.sales_amount) AS revenue
# MAGIC   FROM fact_sales fs
# MAGIC   INNER JOIN dim_product p ON fs.product_key = p.product_key
# MAGIC   INNER JOIN dim_product_category pc ON p.category_id = pc.category_id
# MAGIC   GROUP BY p.product_key, p.product_name, pc.category_id, pc.category_name
# MAGIC ),
# MAGIC product_metrics AS (
# MAGIC   SELECT
# MAGIC     category_name,
# MAGIC     product_name,
# MAGIC     revenue,
# MAGIC     ROUND(PERCENT_RANK() OVER (
# MAGIC       PARTITION BY category_id
# MAGIC       ORDER BY revenue DESC
# MAGIC     ), 2) AS percentile,
# MAGIC     FIRST_VALUE(revenue) OVER (
# MAGIC       PARTITION BY category_id
# MAGIC       ORDER BY revenue DESC
# MAGIC       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
# MAGIC     ) AS category_top_revenue,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY category_id
# MAGIC       ORDER BY revenue DESC
# MAGIC     ) AS revenue_rank
# MAGIC   FROM product_revenue
# MAGIC )
# MAGIC SELECT
# MAGIC   category_name,
# MAGIC   product_name,
# MAGIC   ROUND(revenue, 2) AS revenue,
# MAGIC   percentile,
# MAGIC   CASE
# MAGIC     WHEN percentile >= 0.80 THEN 'Yes'  -- Top 20% (PERCENT_RANK is 0.80-1.00)
# MAGIC     ELSE 'No'
# MAGIC   END AS top_20_pct,
# MAGIC   ROUND(category_top_revenue - revenue, 2) AS gap_to_top,
# MAGIC   revenue_rank
# MAGIC FROM product_metrics
# MAGIC ORDER BY category_name, revenue DESC;
# MAGIC
# MAGIC -- Interview talking points:
# MAGIC -- 1. "PERCENT_RANK() returns 0 (lowest) to 1 (highest)"
# MAGIC -- 2. "Top 20% = PERCENT_RANK >= 0.80, NOT <= 0.20 (common mistake)"
# MAGIC -- 3. "FIRST_VALUE with UNBOUNDED FOLLOWING ensures correct top value"
# MAGIC -- 4. "Could use MAX() OVER instead of FIRST_VALUE (simpler)"
# MAGIC -- 5. "ROW_NUMBER for explicit ranking (1, 2, 3...)"
# MAGIC
# MAGIC -- Business use cases:
# MAGIC -- • Identify underperforming products (bottom 20%)
# MAGIC -- • Calculate performance gaps for improvement targets
# MAGIC -- • Segment products (top/middle/bottom performers)
# MAGIC -- • Dashboard KPI: "X% of products drive 80% of revenue"
# MAGIC
# MAGIC -- Performance optimization:
# MAGIC -- • If category_id not in product_revenue CTE, join will scan full table
# MAGIC -- • Consider pre-aggregating in gold layer (mart_product_performance)
# MAGIC -- • Indexes: (category_id, revenue DESC) for faster window operations
# MAGIC
# MAGIC -- Alternative: Use NTILE for simpler quintile bucketing
# MAGIC /*
# MAGIC NTILE(5) OVER (PARTITION BY category_id ORDER BY revenue DESC) AS quintile
# MAGIC -- Then: WHERE quintile = 1  → Top 20%
# MAGIC */