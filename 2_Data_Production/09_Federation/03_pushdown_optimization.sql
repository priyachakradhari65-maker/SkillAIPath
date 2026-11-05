-- Query Pushdown Optimization
-- Databricks pushes filters/aggregations to external DB

-- GOOD: Filter pushed down to MySQL (only matching rows transferred)
SELECT * FROM mysql_catalog.production.customers
WHERE created_date >= '2024-01-01';

-- BAD: All rows transferred, then filtered in Databricks
SELECT * FROM mysql_catalog.production.customers;
-- Then filter in Python: df.filter(col("created_date") >= "2024-01-01")

-- Check pushdown with EXPLAIN
EXPLAIN SELECT * FROM mysql_catalog.production.customers
WHERE country = 'India';
