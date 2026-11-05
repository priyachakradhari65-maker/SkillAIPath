-- Databricks notebook source
-- EXECUTION PLAN ANALYSIS
-- Purpose: Understand query optimization and identify inefficiencies
-- Usage: Run EXPLAIN on slow queries to analyze execution strategy

-- Query 1: Sample Query with Execution Plan
EXPLAIN EXTENDED
SELECT 
  s.store_id,
  s.store_name,
  SUM(o.order_amount) as total_revenue,
  COUNT(DISTINCT o.customer_id) as unique_customers,
  AVG(o.order_amount) as avg_order_value
FROM 
  main.silver.orders o
  INNER JOIN main.silver.stores s ON o.store_id = s.store_id
WHERE 
  o.order_date >= CURRENT_DATE() - INTERVAL 90 DAYS
GROUP BY 
  s.store_id, s.store_name
HAVING 
  SUM(o.order_amount) > 100000
ORDER BY 
  total_revenue DESC;

-- Query 2: Identify Queries with Multiple Shuffles
-- High shuffle count indicates optimization opportunity
SELECT 
  query_id,
  statement_text,
  execution_status,
  rows_produced,
  total_task_duration_ms / 1000.0 as total_duration_sec,
  shuffle_read_bytes / 1024.0 / 1024.0 / 1024.0 as shuffle_read_gb,
  shuffle_write_bytes / 1024.0 / 1024.0 / 1024.0 as shuffle_write_gb
FROM 
  system.query.history
WHERE 
  start_time >= CURRENT_DATE() - INTERVAL 7 DAYS
  AND (shuffle_read_bytes > 10737418240 OR shuffle_write_bytes > 10737418240)
ORDER BY 
  shuffle_read_gb DESC
LIMIT 50;

-- Query 3: Long-Running Queries (Potential Optimization Targets)
SELECT 
  query_id,
  user_name,
  statement_text,
  start_time,
  end_time,
  TIMESTAMPDIFF(MINUTE, start_time, end_time) as runtime_minutes,
  rows_produced,
  execution_status,
  error_message
FROM 
  system.query.history
WHERE 
  start_time >= CURRENT_DATE() - INTERVAL 3 DAYS
  AND TIMESTAMPDIFF(MINUTE, start_time, end_time) > 30
ORDER BY 
  runtime_minutes DESC
LIMIT 100;