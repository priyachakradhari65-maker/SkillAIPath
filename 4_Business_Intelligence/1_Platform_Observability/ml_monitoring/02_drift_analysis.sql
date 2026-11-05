-- Databricks notebook source
-- DRIFT ANALYSIS
-- Purpose: Analyze schema and distribution changes over time
-- Usage: Run weekly to review drift patterns

-- Query 1: Schema Drift Detection (Last 30 Days)
SELECT 
  table_name,
  column_name,
  change_type,
  change_date,
  previous_data_type,
  current_data_type
FROM 
  main.monitoring_output.schema_changes
WHERE 
  change_date >= CURRENT_DATE() - INTERVAL 30 DAYS
ORDER BY 
  change_date DESC, table_name;

-- Query 2: Statistical Drift Metrics
SELECT 
  table_name,
  column_name,
  window_start_time as metric_date,
  null_count,
  null_count / total_count as null_rate,
  LAG(null_count / total_count) OVER (PARTITION BY table_name, column_name ORDER BY window_start_time) as prev_null_rate,
  (null_count / total_count - LAG(null_count / total_count) OVER (PARTITION BY table_name, column_name ORDER BY window_start_time)) as null_rate_change,
  CASE 
    WHEN ABS(null_count / total_count - LAG(null_count / total_count) OVER (PARTITION BY table_name, column_name ORDER BY window_start_time)) > 0.05 
    THEN 'DRIFT_DETECTED'
    ELSE 'STABLE'
  END as drift_status
FROM 
  main.monitoring_output.profile_metrics
WHERE 
  window_start_time >= CURRENT_DATE() - INTERVAL 14 DAYS
ORDER BY 
  metric_date DESC, table_name, column_name;

-- Query 3: Volume Drift Analysis
WITH daily_counts AS (
  SELECT 
    table_name,
    DATE(window_start_time) as metric_date,
    MAX(total_count) as row_count
  FROM 
    main.monitoring_output.profile_metrics
  WHERE 
    window_start_time >= CURRENT_DATE() - INTERVAL 30 DAYS
  GROUP BY 
    table_name, DATE(window_start_time)
)
SELECT 
  table_name,
  metric_date,
  row_count,
  LAG(row_count) OVER (PARTITION BY table_name ORDER BY metric_date) as prev_day_count,
  ROUND((row_count - LAG(row_count) OVER (PARTITION BY table_name ORDER BY metric_date)) * 100.0 / 
        LAG(row_count) OVER (PARTITION BY table_name ORDER BY metric_date), 2) as daily_change_pct,
  CASE 
    WHEN ABS((row_count - LAG(row_count) OVER (PARTITION BY table_name ORDER BY metric_date)) * 100.0 / 
             LAG(row_count) OVER (PARTITION BY table_name ORDER BY metric_date)) > 25.0 
    THEN 'VOLUME_DRIFT'
    ELSE 'NORMAL'
  END as volume_status
FROM 
  daily_counts
ORDER BY 
  metric_date DESC, table_name;