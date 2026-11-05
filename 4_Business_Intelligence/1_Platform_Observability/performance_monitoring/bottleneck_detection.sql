-- Databricks notebook source
-- QUERY PERFORMANCE BOTTLENECK DETECTION

SELECT 
  query_id,
  user_name,
  executed_by,
  query_start_time,
  execution_duration / 1000.0 as duration_sec,
  rows_produced,
  read_bytes / 1024.0 / 1024.0 as read_mb,
  spill_to_disk_bytes / 1024.0 / 1024.0 as spill_mb,
  CASE 
    WHEN execution_duration > 300000 THEN 'SLOW'
    WHEN spill_to_disk_bytes > 0 THEN 'MEMORY_PRESSURE'
    ELSE 'NORMAL'
  END as bottleneck_type
FROM system.query.history
WHERE query_start_time >= DATEADD(day, -1, CURRENT_TIMESTAMP())
  AND execution_duration > 10000
ORDER BY execution_duration DESC
LIMIT 100;
