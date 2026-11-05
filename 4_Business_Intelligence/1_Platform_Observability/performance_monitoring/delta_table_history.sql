-- Databricks notebook source
-- DELTA TABLE OPERATION HISTORY

SELECT 
  'fact_sales' as table_name,
  timestamp,
  version,
  operation,
  operationMetrics,
  userIdentity,
  ROUND((operationMetrics['numOutputBytes'] / 1024.0 / 1024.0), 2) as output_mb
FROM (DESCRIBE HISTORY sap_dev.silver.fact_sales)
WHERE timestamp >= DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY timestamp DESC;
