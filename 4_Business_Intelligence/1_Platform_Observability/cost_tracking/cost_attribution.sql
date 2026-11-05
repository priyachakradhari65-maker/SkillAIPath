-- Databricks notebook source
-- MAGIC %md
-- MAGIC # SkillAIPath Cost Tracking & Optimization
-- MAGIC 
-- MAGIC **Purpose**: Monitor Databricks usage costs and identify optimization opportunities
-- MAGIC 
-- MAGIC **Metrics**:
-- MAGIC * DBU usage by workspace/cluster
-- MAGIC * Query performance costs
-- MAGIC * Storage costs
-- MAGIC * Idle compute detection
-- MAGIC 
-- MAGIC **Schedule**: Run weekly on Mondays

-- COMMAND ----------

-- DBU USAGE ANALYSIS (Last 30 days)

SELECT 
  usage_date,
  sku_name,
  SUM(usage_quantity) as total_dbus,
  COUNT(DISTINCT workspace_id) as workspaces,
  COUNT(DISTINCT cluster_id) as clusters,
  ROUND(SUM(usage_quantity * list_price), 2) as estimated_cost_usd
FROM system.billing.usage
WHERE usage_date >= DATEADD(day, -30, CURRENT_DATE())
  AND usage_unit = 'DBU'
GROUP BY usage_date, sku_name
ORDER BY usage_date DESC, total_dbus DESC;

-- COMMAND ----------

-- TOP COST DRIVERS (Clusters)

SELECT 
  cluster_id,
  cluster_name,
  SUM(usage_quantity) as total_dbus,
  ROUND(SUM(usage_quantity * list_price), 2) as estimated_cost_usd,
  COUNT(DISTINCT usage_date) as active_days,
  ROUND(SUM(usage_quantity) / NULLIF(COUNT(DISTINCT usage_date), 0), 2) as avg_dbus_per_day
FROM system.billing.usage
WHERE usage_date >= DATEADD(day, -30, CURRENT_DATE())
  AND usage_unit = 'DBU'
  AND cluster_id IS NOT NULL
GROUP BY cluster_id, cluster_name
ORDER BY total_dbus DESC
LIMIT 20;

-- COMMAND ----------

-- EXPENSIVE QUERIES (Last 7 days)

SELECT 
  statement_id,
  user_name,
  executed_as_user_name,
  start_time,
  execution_duration / 1000.0 / 60.0 as duration_minutes,
  total_task_duration_ms / 1000.0 / 60.0 as compute_minutes,
  read_bytes / 1024.0 / 1024.0 / 1024.0 as read_gb,
  CASE 
    WHEN execution_duration > 300000 THEN 'SLOW_QUERY'
    WHEN read_bytes > 10737418240 THEN 'HIGH_IO'
    ELSE 'NORMAL'
  END as cost_flag,
  statement_text
FROM system.query.history
WHERE start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
  AND execution_duration > 60000  -- > 1 minute
ORDER BY compute_minutes DESC
LIMIT 50;

-- COMMAND ----------

-- STORAGE COST ANALYSIS

WITH table_sizes AS (
  SELECT 
    table_catalog,
    table_schema,
    table_name,
    ROUND(SUM(size) / 1024.0 / 1024.0 / 1024.0, 2) as size_gb
  FROM (
    SELECT 'bronze' as table_schema, 'stg_sales' as table_name, size 
    FROM sap_dev.bronze.stg_sales.files
    UNION ALL
    SELECT 'bronze', 'stg_purchase', size 
    FROM sap_dev.bronze.stg_purchase.files
    UNION ALL
    SELECT 'silver', 'fact_sales', size 
    FROM sap_dev.silver.fact_sales.files
    UNION ALL
    SELECT 'silver', 'fact_purchase', size 
    FROM sap_dev.silver.fact_purchase.files
    UNION ALL
    SELECT 'gold', 'mart_customer_360', size 
    FROM sap_dev.gold.mart_customer_360.files
  ) t
  CROSS JOIN (SELECT 'sap_dev' as table_catalog)
  GROUP BY table_catalog, table_schema, table_name
)
SELECT 
  *,
  ROUND(size_gb * 0.023, 2) as estimated_monthly_storage_cost_usd,
  CASE 
    WHEN size_gb > 1000 THEN 'LARGE_TABLE'
    WHEN size_gb > 100 THEN 'MEDIUM_TABLE'
    ELSE 'SMALL_TABLE'
  END as size_category
FROM table_sizes
ORDER BY size_gb DESC;

-- COMMAND ----------

-- COST OPTIMIZATION RECOMMENDATIONS

WITH cost_analysis AS (
  SELECT 
    'Slow Queries' as optimization_area,
    COUNT(*) as issue_count,
    'Optimize query performance to reduce compute costs' as recommendation,
    'HIGH' as priority
  FROM system.query.history
  WHERE start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
    AND execution_duration > 300000  -- > 5 minutes
  
  UNION ALL
  
  SELECT 
    'Small Files' as optimization_area,
    SUM(CASE WHEN size < 134217728 THEN 1 ELSE 0 END) as issue_count,
    'Run OPTIMIZE on tables with many small files' as recommendation,
    'MEDIUM' as priority
  FROM sap_dev.bronze.stg_sales.files
)
SELECT 
  *,
  CURRENT_TIMESTAMP() as report_timestamp
FROM cost_analysis
WHERE issue_count > 0
ORDER BY 
  CASE priority 
    WHEN 'HIGH' THEN 1 
    WHEN 'MEDIUM' THEN 2 
    ELSE 3 
  END;
