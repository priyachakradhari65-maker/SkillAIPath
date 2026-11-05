-- ============================================================================
-- Lakebase SQL Editor Showcase - Demonstrating OLTP Capabilities
-- Run these queries in Lakebase SQL Editor to showcase functionality
-- ============================================================================

-- ============================================================================
-- SECTION 1: Database & Schema Exploration
-- ============================================================================

-- 1.1 Check current database and connection
SELECT 
    current_database() as database_name,
    current_user as connected_as,
    version() as postgres_version;

-- 1.2 List all tables in public schema
SELECT 
    table_name,
    table_type
FROM information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_name;

-- 1.3 Get row counts for all tables
SELECT 
    'reorder_approvals' as table_name, 
    COUNT(*) as row_count,
    pg_size_pretty(pg_total_relation_size('reorder_approvals')) as total_size
FROM reorder_approvals
UNION ALL
SELECT 
    'forecast_runs', 
    COUNT(*),
    pg_size_pretty(pg_total_relation_size('forecast_runs'))
FROM forecast_runs
UNION ALL
SELECT 
    'pricing_rules', 
    COUNT(*),
    pg_size_pretty(pg_total_relation_size('pricing_rules'))
FROM pricing_rules
UNION ALL
SELECT 
    'price_change_history', 
    COUNT(*),
    pg_size_pretty(pg_total_relation_size('price_change_history'))
FROM price_change_history;


-- ============================================================================
-- SECTION 2: Reorder Approvals (Inventory Command Center)
-- ============================================================================

-- 2.1 View all pending reorder requests (by urgency)
SELECT 
    approval_id,
    product_name,
    warehouse_name,
    current_stock,
    recommended_qty,
    (recommended_qty - current_stock) as qty_needed,
    urgency,
    status,
    created_at
FROM reorder_approvals
WHERE status = 'pending'
ORDER BY 
    CASE urgency 
        WHEN 'Critical' THEN 1
        WHEN 'High' THEN 2
        WHEN 'Medium' THEN 3
        WHEN 'Low' THEN 4
    END,
    created_at ASC;

-- 2.2 Critical items needing immediate attention
SELECT 
    product_name,
    warehouse_name,
    current_stock,
    recommended_qty,
    urgency,
    AGE(NOW(), created_at) as pending_duration
FROM reorder_approvals
WHERE status = 'pending' 
  AND urgency = 'Critical'
ORDER BY created_at ASC;

-- 2.3 Approval workflow statistics
SELECT 
    status,
    urgency,
    COUNT(*) as request_count,
    AVG(recommended_qty - current_stock) as avg_qty_needed,
    SUM(recommended_qty - current_stock) as total_qty_needed
FROM reorder_approvals
GROUP BY status, urgency
ORDER BY status, urgency;


-- ============================================================================
-- SECTION 3: ACID Transaction Demonstration
-- ============================================================================

-- 3.1 Atomic approval workflow (TRANSACTION)
BEGIN;

-- Approve a pending request
UPDATE reorder_approvals
SET 
    status = 'approved',
    approved_by = 'manager@company.com',
    updated_at = CURRENT_TIMESTAMP
WHERE approval_id = 1 
  AND status = 'pending';

-- Verify the change
SELECT 
    approval_id,
    product_name,
    status,
    approved_by,
    updated_at
FROM reorder_approvals
WHERE approval_id = 1;

-- If everything looks good, commit
COMMIT;
-- If you want to undo: ROLLBACK;


-- ============================================================================
-- SECTION 4: Dynamic Pricing Engine
-- ============================================================================

-- 4.1 View all active pricing rules
SELECT 
    rule_id,
    rule_name,
    stock_status,
    health_status,
    price_adjustment_pct,
    created_by,
    created_at
FROM pricing_rules
WHERE is_active = true
ORDER BY price_adjustment_pct DESC;

-- 4.2 Price change audit trail (last 30 days)
SELECT 
    product_sk,
    product_name,
    old_price,
    new_price,
    price_adjustment_pct,
    rule_applied,
    stock_status,
    health_status,
    changed_at
FROM price_change_history
WHERE changed_at >= NOW() - INTERVAL '30 days'
ORDER BY changed_at DESC
LIMIT 50;


-- ============================================================================
-- SECTION 5: Sample Data Insert (For Testing)
-- ============================================================================

-- 5.1 Insert sample reorder approval
INSERT INTO reorder_approvals (
    product_sk, product_name, warehouse_sk, warehouse_name,
    recommended_qty, current_stock, urgency, status
) VALUES (
    1, 'Aashirvaad Atta 10kg', 5, 'Chennai Port WH',
    250, 172, 'High', 'pending'
)
RETURNING *;

-- 5.2 Insert sample pricing rule
INSERT INTO pricing_rules (
    rule_name, stock_status, health_status, 
    price_adjustment_pct, is_active, created_by
) VALUES (
    'Flash Sale - Overstocked Items', 'Overstock', 'Overstock',
    -30.00, true, 'pricing_engine'
)
RETURNING *;


-- ============================================================================
-- SECTION 6: Advanced Analytics (Premium Demo Queries)
-- ============================================================================

-- 6.1 Multi-warehouse inventory crisis dashboard
SELECT 
    warehouse_name,
    COUNT(*) as total_requests,
    COUNT(*) FILTER (WHERE status = 'pending') as pending_count,
    COUNT(*) FILTER (WHERE status = 'approved') as approved_count,
    COUNT(*) FILTER (WHERE urgency = 'Critical') as critical_count,
    SUM(recommended_qty - current_stock) as total_units_needed,
    AVG(recommended_qty - current_stock)::INTEGER as avg_reorder_size,
    MIN(created_at) as first_request_time,
    AGE(NOW(), MIN(created_at)) as time_since_first_request
FROM reorder_approvals
GROUP BY warehouse_name
ORDER BY critical_count DESC, pending_count DESC;

-- 6.2 Stock health scoring with window functions
SELECT 
    approval_id,
    product_name,
    warehouse_name,
    current_stock,
    recommended_qty,
    ROUND(100.0 * current_stock / NULLIF(recommended_qty, 0), 1) as stock_health_pct,
    urgency,
    status,
    RANK() OVER (PARTITION BY warehouse_name ORDER BY current_stock ASC) as warehouse_priority,
    PERCENT_RANK() OVER (ORDER BY (recommended_qty - current_stock) DESC) as global_urgency_score,
    LAG(product_name) OVER (PARTITION BY warehouse_name ORDER BY approval_id) as prev_product,
    LEAD(product_name) OVER (PARTITION BY warehouse_name ORDER BY approval_id) as next_product
FROM reorder_approvals
WHERE status = 'pending'
ORDER BY warehouse_name, warehouse_priority;

-- 6.3 Forecast vs Actual Demand Analysis (CTE + Complex Join)
WITH forecast_summary AS (
    SELECT 
        warehouse_name,
        COUNT(*) as total_forecasts,
        AVG(model_mae) as avg_mae,
        MIN(confidence_level) as min_confidence,
        MAX(confidence_level) as max_confidence
    FROM forecast_runs
    GROUP BY warehouse_name
),
reorder_summary AS (
    SELECT 
        warehouse_name,
        COUNT(*) as total_reorders,
        SUM(recommended_qty - current_stock) as total_shortage
    FROM reorder_approvals
    WHERE status = 'pending'
    GROUP BY warehouse_name
)
SELECT 
    COALESCE(f.warehouse_name, r.warehouse_name) as warehouse,
    COALESCE(f.total_forecasts, 0) as forecast_runs,
    COALESCE(f.avg_mae, 0)::NUMERIC(10,2) as avg_forecast_error,
    COALESCE(f.min_confidence, 0) as min_conf_level,
    COALESCE(r.total_reorders, 0) as pending_reorders,
    COALESCE(r.total_shortage, 0) as total_units_short,
    CASE 
        WHEN f.avg_mae < 15 THEN 'Excellent Forecast'
        WHEN f.avg_mae < 20 THEN 'Good Forecast'
        ELSE 'Needs Improvement'
    END as forecast_quality
FROM forecast_summary f
FULL OUTER JOIN reorder_summary r ON f.warehouse_name = r.warehouse_name
ORDER BY pending_reorders DESC, avg_forecast_error ASC;

-- 6.4 Real-time system health dashboard
SELECT 
    'SYSTEM STATUS' as category,
    'Lakebase Postgres 17' as metric,
    'OPERATIONAL' as status,
    current_timestamp as checked_at
UNION ALL
SELECT 
    'DATA VOLUME',
    'Total Records',
    (SELECT COUNT(*) FROM reorder_approvals)::TEXT || ' reorders + ' ||
    (SELECT COUNT(*) FROM forecast_runs)::TEXT || ' forecasts + ' ||
    (SELECT COUNT(*) FROM price_change_history)::TEXT || ' price changes',
    current_timestamp
UNION ALL
SELECT 
    'BUSINESS METRICS',
    'Critical Alerts',
    (SELECT COUNT(*)::TEXT FROM reorder_approvals WHERE urgency = 'Critical' AND status = 'pending') || ' pending',
    current_timestamp
UNION ALL
SELECT 
    'BUSINESS METRICS',
    'Approval Rate',
    ROUND((SELECT COUNT(*)::NUMERIC FROM reorder_approvals WHERE status = 'approved') * 100.0 / 
          NULLIF((SELECT COUNT(*) FROM reorder_approvals), 0), 1)::TEXT || '%',
    current_timestamp
UNION ALL
SELECT 
    'BUSINESS METRICS',
    'Best Forecast',
    (SELECT warehouse_name || ' (' || model_mae::TEXT || ' MAE)' 
     FROM forecast_runs ORDER BY model_mae ASC LIMIT 1),
    current_timestamp
UNION ALL
SELECT 
    'BUSINESS METRICS',
    'Recent Price Changes',
    (SELECT COUNT(*)::TEXT FROM price_change_history WHERE changed_at > NOW() - INTERVAL '1 hour') || ' in last hour',
    current_timestamp;


-- ============================================================================
-- END OF SHOWCASE QUERIES
-- ============================================================================
