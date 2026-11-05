-- ============================================================================
-- Lakebase Postgres Schema for Live Apps
-- Production-grade table schemas with constraints, indexes, and audit fields
-- ============================================================================

-- Database: retail-ops-apps
-- Branch: production
-- Endpoint: projects/retail-ops-apps/branches/production/endpoints/primary

-- ============================================================================
-- 1. Reorder Approvals Table (App 1: Inventory Command Center)
-- ============================================================================

CREATE TABLE IF NOT EXISTS reorder_approvals (
    approval_id SERIAL PRIMARY KEY,
    product_sk INTEGER NOT NULL,
    product_name VARCHAR(200) NOT NULL,
    warehouse_sk INTEGER NOT NULL,
    warehouse_name VARCHAR(100) NOT NULL,
    recommended_qty INTEGER NOT NULL CHECK (recommended_qty > 0),
    current_stock INTEGER NOT NULL CHECK (current_stock >= 0),
    urgency VARCHAR(20) NOT NULL CHECK (urgency IN ('Critical', 'High', 'Medium', 'Low')),
    status VARCHAR(20) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'rejected')),
    approved_by VARCHAR(100),
    rejected_reason TEXT,
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_reorder_status ON reorder_approvals(status);
CREATE INDEX IF NOT EXISTS idx_reorder_urgency ON reorder_approvals(urgency);
CREATE INDEX IF NOT EXISTS idx_reorder_created ON reorder_approvals(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_reorder_product ON reorder_approvals(product_sk, warehouse_sk);

-- Trigger to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_reorder_approvals_updated_at BEFORE UPDATE
    ON reorder_approvals FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

COMMENT ON TABLE reorder_approvals IS 'Stores reorder approval requests and their workflow status';
COMMENT ON COLUMN reorder_approvals.urgency IS 'Urgency level based on days of stock remaining';
COMMENT ON COLUMN reorder_approvals.status IS 'Approval workflow status: pending, approved, rejected';


-- ============================================================================
-- 2. Forecast Runs Table (App 2: Demand Forecasting Dashboard)
-- ============================================================================

CREATE TABLE IF NOT EXISTS forecast_runs (
    run_id SERIAL PRIMARY KEY,
    warehouse_name VARCHAR(100) NOT NULL,
    product_category VARCHAR(100) NOT NULL,
    forecast_horizon_days INTEGER NOT NULL CHECK (forecast_horizon_days BETWEEN 7 AND 365),
    confidence_level INTEGER NOT NULL CHECK (confidence_level BETWEEN 80 AND 99),
    model_config JSONB,
    mape DECIMAL(10, 2),
    rmse DECIMAL(10, 2),
    data_points INTEGER,
    run_by VARCHAR(100) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_forecast_warehouse ON forecast_runs(warehouse_name);
CREATE INDEX IF NOT EXISTS idx_forecast_created ON forecast_runs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_forecast_run_by ON forecast_runs(run_by);

COMMENT ON TABLE forecast_runs IS 'Audit trail of all demand forecast runs with model configs and metrics';
COMMENT ON COLUMN forecast_runs.model_config IS 'JSON configuration of Prophet model parameters';
COMMENT ON COLUMN forecast_runs.mape IS 'Mean Absolute Percentage Error (forecast accuracy metric)';
COMMENT ON COLUMN forecast_runs.rmse IS 'Root Mean Square Error';


-- ============================================================================
-- 3. Pricing Rules Table (App 3: Dynamic Pricing API)
-- ============================================================================

CREATE TABLE IF NOT EXISTS pricing_rules (
    rule_id SERIAL PRIMARY KEY,
    rule_name VARCHAR(100) UNIQUE NOT NULL,
    stock_status VARCHAR(20) CHECK (stock_status IN ('Critical', 'Understock', 'Overstock', 'Healthy')),
    health_status VARCHAR(20) CHECK (health_status IN ('Critical', 'Understock', 'Overstock', 'Healthy')),
    price_adjustment_pct DECIMAL(5, 2) NOT NULL CHECK (price_adjustment_pct BETWEEN -90 AND 100),
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_by VARCHAR(100) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_pricing_active ON pricing_rules(is_active);
CREATE INDEX IF NOT EXISTS idx_pricing_stock_health ON pricing_rules(stock_status, health_status);
CREATE INDEX IF NOT EXISTS idx_pricing_created ON pricing_rules(created_at DESC);

CREATE TRIGGER update_pricing_rules_updated_at BEFORE UPDATE
    ON pricing_rules FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

COMMENT ON TABLE pricing_rules IS 'Dynamic pricing rules based on inventory health and stock status';
COMMENT ON COLUMN pricing_rules.price_adjustment_pct IS 'Percentage to adjust price (-90 to +100)';
COMMENT ON COLUMN pricing_rules.is_active IS 'Whether this rule is currently active';


-- ============================================================================
-- 4. Price Change History Table (App 3: Dynamic Pricing API)
-- ============================================================================

CREATE TABLE IF NOT EXISTS price_change_history (
    change_id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL,
    warehouse_id INTEGER NOT NULL,
    old_price DECIMAL(10, 2) NOT NULL CHECK (old_price >= 0),
    new_price DECIMAL(10, 2) NOT NULL CHECK (new_price >= 0),
    adjustment_pct DECIMAL(5, 2) NOT NULL,
    applied_rule VARCHAR(100),
    changed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_price_history_product ON price_change_history(product_id);
CREATE INDEX IF NOT EXISTS idx_price_history_warehouse ON price_change_history(warehouse_id);
CREATE INDEX IF NOT EXISTS idx_price_history_changed ON price_change_history(changed_at DESC);
CREATE INDEX IF NOT EXISTS idx_price_history_product_warehouse ON price_change_history(product_id, warehouse_id);

COMMENT ON TABLE price_change_history IS 'Audit trail of all price changes with applied rules';
COMMENT ON COLUMN price_change_history.adjustment_pct IS 'Percentage adjustment applied to base price';
COMMENT ON COLUMN price_change_history.applied_rule IS 'Name of pricing rule that was applied';


-- ============================================================================
-- Verification Queries
-- ============================================================================

-- Check tables exist
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'public' 
AND table_name IN ('reorder_approvals', 'forecast_runs', 'pricing_rules', 'price_change_history');

-- Check row counts
SELECT 
    'reorder_approvals' as table_name, COUNT(*) as row_count FROM reorder_approvals
UNION ALL
SELECT 
    'forecast_runs', COUNT(*) FROM forecast_runs
UNION ALL
SELECT 
    'pricing_rules', COUNT(*) FROM pricing_rules
UNION ALL
SELECT 
    'price_change_history', COUNT(*) FROM price_change_history;

-- ============================================================================
-- Sample Data Insertion (Optional - for testing)
-- ============================================================================

-- Sample pricing rule
INSERT INTO pricing_rules (
    rule_name, stock_status, health_status, price_adjustment_pct, created_by
) VALUES 
    ('Critical Stock Discount', 'Critical', 'Critical', -15.00, 'system'),
    ('Overstock Clearance', 'Overstock', 'Overstock', -25.00, 'system'),
    ('Healthy Stock Premium', 'Healthy', 'Healthy', 5.00, 'system')
ON CONFLICT (rule_name) DO NOTHING;

COMMENT ON SCHEMA public IS 'Production schema for retail operations apps';
