-- ============================================================================
-- 06_curated_view_builder.sql
-- Google Sheets Curated View - Data Quality & Business Rules
-- ============================================================================
-- PURPOSE: Transform raw Google Sheets data into clean, curated analytics view
-- AUTHOR: SkillAIPath
-- UPDATED: January 2025
-- FREE EDITION COMPATIBLE: 100%
-- ============================================================================

-- Configuration (Override via environment variables)
-- Default: sap catalog, dev_gold schema
-- To override: export SKILLAIPPATH_CATALOG=<catalog_name>
--             export SKILLAIPPATH_ENV=<dev|uat|prod>

USE CATALOG sap;

-- ============================================================================
-- CURATED VIEW: google_sheet_curated
-- ============================================================================
-- TRANSFORMATIONS APPLIED:
-- 1. Data Quality: NULL filtering, data type validation
-- 2. Standardization: TRIM, UPPER for consistency
-- 3. Business Rules: Revenue segmentation, categorical enrichment
-- 4. Metadata: Audit trail columns preserved
-- 5. Freshness Filter: Only data synced within last 30 days
-- ============================================================================

CREATE OR REPLACE VIEW dev_gold.google_sheet_curated AS

SELECT
  -- ===========================================================================
  -- DATA QUALITY TRANSFORMATIONS
  -- ===========================================================================
  
  -- Standardize text columns (remove whitespace, uppercase for consistency)
  TRIM(UPPER(Product)) AS product_name_clean,
  TRIM(UPPER(City)) AS city_clean,
  
  -- Coalesce NULLs to defaults (prevent downstream errors)
  COALESCE(Revenue, 0.0) AS revenue_clean,
  COALESCE(Quantity, 0) AS quantity_clean,
  
  -- ===========================================================================
  -- BUSINESS RULES
  -- ===========================================================================
  
  -- Revenue Segmentation (business logic)
  CASE 
    WHEN Revenue >= 10000 THEN 'High Value'
    WHEN Revenue >= 5000 THEN 'Medium Value'
    WHEN Revenue >= 1000 THEN 'Low Value'
    ELSE 'Micro Value'
  END AS revenue_segment,
  
  -- Order Size Classification (operational categorization)
  CASE
    WHEN Quantity >= 10 THEN 'Bulk'
    WHEN Quantity >= 5 THEN 'Standard'
    ELSE 'Small'
  END AS order_size_category,
  
  -- Unit Price Calculation (derived metric)
  CASE 
    WHEN Quantity > 0 THEN ROUND(Revenue / Quantity, 2)
    ELSE 0.0
  END AS unit_price,
  
  -- ===========================================================================
  -- ORIGINAL COLUMNS (preserved for traceability)
  -- ===========================================================================
  
  Date AS transaction_date,
  Product AS product_name_original,
  Revenue AS revenue_original,
  Quantity AS quantity_original,
  City AS city_original,
  
  -- ===========================================================================
  -- METADATA COLUMNS (audit trail from raw table)
  -- ===========================================================================
  
  sync_timestamp,
  sync_date,
  source
  
FROM dev_gold.google_sheet_raw

-- ===========================================================================
-- DATA QUALITY FILTERS (exclude invalid records)
-- ===========================================================================

WHERE 
  -- Exclude NULL critical fields
  Product IS NOT NULL
  AND Revenue IS NOT NULL
  AND Quantity IS NOT NULL
  AND Date IS NOT NULL
  AND City IS NOT NULL
  
  -- Exclude negative/zero revenue (data quality issue)
  AND Revenue > 0
  
  -- Exclude negative quantity (data quality issue)
  AND Quantity > 0
  
  -- Freshness filter: Only data synced within last 30 days
  -- (prevents stale data from appearing in dashboards)
  AND sync_date >= CURRENT_DATE - INTERVAL 30 DAYS
;

-- ============================================================================
-- VERIFICATION QUERY
-- ============================================================================
-- Run this after view creation to validate

SELECT 
  'CURATED VIEW VALIDATION' AS check_type,
  COUNT(*) AS total_rows,
  COUNT(DISTINCT product_name_clean) AS unique_products,
  COUNT(DISTINCT city_clean) AS unique_cities,
  ROUND(SUM(revenue_clean), 2) AS total_revenue,
  ROUND(AVG(revenue_clean), 2) AS avg_revenue,
  MIN(transaction_date) AS earliest_date,
  MAX(transaction_date) AS latest_date,
  COUNT(DISTINCT sync_date) AS sync_runs
FROM dev_gold.google_sheet_curated;

-- ============================================================================
-- SAMPLE OUTPUT
-- ============================================================================
-- Preview first 10 rows to verify transformations

SELECT *
FROM dev_gold.google_sheet_curated
ORDER BY transaction_date DESC, revenue_clean DESC
LIMIT 10;

-- ============================================================================
-- NEXT STEPS
-- ============================================================================
-- 1. Verify view created successfully:
--    SHOW VIEWS IN dev_gold LIKE 'google_sheet_curated';
--
-- 2. Create Genie Space (07_genie_space_setup_sheets.py)
--    Point to: sap.dev_gold.google_sheet_curated
--
-- 3. Build Dashboard (08_dashboard_builder_sheets.py)
--    Query curated view, not raw table
--
-- 4. Schedule Refresh (09_schedule_refresh_job.py)
--    Job runs: 05_google_sheet_sync.py -> Auto-updates curated view
-- ============================================================================