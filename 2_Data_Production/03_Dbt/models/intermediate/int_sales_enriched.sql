-- dbt Intermediate Model: Sales with Customer and Product Context
-- Enriches sales transactions with customer and product metrics
-- Supports incremental loading with watermark-based filtering

{{
  config(
    materialized='incremental',
    unique_key=['OrderID', 'BatchDate'],
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    tags=['intermediate', 'sales', 'enriched']
  )
}}

WITH sales_base AS (
  SELECT
    OrderID,
    BatchDate,
    CustomerName,
    ProductName,
    WareHouseName,
    RetailerName,
    OrderDate,
    ProductQuantity,
    SellingPrice,
    CostPrice,
    FinalPrice,
    TotalAmount,
    LoadedAt
  FROM {{ ref('stg_sales') }}
  {% if is_incremental() %}
  WHERE LoadedAt > (
    SELECT COALESCE(MAX(LoadedAt), TIMESTAMP'1900-01-01 00:00:00')
    FROM {{ this }}
  )
  {% endif %}
),

customer_context AS (
  SELECT
    CustomerName,
    customer_segment,
    activity_status,
    total_spent AS customer_lifetime_value
  FROM {{ ref('int_customer_metrics') }}
),

product_context AS (
  SELECT
    ProductName,
    avg_margin AS product_avg_margin,
    total_orders AS product_popularity,
    avg_stock_level
  FROM {{ ref('int_product_metrics') }}
),

enriched AS (
  SELECT
    s.OrderID,
    s.BatchDate,
    s.CustomerName,
    c.customer_segment,
    c.activity_status,
    c.customer_lifetime_value,
    s.ProductName,
    p.product_avg_margin,
    p.product_popularity,
    p.avg_stock_level,
    s.WareHouseName,
    s.RetailerName,
    s.OrderDate,
    s.ProductQuantity,
    s.SellingPrice,
    s.CostPrice,
    s.FinalPrice,
    s.TotalAmount,
    ROUND(s.SellingPrice - s.CostPrice, 2) AS transaction_margin,
    ROUND((s.SellingPrice - s.CostPrice) / NULLIF(s.CostPrice, 0) * 100, 2) AS margin_pct,
    s.LoadedAt
  FROM sales_base s
  LEFT JOIN customer_context c ON s.CustomerName = c.CustomerName
  LEFT JOIN product_context p ON s.ProductName = p.ProductName
),

-- Defensive dedup at the business grain: keep latest LoadedAt per (OrderID, BatchDate)
deduped AS (
  SELECT * EXCEPT (_rn) FROM (
    SELECT *,
      ROW_NUMBER() OVER (
        PARTITION BY OrderID, BatchDate
        ORDER BY LoadedAt DESC
      ) AS _rn
    FROM enriched
  ) WHERE _rn = 1
)

SELECT * FROM deduped
