-- dbt Intermediate Model: Sales Fact Table
-- Core sales transactions with dimensional surrogate keys

{{
  config(
    materialized='incremental',
    unique_key=['OrderID', 'BatchDate'],
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    tags=['intermediate', 'fact', 'sales']
  )
}}

WITH sales_data AS (
  SELECT
    s.OrderID,
    s.CustomerID, -- added PhaseC2: CDC-owned dim_customer is keyed by CustomerID
    s.OrderDate,
    s.MFDDate,
    s.EXPDate,
    s.DeliveryDate,
    s.CustomerName,
    s.ProductName,
    s.ProductCategory,
    s.WareHouseName,
    s.RetailerName,
    s.AgentName,
    s.PaymentMode,
    s.ProductQuantity,
    s.CostPrice,
    s.SellingPrice,
    s.Discount,
    s.FinalPrice,
    s.CGST,
    s.SGST,
    s.BatchDate,
    s.LoadedAt
  FROM {{ ref('stg_sales') }} s
  {% if is_incremental() %}
  WHERE s.LoadedAt > (
    SELECT COALESCE(MAX(LoadedAt), TIMESTAMP'1900-01-01 00:00:00')
    FROM {{ this }}
  )
  -- >>> GUARD (Phase P1, 2026-11-03): forward-only batch filter <<<
  -- Jobs 07/24 generators re-write staging rows for already-processed BatchDates
  -- (different random data, newer LoadedAt). Without this guard the incremental
  -- MERGE silently UPDATE-overwrites valid fact history for those dates.
  AND s.BatchDate > (
    SELECT COALESCE(MAX(BatchDate), DATE'1900-01-01')
    FROM {{ this }}
  )
  {% endif %}
),

dimension_keys AS (
  SELECT
    CAST(s.OrderID AS BIGINT) AS OrderID,
    s.CustomerID, -- added PhaseC2: business key (carried for traceability)
    COALESCE(c.CustomerSK, -1) AS CustomerSK,
    COALESCE(p.ProductSK, -1) AS ProductSK,
    COALESCE(w.WareHouseSK, -1) AS WarehouseSK,
    COALESCE(r.RetailerSK, -1) AS RetailerSK,
    COALESCE(a.AgentSK, -1) AS AgentSK,
    COALESCE(pm.PaymentModeSK, -1) AS PaymentModeSK,
    COALESCE(pc.ProductCategorySK, -1) AS ProductCategorySK,
    COALESCE(d1.DateID, -1) AS OrderDateID,
    COALESCE(d2.DateID, -1) AS MFDDateID,
    COALESCE(d3.DateID, -1) AS EXPDateID,
    COALESCE(d4.DateID, -1) AS DeliveryDateID,
    s.ProductQuantity,
    CAST(s.CostPrice AS DECIMAL(10,2)) AS CostPrice,
    CAST(s.SellingPrice AS DECIMAL(10,2)) AS sellingPrice,
    CAST(s.Discount AS DECIMAL(10,2)) AS discount,
    CAST(s.FinalPrice AS DECIMAL(10,2)) AS finalprice,
    CAST(s.CGST AS DECIMAL(10,2)) AS CGST,
    CAST(s.SGST AS DECIMAL(10,2)) AS SGST,
    s.BatchDate,
    s.LoadedAt
  FROM sales_data s
  LEFT JOIN {{ source('silver', 'dim_customer') }} c -- PhaseC2: CDC-owned source, ID join + SCD2 PIT
    ON  s.CustomerID = c.CustomerID
    AND s.OrderDate >= c.StartDate
    AND (c.IsCurrent = 1 OR s.OrderDate <= c.EndDate)
  LEFT JOIN {{ ref('dim_product') }} p 
    ON s.ProductName = p.ProductName
  LEFT JOIN {{ ref('dim_product_category') }} pc 
    ON s.ProductCategory = pc.ProductCategory
  LEFT JOIN {{ ref('dim_warehouse') }} w 
    ON s.WareHouseName = w.WareHouseName
  LEFT JOIN {{ ref('dim_retailer') }} r 
    ON s.RetailerName = r.RetailerName
  LEFT JOIN {{ ref('dim_agent') }} a 
    ON s.AgentName = a.AgentName
  LEFT JOIN {{ ref('dim_payment_mode') }} pm 
    ON s.PaymentMode = pm.PaymentMode
  LEFT JOIN {{ ref('dim_date') }} d1 
    ON s.OrderDate = d1.Date
  LEFT JOIN {{ ref('dim_date') }} d2 
    ON s.MFDDate = d2.Date
  LEFT JOIN {{ ref('dim_date') }} d3 
    ON s.EXPDate = d3.Date
  LEFT JOIN {{ ref('dim_date') }} d4 
    ON s.DeliveryDate = d4.Date
)

-- Defensive dedup at the business grain: keep latest LoadedAt per (OrderID, BatchDate)
-- (protects the merge from DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET if the source
--  ever delivers duplicate rows at grain)
,
deduped AS (
  SELECT * EXCEPT (_rn) FROM (
    SELECT *,
      ROW_NUMBER() OVER (
        PARTITION BY OrderID, BatchDate
        ORDER BY LoadedAt DESC
      ) AS _rn
    FROM dimension_keys
  ) WHERE _rn = 1
)

SELECT * FROM deduped
