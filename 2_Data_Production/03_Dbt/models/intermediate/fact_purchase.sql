-- dbt Intermediate Model: Purchase Fact Table
-- Core purchase transactions with dimensional surrogate keys
--
-- >>> Phase P1 (2026-11-03): interim FULL REBUILD (self-healing) <<<
-- PurchaseID restarts at 1 every batch day, so the previous PurchaseID-only
-- incremental merge collapse-deleted duplicate-ID history (evidence: 1,100
-- bronze rows -> 1,000 fact rows; 100 lost, 200 PurchaseDate drifts). A
-- composite-key incremental merge is NOT deployable until existing rows get
-- their BatchDate backfilled (NULL keys would duplicate every row on the
-- first run). Until that backfill lands, rebuild daily from the (now
-- shielded) bronze history — deterministic, idempotent, and RESTORES the 100
-- lost rows and correct PurchaseDates automatically. Switch back to
-- incremental with unique_key=['PurchaseID','BatchDate'] + a forward-only
-- BatchDate guard after the backfill.

{{
  config(
    materialized='table',
    tags=['intermediate', 'fact', 'purchase']
  )
}}

WITH purchase_data AS (
  SELECT
    CAST(PurchaseID AS BIGINT) AS PurchaseID,
    ProductName,
    WareHouseName,
    WholeSalerName,
    CAST(PurchaseDate AS DATE) AS PurchaseDate,
    Quantity,
    UnitCost,
    NetAmount,
    BatchDate, -- added Phase P1: composite grain (PurchaseID, BatchDate)
    LoadedAt
  FROM {{ source('bronze', 'purchases') }}
  {% if is_incremental() %}
  WHERE LoadedAt > (
    SELECT COALESCE(MAX(LoadedAt), TIMESTAMP'1900-01-01 00:00:00')
    FROM {{ this }}
  )
  {% endif %}
),

dimension_keys AS (
  SELECT
    p.PurchaseID,
    COALESCE(prod.ProductSK, -1) AS ProductSK,
    COALESCE(w.WareHouseSK, -1) AS WarehouseSK,
    COALESCE(ws.WholeSalerSK, -1) AS WholesalerSK,
    COALESCE(d.DateID, -1) AS PurchaseDateID,
    p.Quantity,
    CAST(p.UnitCost AS DECIMAL(10,2)) AS UnitCost,
    CAST(p.NetAmount AS DECIMAL(10,2)) AS NetAmount,
    p.PurchaseDate,
    p.BatchDate, -- added Phase P1: composite grain (PurchaseID, BatchDate)
    p.LoadedAt
  FROM purchase_data p
  LEFT JOIN {{ ref('dim_product') }} prod 
    ON p.ProductName = prod.ProductName
  LEFT JOIN {{ ref('dim_warehouse') }} w 
    ON p.WareHouseName = w.WareHouseName
  LEFT JOIN {{ ref('dim_wholesaler') }} ws 
    ON p.WholeSalerName = ws.WholeSalerName
  LEFT JOIN {{ ref('dim_date') }} d 
    ON p.PurchaseDate = d.Date
)

SELECT * FROM dimension_keys
