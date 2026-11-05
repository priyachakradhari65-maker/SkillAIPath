-- dbt Staging Model: Stock Inventory
-- Cleanses and standardizes bronze inventory data
-- Pure staging: no business logic

{{
  config(
    materialized='view',
    alias='vw_stock',
    tags=['staging', 'inventory']
  )
}}

SELECT
  StockID,
  ProductName,
  WarehouseName AS WareHouseName,
  CAST(StockDate AS DATE) AS SnapshotDate,
  StockQuantity AS CurrentStock,
  ReorderLevel,
  CAST(BatchDate AS DATE) AS BatchDate,
  CAST(LoadedAt AS TIMESTAMP) AS LoadedAt
FROM {{ source('bronze', 'inventory') }}
WHERE StockQuantity >= 0
