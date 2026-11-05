-- dbt Staging Model: Purchase Orders
-- Cleanses and standardizes bronze purchase data

{{
  config(
    materialized='view',
    alias='vw_purchases',
    tags=['staging', 'purchase']
  )
}}

SELECT
  PurchaseID,
  WholeSalerName AS WholesalerName,
  ProductName,
  WareHouseName,
  CAST(PurchaseDate AS DATE) AS PurchaseDate,
  Quantity AS ProductQuantity,
  ROUND(UnitCost, 2) AS PurchasePrice,
  ROUND(NetAmount, 2) AS TotalCost,
  CAST(BatchDate AS DATE) AS BatchDate
FROM {{ source('bronze', 'purchases') }}
WHERE NetAmount > 0
