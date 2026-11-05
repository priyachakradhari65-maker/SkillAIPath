-- dbt Intermediate Model: Warehouse Dimension
-- Creates warehouse dimension with location information

{{
  config(
    materialized='table',
    tags=['intermediate', 'dimension', 'warehouse']
  )
}}

WITH unique_warehouses AS (
  SELECT DISTINCT
    WareHouseName,
    WareHouseCity
  FROM {{ source('bronze', 'sales') }}
  WHERE WareHouseName IS NOT NULL
)

SELECT
  ROW_NUMBER() OVER (ORDER BY WareHouseName) AS WareHouseSK,
  ROW_NUMBER() OVER (ORDER BY WareHouseName) AS WareHouseID,
  WareHouseName,
  WareHouseCity
FROM unique_warehouses
