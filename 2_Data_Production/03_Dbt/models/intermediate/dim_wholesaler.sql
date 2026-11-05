-- dbt Intermediate Model: Wholesaler Dimension
-- Creates wholesaler dimension from purchase transactions

{{
  config(
    materialized='table',
    tags=['intermediate', 'dimension', 'wholesaler']
  )
}}

WITH unique_wholesalers AS (
  SELECT DISTINCT
    WholeSalerName
  FROM {{ ref('stg_purchases') }}
  WHERE WholeSalerName IS NOT NULL
)

SELECT
  ROW_NUMBER() OVER (ORDER BY WholeSalerName) AS WholeSalerSK,
  ROW_NUMBER() OVER (ORDER BY WholeSalerName) AS WholeSalerID,
  WholeSalerName
FROM unique_wholesalers
