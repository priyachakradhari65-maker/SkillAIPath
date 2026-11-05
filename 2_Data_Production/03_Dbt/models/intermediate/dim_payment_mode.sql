-- dbt Intermediate Model: Payment Mode Dimension
-- Creates payment mode dimension from sales transactions

{{
  config(
    materialized='table',
    tags=['intermediate', 'dimension', 'payment']
  )
}}

WITH unique_payment_modes AS (
  SELECT DISTINCT
    PaymentMode
  FROM {{ ref('stg_sales') }}
  WHERE PaymentMode IS NOT NULL
)

SELECT
  ROW_NUMBER() OVER (ORDER BY PaymentMode) AS PaymentModeSK,
  ROW_NUMBER() OVER (ORDER BY PaymentMode) AS PaymentModeID,
  PaymentMode
FROM unique_payment_modes
