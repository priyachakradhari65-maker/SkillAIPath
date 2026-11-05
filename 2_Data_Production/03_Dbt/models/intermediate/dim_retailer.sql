-- dbt Intermediate Model: Retailer Dimension
-- Creates retailer dimension from sales transactions

{{
  config(
    materialized='table',
    tags=['intermediate', 'dimension', 'retailer']
  )
}}

WITH unique_retailers AS (
  SELECT DISTINCT
    RetailerName,
    RetailerCity,
    RetailerContact,
    RetailerMail
  FROM {{ ref('stg_sales') }}
  WHERE RetailerName IS NOT NULL
)

SELECT
  ROW_NUMBER() OVER (ORDER BY RetailerName) AS RetailerSK,
  ROW_NUMBER() OVER (ORDER BY RetailerName) AS RetailerID,
  RetailerName,
  RetailerCity,
  RetailerContact,
  RetailerMail
FROM unique_retailers
