-- dbt Staging Model: Product Master
-- Extracts distinct products from bronze sales
-- Pure staging: no aggregations, no business logic

{{
  config(
    materialized='view',
    alias='vw_products',
    tags=['staging', 'product']
  )
}}

SELECT DISTINCT
  {{ dbt_utils.generate_surrogate_key(['ProductName']) }} AS ProductKey,
  ProductName,
  CURRENT_TIMESTAMP() AS LoadedAt
FROM {{ source('bronze', 'sales') }}
WHERE ProductName IS NOT NULL
