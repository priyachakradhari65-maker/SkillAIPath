-- dbt Staging Model: Customer Master
-- Extracts distinct customers from bronze sales
-- Pure staging: no aggregations, no business logic

{{
  config(
    materialized='view',
    alias='vw_customers',
    tags=['staging', 'customer']
  )
}}

SELECT DISTINCT
  {{ dbt_utils.generate_surrogate_key(['CustomerName']) }} AS CustomerKey,
  CustomerName,
  CURRENT_TIMESTAMP() AS LoadedAt
FROM {{ source('bronze', 'sales') }}
WHERE CustomerName IS NOT NULL
