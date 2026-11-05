-- Custom Test: All sales revenue must be positive

SELECT
  OrderID,
  FinalPrice
FROM {{ ref('stg_sales') }}
WHERE FinalPrice <= 0
