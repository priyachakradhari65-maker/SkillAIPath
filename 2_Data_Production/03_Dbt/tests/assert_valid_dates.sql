-- Custom Test: Delivery must be after order

SELECT
  OrderID,
  OrderDate,
  DeliveryDate
FROM {{ ref('stg_sales') }}
WHERE DeliveryDate < OrderDate
