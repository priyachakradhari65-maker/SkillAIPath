-- Custom Test: Delivery must be after order

SELECT
  OrderID,
  OrderDate,
  DeliveryDate
FROM `sap_dev`.`staging`.`vw_sales`
WHERE DeliveryDate < OrderDate