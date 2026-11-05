-- Custom Test: All sales revenue must be positive

SELECT
  OrderID,
  FinalPrice
FROM `sap_dev`.`staging`.`vw_sales`
WHERE FinalPrice <= 0