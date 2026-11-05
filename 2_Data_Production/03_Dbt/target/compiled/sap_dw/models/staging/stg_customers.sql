-- dbt Staging Model: Customer Master
-- Extracts distinct customers from bronze sales
-- Pure staging: no aggregations, no business logic



SELECT DISTINCT
  md5(cast(concat(coalesce(cast(CustomerName as string), '')) as string)) AS CustomerKey,
  CustomerName,
  CURRENT_TIMESTAMP() AS LoadedAt
FROM `sap_dev`.`bronze`.`stg_sales`
WHERE CustomerName IS NOT NULL