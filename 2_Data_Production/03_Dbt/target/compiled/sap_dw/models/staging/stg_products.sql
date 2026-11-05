-- dbt Staging Model: Product Master
-- Extracts distinct products from bronze sales
-- Pure staging: no aggregations, no business logic



SELECT DISTINCT
  md5(cast(concat(coalesce(cast(ProductName as string), '')) as string)) AS ProductKey,
  ProductName,
  CURRENT_TIMESTAMP() AS LoadedAt
FROM `sap_dev`.`bronze`.`stg_sales`
WHERE ProductName IS NOT NULL