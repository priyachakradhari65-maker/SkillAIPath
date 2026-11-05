-- dbt Intermediate Model: Warehouse Dimension
-- Creates warehouse dimension with location information



WITH unique_warehouses AS (
  SELECT DISTINCT
    WareHouseName,
    WareHouseCity
  FROM `sap_dev`.`bronze`.`stg_sales`
  WHERE WareHouseName IS NOT NULL
)

SELECT
  ROW_NUMBER() OVER (ORDER BY WareHouseName) AS WareHouseSK,
  ROW_NUMBER() OVER (ORDER BY WareHouseName) AS WareHouseID,
  WareHouseName,
  WareHouseCity
FROM unique_warehouses