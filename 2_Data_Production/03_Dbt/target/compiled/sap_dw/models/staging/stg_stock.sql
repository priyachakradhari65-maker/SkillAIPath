-- dbt Staging Model: Stock Inventory
-- Cleanses and standardizes bronze inventory data
-- Pure staging: no business logic



SELECT
  StockID,
  ProductName,
  WarehouseName AS WareHouseName,
  CAST(StockDate AS DATE) AS SnapshotDate,
  StockQuantity AS CurrentStock,
  ReorderLevel,
  CAST(BatchDate AS DATE) AS BatchDate,
  CAST(LoadedAt AS TIMESTAMP) AS LoadedAt
FROM `sap_dev`.`bronze`.`stg_stock_status`
WHERE StockQuantity >= 0