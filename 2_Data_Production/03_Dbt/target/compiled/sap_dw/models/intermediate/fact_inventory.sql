-- dbt Intermediate Model: Inventory Fact Table
-- Core inventory records with dimensional surrogate keys



WITH inventory_data AS (
  SELECT
    ProductName,
    WareHouseName,
    CAST(StockDate AS DATE) AS StockDate,
    StockQuantity,
    StockStatus,
    CAST(BatchDate AS DATE) AS BatchDate,
    LoadedAt
  FROM `sap_dev`.`bronze`.`stg_stock_status`
  
  WHERE LoadedAt > (
    SELECT COALESCE(MAX(LoadedAt), TIMESTAMP'1900-01-01 00:00:00')
    FROM `sap_dev`.`silver`.`fact_inventory`
  )
  
),

dimension_keys AS (
  SELECT
    COALESCE(p.ProductSK, -1) AS ProductSK,
    COALESCE(w.WareHouseSK, -1) AS WarehouseSK,
    COALESCE(d.DateID, -1) AS DateID,
    i.StockQuantity,
    CAST(i.StockQuantity * 0 AS DECIMAL(15,2)) AS StockValue,
    i.StockStatus,
    i.BatchDate,
    i.LoadedAt
  FROM inventory_data i
  LEFT JOIN `sap_dev`.`silver`.`dim_product` p 
    ON i.ProductName = p.ProductName
  LEFT JOIN `sap_dev`.`silver`.`dim_warehouse` w 
    ON i.WareHouseName = w.WareHouseName
  LEFT JOIN `sap_dev`.`silver`.`dim_date` d 
    ON i.StockDate = d.Date
)

SELECT * FROM dimension_keys