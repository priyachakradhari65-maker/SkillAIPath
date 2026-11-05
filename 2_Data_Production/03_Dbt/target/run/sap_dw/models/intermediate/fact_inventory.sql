-- back compat for old kwarg name
  
  
  
  
  
  
      
          
              
              
          
              
              
          
              
              
          
      
  

    merge
    into
        `sap_dev`.`silver`.`fact_inventory` as DBT_INTERNAL_DEST
    using
        `fact_inventory__dbt_tmp` as DBT_INTERNAL_SOURCE
    on
        
                  DBT_INTERNAL_SOURCE.`ProductSK` <=> DBT_INTERNAL_DEST.`ProductSK`
              
    and 
                  DBT_INTERNAL_SOURCE.`WarehouseSK` <=> DBT_INTERNAL_DEST.`WarehouseSK`
              
    and 
                  DBT_INTERNAL_SOURCE.`DateID` <=> DBT_INTERNAL_DEST.`DateID`
              
    when matched
        then update set
            `ProductSK` = DBT_INTERNAL_SOURCE.`ProductSK`, `WarehouseSK` = DBT_INTERNAL_SOURCE.`WarehouseSK`, `DateID` = DBT_INTERNAL_SOURCE.`DateID`, `StockQuantity` = DBT_INTERNAL_SOURCE.`StockQuantity`, `StockValue` = DBT_INTERNAL_SOURCE.`StockValue`, `StockStatus` = DBT_INTERNAL_SOURCE.`StockStatus`, `BatchDate` = DBT_INTERNAL_SOURCE.`BatchDate`, `LoadedAt` = DBT_INTERNAL_SOURCE.`LoadedAt`
    when not matched
        then insert
            (`ProductSK`, `WarehouseSK`, `DateID`, `StockQuantity`, `StockValue`, `StockStatus`, `BatchDate`, `LoadedAt`) VALUES (DBT_INTERNAL_SOURCE.`ProductSK`, DBT_INTERNAL_SOURCE.`WarehouseSK`, DBT_INTERNAL_SOURCE.`DateID`, DBT_INTERNAL_SOURCE.`StockQuantity`, DBT_INTERNAL_SOURCE.`StockValue`, DBT_INTERNAL_SOURCE.`StockStatus`, DBT_INTERNAL_SOURCE.`BatchDate`, DBT_INTERNAL_SOURCE.`LoadedAt`)
