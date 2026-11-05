-- back compat for old kwarg name
  
  
  
  
  
  
      
          
              
              
          
              
              
          
      
  

    merge
    into
        `sap_dev`.`silver`.`fact_sales` as DBT_INTERNAL_DEST
    using
        `fact_sales__dbt_tmp` as DBT_INTERNAL_SOURCE
    on
        
                  DBT_INTERNAL_SOURCE.`OrderID` <=> DBT_INTERNAL_DEST.`OrderID`
              
    and 
                  DBT_INTERNAL_SOURCE.`BatchDate` <=> DBT_INTERNAL_DEST.`BatchDate`
              
    when matched
        then update set
            `OrderID` = DBT_INTERNAL_SOURCE.`OrderID`, `CustomerID` = DBT_INTERNAL_SOURCE.`CustomerID`, `CustomerSK` = DBT_INTERNAL_SOURCE.`CustomerSK`, `ProductSK` = DBT_INTERNAL_SOURCE.`ProductSK`, `WarehouseSK` = DBT_INTERNAL_SOURCE.`WarehouseSK`, `RetailerSK` = DBT_INTERNAL_SOURCE.`RetailerSK`, `AgentSK` = DBT_INTERNAL_SOURCE.`AgentSK`, `PaymentModeSK` = DBT_INTERNAL_SOURCE.`PaymentModeSK`, `ProductCategorySK` = DBT_INTERNAL_SOURCE.`ProductCategorySK`, `OrderDateID` = DBT_INTERNAL_SOURCE.`OrderDateID`, `MFDDateID` = DBT_INTERNAL_SOURCE.`MFDDateID`, `EXPDateID` = DBT_INTERNAL_SOURCE.`EXPDateID`, `DeliveryDateID` = DBT_INTERNAL_SOURCE.`DeliveryDateID`, `ProductQuantity` = DBT_INTERNAL_SOURCE.`ProductQuantity`, `CostPrice` = DBT_INTERNAL_SOURCE.`CostPrice`, `sellingPrice` = DBT_INTERNAL_SOURCE.`sellingPrice`, `discount` = DBT_INTERNAL_SOURCE.`discount`, `finalprice` = DBT_INTERNAL_SOURCE.`finalprice`, `CGST` = DBT_INTERNAL_SOURCE.`CGST`, `SGST` = DBT_INTERNAL_SOURCE.`SGST`, `BatchDate` = DBT_INTERNAL_SOURCE.`BatchDate`, `LoadedAt` = DBT_INTERNAL_SOURCE.`LoadedAt`
    when not matched
        then insert
            (`OrderID`, `CustomerID`, `CustomerSK`, `ProductSK`, `WarehouseSK`, `RetailerSK`, `AgentSK`, `PaymentModeSK`, `ProductCategorySK`, `OrderDateID`, `MFDDateID`, `EXPDateID`, `DeliveryDateID`, `ProductQuantity`, `CostPrice`, `sellingPrice`, `discount`, `finalprice`, `CGST`, `SGST`, `BatchDate`, `LoadedAt`) VALUES (DBT_INTERNAL_SOURCE.`OrderID`, DBT_INTERNAL_SOURCE.`CustomerID`, DBT_INTERNAL_SOURCE.`CustomerSK`, DBT_INTERNAL_SOURCE.`ProductSK`, DBT_INTERNAL_SOURCE.`WarehouseSK`, DBT_INTERNAL_SOURCE.`RetailerSK`, DBT_INTERNAL_SOURCE.`AgentSK`, DBT_INTERNAL_SOURCE.`PaymentModeSK`, DBT_INTERNAL_SOURCE.`ProductCategorySK`, DBT_INTERNAL_SOURCE.`OrderDateID`, DBT_INTERNAL_SOURCE.`MFDDateID`, DBT_INTERNAL_SOURCE.`EXPDateID`, DBT_INTERNAL_SOURCE.`DeliveryDateID`, DBT_INTERNAL_SOURCE.`ProductQuantity`, DBT_INTERNAL_SOURCE.`CostPrice`, DBT_INTERNAL_SOURCE.`sellingPrice`, DBT_INTERNAL_SOURCE.`discount`, DBT_INTERNAL_SOURCE.`finalprice`, DBT_INTERNAL_SOURCE.`CGST`, DBT_INTERNAL_SOURCE.`SGST`, DBT_INTERNAL_SOURCE.`BatchDate`, DBT_INTERNAL_SOURCE.`LoadedAt`)
