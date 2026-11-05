-- back compat for old kwarg name
  
  
  
  
  
  
      
          
              
              
          
              
              
          
      
  

    merge
    into
        `sap_dev`.`silver`.`int_sales_enriched` as DBT_INTERNAL_DEST
    using
        `int_sales_enriched__dbt_tmp` as DBT_INTERNAL_SOURCE
    on
        
                  DBT_INTERNAL_SOURCE.`OrderID` <=> DBT_INTERNAL_DEST.`OrderID`
              
    and 
                  DBT_INTERNAL_SOURCE.`BatchDate` <=> DBT_INTERNAL_DEST.`BatchDate`
              
    when matched
        then update set
            `OrderID` = DBT_INTERNAL_SOURCE.`OrderID`, `BatchDate` = DBT_INTERNAL_SOURCE.`BatchDate`, `CustomerName` = DBT_INTERNAL_SOURCE.`CustomerName`, `customer_segment` = DBT_INTERNAL_SOURCE.`customer_segment`, `activity_status` = DBT_INTERNAL_SOURCE.`activity_status`, `customer_lifetime_value` = DBT_INTERNAL_SOURCE.`customer_lifetime_value`, `ProductName` = DBT_INTERNAL_SOURCE.`ProductName`, `product_avg_margin` = DBT_INTERNAL_SOURCE.`product_avg_margin`, `product_popularity` = DBT_INTERNAL_SOURCE.`product_popularity`, `avg_stock_level` = DBT_INTERNAL_SOURCE.`avg_stock_level`, `WareHouseName` = DBT_INTERNAL_SOURCE.`WareHouseName`, `RetailerName` = DBT_INTERNAL_SOURCE.`RetailerName`, `OrderDate` = DBT_INTERNAL_SOURCE.`OrderDate`, `ProductQuantity` = DBT_INTERNAL_SOURCE.`ProductQuantity`, `SellingPrice` = DBT_INTERNAL_SOURCE.`SellingPrice`, `CostPrice` = DBT_INTERNAL_SOURCE.`CostPrice`, `FinalPrice` = DBT_INTERNAL_SOURCE.`FinalPrice`, `TotalAmount` = DBT_INTERNAL_SOURCE.`TotalAmount`, `transaction_margin` = DBT_INTERNAL_SOURCE.`transaction_margin`, `margin_pct` = DBT_INTERNAL_SOURCE.`margin_pct`, `LoadedAt` = DBT_INTERNAL_SOURCE.`LoadedAt`
    when not matched
        then insert
            (`OrderID`, `BatchDate`, `CustomerName`, `customer_segment`, `activity_status`, `customer_lifetime_value`, `ProductName`, `product_avg_margin`, `product_popularity`, `avg_stock_level`, `WareHouseName`, `RetailerName`, `OrderDate`, `ProductQuantity`, `SellingPrice`, `CostPrice`, `FinalPrice`, `TotalAmount`, `transaction_margin`, `margin_pct`, `LoadedAt`) VALUES (DBT_INTERNAL_SOURCE.`OrderID`, DBT_INTERNAL_SOURCE.`BatchDate`, DBT_INTERNAL_SOURCE.`CustomerName`, DBT_INTERNAL_SOURCE.`customer_segment`, DBT_INTERNAL_SOURCE.`activity_status`, DBT_INTERNAL_SOURCE.`customer_lifetime_value`, DBT_INTERNAL_SOURCE.`ProductName`, DBT_INTERNAL_SOURCE.`product_avg_margin`, DBT_INTERNAL_SOURCE.`product_popularity`, DBT_INTERNAL_SOURCE.`avg_stock_level`, DBT_INTERNAL_SOURCE.`WareHouseName`, DBT_INTERNAL_SOURCE.`RetailerName`, DBT_INTERNAL_SOURCE.`OrderDate`, DBT_INTERNAL_SOURCE.`ProductQuantity`, DBT_INTERNAL_SOURCE.`SellingPrice`, DBT_INTERNAL_SOURCE.`CostPrice`, DBT_INTERNAL_SOURCE.`FinalPrice`, DBT_INTERNAL_SOURCE.`TotalAmount`, DBT_INTERNAL_SOURCE.`transaction_margin`, DBT_INTERNAL_SOURCE.`margin_pct`, DBT_INTERNAL_SOURCE.`LoadedAt`)
