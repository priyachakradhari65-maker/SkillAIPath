
  
    
        create or replace table `sap_dev`.`silver`.`dim_customer`
      
      
    using delta
  
      
      
      
      
      
      
      
      
      as
      -- dbt Intermediate Model: Customer Dimension (SCD Type 2)
-- Creates customer dimension with historical tracking



WITH customer_base AS (
  SELECT DISTINCT
    CustomerID,
    CustomerName,
    CustomerMail,
    CustomerAge,
    CustomerGender,
    CustomerContact,
    CustomerAddress,
    OrderDate
  FROM `sap_dev`.`bronze`.`stg_sales`
),

customer_first_last AS (
  SELECT
    CustomerID,
    CustomerName,
    CustomerMail,
    CustomerAge,
    CustomerGender,
    CustomerContact,
    CustomerAddress,
    MIN(OrderDate) AS StartDate,
    MAX(OrderDate) AS EndDate
  FROM customer_base
  GROUP BY CustomerID, CustomerName, CustomerMail, CustomerAge, CustomerGender, CustomerContact, CustomerAddress
),

customer_dedup AS (
  SELECT
    CustomerID,
    CustomerName,
    CustomerMail,
    CustomerAge,
    CustomerGender,
    CustomerContact,
    CustomerAddress,
    StartDate,
    EndDate,
    ROW_NUMBER() OVER (PARTITION BY CustomerName ORDER BY EndDate DESC, CustomerID) AS rn
  FROM customer_first_last
)

SELECT
  ROW_NUMBER() OVER (ORDER BY CustomerName) AS CustomerSK,
  CAST(CustomerID AS INT) AS CustomerID,
  CustomerName,
  CustomerMail,
  CAST(CustomerAge AS INT) AS CustomerAge,
  CustomerGender,
  CustomerContact,
  CustomerAddress,
  StartDate,
  EndDate,
  1 AS IsCurrent,
  CURRENT_TIMESTAMP() AS LoadedAt
FROM customer_dedup
WHERE rn = 1
  