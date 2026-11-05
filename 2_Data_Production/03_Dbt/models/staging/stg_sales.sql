-- dbt Staging Model: Sales Transactions
-- Cleanses and standardizes bronze sales data
-- Single ownership: dbt owns all transformations from bronze onwards

{{
  config(
    materialized='view',
    alias='vw_sales',
    tags=['staging', 'sales']
  )
}}

SELECT
  OrderID,
  CustomerID, -- added PhaseC2: business key for the CDC-owned dim_customer ID join
  CustomerName,
  ProductName,
  ProductCategory,
  WareHouseName,
  RetailerName,
  RetailerCity,
  RetailerContact,
  RetailerMail,
  AgentName,
  AgentCity,
  AgentContact,
  AgentMail,
  PaymentMode,
  CAST(OrderDate AS DATE) AS OrderDate,
  CAST(MFDDate AS DATE) AS MFDDate,
  CAST(EXPDate AS DATE) AS EXPDate,
  CAST(DeliveryDate AS DATE) AS DeliveryDate,
  ProductQuantity,
  ROUND(CostPrice, 2) AS CostPrice,
  ROUND(sellingPrice, 2) AS SellingPrice,
  ROUND(discount, 2) AS Discount,
  ROUND(finalprice, 2) AS FinalPrice,
  ROUND(CGST, 2) AS CGST,
  ROUND(SGST, 2) AS SGST,
  ROUND(finalprice + CGST + SGST, 2) AS TotalAmount,
  CAST(BatchDate AS DATE) AS BatchDate,
  CAST(LoadedAt AS TIMESTAMP) AS LoadedAt
FROM {{ source('bronze', 'sales') }}
WHERE finalprice > 0  -- Data quality: Remove invalid transactions
  AND ProductQuantity > 0
