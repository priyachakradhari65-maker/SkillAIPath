-- dbt Mart Model: RFM Analysis - Customer Segmentation
-- Calculates Recency, Frequency, Monetary metrics and assigns customer segments
-- Uses dbt intermediate models



WITH rfm_metrics AS (
  SELECT
    fs.CustomerSK,
    DATEDIFF(CURRENT_DATE(), MAX(d.Date)) AS Recency,
    COUNT(*) AS Frequency,
    SUM(fs.finalprice) AS Monetary
  FROM `sap_dev`.`silver`.`fact_sales` fs
  INNER JOIN `sap_dev`.`silver`.`dim_date` d ON fs.OrderDateID = d.DateID
  WHERE fs.CustomerSK != -1
  GROUP BY fs.CustomerSK
),

rfm_scores AS (
  SELECT
    CustomerSK,
    Recency,
    Frequency,
    Monetary,
    NTILE(5) OVER (ORDER BY Recency DESC) AS R_Score,
    NTILE(5) OVER (ORDER BY Frequency ASC) AS F_Score,
    NTILE(5) OVER (ORDER BY Monetary ASC) AS M_Score
  FROM rfm_metrics
),

rfm_segments AS (
  SELECT
    CustomerSK,
    Recency,
    Frequency,
    Monetary / 100000 AS Monetary_Lakhs,
    R_Score,
    F_Score,
    M_Score,
    CONCAT(CAST(R_Score AS STRING), CAST(F_Score AS STRING), CAST(M_Score AS STRING)) AS RFM_Score,
    CASE
      WHEN R_Score >= 4 AND F_Score >= 4 AND M_Score >= 4 THEN 'Champions'
      WHEN R_Score >= 3 AND F_Score >= 3 AND M_Score >= 3 THEN 'Loyal Customers'
      WHEN R_Score >= 4 AND F_Score <= 2 AND M_Score <= 2 THEN 'Recent Customers'
      WHEN R_Score <= 2 AND F_Score >= 3 AND M_Score >= 3 THEN 'At Risk'
      WHEN R_Score <= 2 AND F_Score <= 2 AND M_Score >= 4 THEN 'Cant Lose Them'
      WHEN R_Score >= 3 AND F_Score <= 2 AND M_Score <= 2 THEN 'Promising'
      WHEN R_Score >= 3 AND F_Score >= 1 AND M_Score >= 1 THEN 'Potential Loyalist'
      WHEN R_Score >= 4 AND F_Score <= 1 THEN 'New Customers'
      WHEN R_Score >= 3 AND F_Score <= 1 THEN 'Promising'
      WHEN R_Score <= 2 AND F_Score >= 2 THEN 'Needs Attention'
      WHEN R_Score <= 2 AND F_Score >= 1 THEN 'About to Sleep'
      WHEN R_Score <= 2 AND F_Score <= 2 AND M_Score <= 2 THEN 'Hibernating'
      WHEN R_Score <= 1 AND F_Score <= 1 THEN 'Lost Customers'
      ELSE 'Others'
    END AS Segment
  FROM rfm_scores
)

SELECT
  r.CustomerSK,
  c.CustomerName,
  r.Recency,
  r.Frequency,
  r.Monetary_Lakhs,
  r.R_Score,
  r.F_Score,
  r.M_Score,
  r.RFM_Score,
  r.Segment
FROM rfm_segments r
LEFT JOIN (
  SELECT DISTINCT CustomerSK, CustomerName
  FROM `sap_dev`.`silver`.`dim_customer`
) c ON r.CustomerSK = c.CustomerSK