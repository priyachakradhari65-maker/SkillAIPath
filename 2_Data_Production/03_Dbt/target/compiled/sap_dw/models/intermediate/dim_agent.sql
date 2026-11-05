-- dbt Intermediate Model: Agent Dimension
-- Creates sales agent dimension from sales transactions



WITH unique_agents AS (
  SELECT DISTINCT
    AgentName,
    AgentCity,
    AgentContact,
    AgentMail
  FROM `sap_dev`.`staging`.`vw_sales`
  WHERE AgentName IS NOT NULL
)

SELECT
  ROW_NUMBER() OVER (ORDER BY AgentName) AS AgentSK,
  ROW_NUMBER() OVER (ORDER BY AgentName) AS AgentID,
  AgentName,
  AgentCity,
  AgentContact,
  AgentMail
FROM unique_agents