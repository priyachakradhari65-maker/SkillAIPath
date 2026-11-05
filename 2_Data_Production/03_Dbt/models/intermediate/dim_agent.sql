-- dbt Intermediate Model: Agent Dimension
-- Creates sales agent dimension from sales transactions

{{
  config(
    materialized='table',
    tags=['intermediate', 'dimension', 'agent']
  )
}}

WITH unique_agents AS (
  SELECT DISTINCT
    AgentName,
    AgentCity,
    AgentContact,
    AgentMail
  FROM {{ ref('stg_sales') }}
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
