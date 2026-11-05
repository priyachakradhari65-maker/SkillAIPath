# Databricks notebook source
# DBTITLE 1,Agent-Territory Bridge Table
# MAGIC %md
# MAGIC # Agent-Territory Assignment Bridge - Sales Coverage Tracking
# MAGIC
# MAGIC ## Business Context
# MAGIC
# MAGIC SkillAIPath employs 200+ field sales agents across India. Territory assignments are complex:
# MAGIC
# MAGIC - **One agent covers multiple territories** (e.g., Agent Raj covers Mumbai Central + Mumbai East)
# MAGIC - **One territory may have multiple agents** (e.g., Delhi NCR has 5 agents for full coverage)
# MAGIC - **Assignments change frequently** (promotions, resignations, territory splits)
# MAGIC
# MAGIC **Business Questions:**
# MAGIC - Which agents are responsible for which territories?
# MAGIC - Which territories have coverage gaps (no assigned agents)?
# MAGIC - What is the sales performance per agent-territory pair?
# MAGIC - How does territory workload distribution compare?
# MAGIC
# MAGIC **Pattern:** Bridge Table (Many-to-Many with Temporal Tracking)
# MAGIC - Tracks assignment history (StartDate, EndDate, IsCurrent)
# MAGIC - Enables territory realignment analysis
# MAGIC - Foundation for agent performance evaluation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Schema Design
# MAGIC
# MAGIC **Bridge Table: bridge_agent_territory**
# MAGIC ```
# MAGIC AgentSK (FK -> dim_agent)
# MAGIC TerritorySK (FK -> dim_territory)
# MAGIC AssignmentStartDate (DATE)
# MAGIC AssignmentEndDate (DATE)
# MAGIC IsCurrent (BOOLEAN)
# MAGIC AssignmentType (Primary, Secondary, Backup)
# MAGIC WorkloadPercent (DECIMAL) - % of agent time allocated
# MAGIC CreatedDate (TIMESTAMP)
# MAGIC ```
# MAGIC
# MAGIC **Grain:** One row per agent-territory-assignment period
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Data Pattern
# MAGIC
# MAGIC Example:
# MAGIC - 200 sales agents
# MAGIC - 150 territories (states, cities, zones)
# MAGIC - **Average:** Each agent covers 2-3 territories
# MAGIC - **Total assignments:** ~500 agent-territory pairs
# MAGIC
# MAGIC **Temporal Tracking:**
# MAGIC - Agent A covers Territory 1 from Jan 2024 - Mar 2024 (IsCurrent=FALSE)
# MAGIC - Agent A covers Territory 2 from Apr 2024 - Present (IsCurrent=TRUE)

# COMMAND ----------

# DBTITLE 1,Configuration
# Configuration - Load from Config module
%run ../../../1_Canvas/1_Setup/1_config

# Table names
bridge_table = f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.bridge_agent_territory"
dim_agent = f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_agent"
dim_territory = f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_territory"
fact_sales = Config.TBL_FACT_SALES

print(f"Target Bridge Table: {bridge_table}")
print(f"Dimensions: {dim_agent}, {dim_territory}")

# COMMAND ----------

# DBTITLE 1,Create Dimension: Sales Agents
#Create sample sales agent dimension
query = f"""
CREATE OR REPLACE TABLE {dim_agent} (
  AgentSK INT,
  AgentID STRING,
  AgentName STRING,
  Email STRING,
  Phone STRING,
  HireDate DATE,
  AgentLevel STRING,
  ManagerID STRING,
  CreatedDate TIMESTAMP
)
"""
spark.sql(query)

query = f"""
INSERT INTO {dim_agent} VALUES
  (1, 'AGT001', 'Rajesh Kumar', 'rajesh@sap.com', '9876543210', '2020-01-15', 'Senior', 'MGR001', current_timestamp()),
  (2, 'AGT002', 'Priya Sharma', 'priya@sap.com', '9876543211', '2020-06-10', 'Senior', 'MGR001', current_timestamp()),
  (3, 'AGT003', 'Amit Patel', 'amit@sap.com', '9876543212', '2021-01-20', 'Mid-Level', 'MGR002', current_timestamp()),
  (4, 'AGT004', 'Sneha Singh', 'sneha@sap.com', '9876543213', '2021-08-15', 'Mid-Level', 'MGR002', current_timestamp()),
  (5, 'AGT005', 'Vikram Mehta', 'vikram@sap.com', '9876543214', '2022-03-01', 'Junior', 'MGR003', current_timestamp()),
  (6, 'AGT006', 'Ananya Reddy', 'ananya@sap.com', '9876543215', '2022-07-10', 'Junior', 'MGR003', current_timestamp())
"""
spark.sql(query)

query = f"""
SELECT
  AgentLevel,
  COUNT(*) as AgentCount
FROM {dim_agent}
GROUP BY AgentLevel
"""
display(spark.sql(query))

# COMMAND ----------

# DBTITLE 1,Create Dimension: Territories
#Create sample territory dimension
query = f"""
CREATE OR REPLACE TABLE {dim_territory} (
  TerritorySK INT,
  TerritoryID STRING,
  TerritoryName STRING,
  City STRING,
  State STRING,
  Zone STRING,
  TerritoryType STRING,
  PopulationSize STRING,
  CreatedDate TIMESTAMP
)
"""
spark.sql(query)

query = f"""
INSERT INTO {dim_territory} VALUES
  (1, 'TERR_MUM_CENTRAL', 'Mumbai Central Zone', 'Mumbai', 'Maharashtra', 'West', 'Urban', 'High', current_timestamp()),
  (2, 'TERR_MUM_EAST', 'Mumbai East Zone', 'Mumbai', 'Maharashtra', 'West', 'Urban', 'High', current_timestamp()),
  (3, 'TERR_DEL_NCR', 'Delhi NCR Zone', 'Delhi', 'Delhi', 'North', 'Metro', 'High', current_timestamp()),
  (4, 'TERR_BLR_NORTH', 'Bangalore North Zone', 'Bangalore', 'Karnataka', 'South', 'Urban', 'High', current_timestamp()),
  (5, 'TERR_BLR_SOUTH', 'Bangalore South Zone', 'Bangalore', 'Karnataka', 'South', 'Urban', 'Medium', current_timestamp()),
  (6, 'TERR_PUN_CITY', 'Pune City Zone', 'Pune', 'Maharashtra', 'West', 'Tier2', 'Medium', current_timestamp()),
  (7, 'TERR_JAI_CITY', 'Jaipur City Zone', 'Jaipur', 'Rajasthan', 'North', 'Tier2', 'Medium', current_timestamp()),
  (8, 'TERR_LKO_CITY', 'Lucknow City Zone', 'Lucknow', 'Uttar Pradesh', 'North', 'Tier2', 'Medium', current_timestamp())
"""
spark.sql(query)

query = f"""
SELECT
  Zone,
  COUNT(*) as TerritoryCount
FROM {dim_territory}
GROUP BY Zone
"""
display(spark.sql(query))

# COMMAND ----------

# DBTITLE 1,Create Bridge Table - Assignment Logic
#Create bridge with agent-territory assignments
query = f"""
CREATE OR REPLACE TABLE {bridge_table} (
  AgentSK INT,
  TerritorySK INT,
  AssignmentStartDate DATE,
  AssignmentEndDate DATE,
  IsCurrent BOOLEAN,
  AssignmentType STRING,
  WorkloadPercent DECIMAL(5,2),
  CreatedDate TIMESTAMP
)
"""
spark.sql(query)

query = f"""
INSERT INTO {bridge_table} VALUES
-- Rajesh Kumar (Senior) - Mumbai Central (Primary) + Mumbai East (Secondary)
  (1, 1, '2024-01-01', NULL, TRUE, 'Primary', 60.00, current_timestamp()),
  (1, 2, '2024-01-01', NULL, TRUE, 'Secondary', 40.00, current_timestamp()),

  -- Priya Sharma (Senior) - Delhi NCR (Primary) + Bangalore North (Backup)
  (2, 3, '2024-01-01', NULL, TRUE, 'Primary', 80.00, current_timestamp()),
  (2, 4, '2024-01-01', NULL, TRUE, 'Backup', 20.00, current_timestamp()),

  -- Amit Patel (Mid-Level) - Bangalore North (Primary) + Bangalore South (Secondary)
  (3, 4, '2024-01-01', NULL, TRUE, 'Primary', 50.00, current_timestamp()),
  (3, 5, '2024-01-01', NULL, TRUE, 'Secondary', 50.00, current_timestamp()),

  -- Sneha Singh (Mid-Level) - Pune City (Primary) + Jaipur City (Secondary)
  (4, 6, '2024-01-01', NULL, TRUE, 'Primary', 60.00, current_timestamp()),
  (4, 7, '2024-01-01', NULL, TRUE, 'Secondary', 40.00, current_timestamp()),

  -- Vikram Mehta (Junior) - Lucknow City (Primary only)
  (5, 8, '2024-03-01', NULL, TRUE, 'Primary', 100.00, current_timestamp()),

  -- Ananya Reddy (Junior) - Jaipur City (Backup)
  (6, 7, '2024-07-10', NULL, TRUE, 'Backup', 100.00, current_timestamp()),
  (1, 4, '2023-01-01', '2023-12-31', FALSE, 'Primary', 100.00, current_timestamp())
"""
spark.sql(query)

# COMMAND ----------

# DBTITLE 1,Bridge Analysis - Coverage Overview
# Agent-territory coverage analysis
query = f"""
SELECT
  COUNT(*) as TotalAssignments,
  COUNT(DISTINCT AgentSK) as UniqueAgents,
  COUNT(DISTINCT TerritorySK) as UniqueTerritories,
  SUM(CASE WHEN IsCurrent THEN 1 ELSE 0 END) as CurrentAssignments,
  AVG(WorkloadPercent) as AvgWorkloadPercent
FROM {bridge_table}
"""
display(spark.sql(query))

# COMMAND ----------

# DBTITLE 1,Analysis 1: Agent Workload Distribution
# How many territories is each agent covering?
query = f"""
SELECT
  a.AgentID,
  a.AgentName,
  a.AgentLevel,
  COUNT(DISTINCT b.TerritorySK) as TerritoriesAssigned,
  SUM(b.WorkloadPercent) as TotalWorkloadPercent,
  STRING_AGG(CONCAT(t.TerritoryName, ' (', b.AssignmentType, ', ', b.WorkloadPercent, '%)'), ' | ') as TerritoryDetails
FROM {dim_agent} a
JOIN {bridge_table} b ON a.AgentSK = b.AgentSK
JOIN {dim_territory} t ON b.TerritorySK = t.TerritorySK
WHERE b.IsCurrent = TRUE
GROUP BY a.AgentID, a.AgentName, a.AgentLevel
ORDER BY TotalWorkloadPercent DESC
"""
display(spark.sql(query))

# COMMAND ----------

# DBTITLE 1,Analysis 2: Territory Coverage Gaps
# Which territories have no assigned agents?
query = f"""
SELECT
  t.TerritoryID,
  t.TerritoryName,
  t.City,
  t.Zone,
  t.PopulationSize,
  COUNT(DISTINCT b.AgentSK) as AssignedAgents,
  CASE
    WHEN COUNT(DISTINCT b.AgentSK) = 0 THEN 'NO COVERAGE'
    WHEN COUNT(DISTINCT b.AgentSK) = 1 THEN 'SINGLE AGENT'
    ELSE 'MULTI AGENT'
  END as CoverageStatus
FROM {dim_territory} t
LEFT JOIN {bridge_table} b
  ON t.TerritorySK = b.TerritorySK AND b.IsCurrent = TRUE
GROUP BY t.TerritoryID, t.TerritoryName, t.City, t.Zone, t.PopulationSize
ORDER BY AssignedAgents ASC, t.PopulationSize DESC
"""
display(spark.sql(query))

# COMMAND ----------

# DBTITLE 1,Analysis 3: Assignment Type Distribution
# What is the distribution of Primary vs Secondary vs Backup assignments?
query = f"""
SELECT
  b.AssignmentType,
  COUNT(*) as AssignmentCount,
  AVG(b.WorkloadPercent) as AvgWorkloadPercent,
  COUNT(DISTINCT b.AgentSK) as UniqueAgents,
  COUNT(DISTINCT b.TerritorySK) as UniqueTerritories
FROM {bridge_table} b
WHERE b.IsCurrent = TRUE
GROUP BY b.AssignmentType
ORDER BY AssignmentCount DESC
"""
display(spark.sql(query))

# COMMAND ----------

# DBTITLE 1,Analysis 4: Sales Performance by Agent-Territory
# Agent-territory assignment summary with sales metrics
query = f"""
SELECT
  a.AgentName,
  t.TerritoryName,
  b.AssignmentType,
  b.WorkloadPercent,
  COUNT(DISTINCT f.OrderID) as TotalTransactions,
  SUM(f.finalprice) as TotalRevenue,
  ROUND(SUM(f.finalprice) / b.WorkloadPercent * 100, 2) as RevenuePerWorkloadPercent
FROM {bridge_table} b
JOIN {dim_agent} a ON b.AgentSK = a.AgentSK
JOIN {dim_territory} t ON b.TerritorySK = t.TerritorySK
LEFT JOIN {fact_sales} f
  ON b.AgentSK = f.AgentSK
WHERE b.IsCurrent = TRUE
GROUP BY a.AgentName, t.TerritoryName, b.AssignmentType, b.WorkloadPercent
ORDER BY TotalRevenue DESC
"""
display(spark.sql(query))

# COMMAND ----------

# DBTITLE 1,Analysis 5: Temporal Analysis - Assignment Changes
# Track agent reassignments over time
query = f"""
SELECT
  a.AgentName,
  t.TerritoryName,
  b.AssignmentStartDate,
  b.AssignmentEndDate,
  b.IsCurrent,
  DATEDIFF(
    COALESCE(b.AssignmentEndDate, CURRENT_DATE()),
    b.AssignmentStartDate
  ) as DaysInTerritory,
  CASE
    WHEN b.AssignmentEndDate IS NULL THEN 'Active'
    ELSE 'Ended'
  END as AssignmentStatus
FROM {bridge_table} b
JOIN {dim_agent} a ON b.AgentSK = a.AgentSK
JOIN {dim_territory} t ON b.TerritorySK = t.TerritorySK
ORDER BY a.AgentName, b.AssignmentStartDate DESC
"""
display(spark.sql(query))

# COMMAND ----------

# DBTITLE 1,Key Insights & Business Value
# MAGIC %md
# MAGIC ## Key Insights from Bridge Table
# MAGIC
# MAGIC **1. Territory Coverage Management:**
# MAGIC - Identify coverage gaps (territories with no agents)
# MAGIC - Balance workload (agents handling 100%+ assignments)
# MAGIC - Plan backup coverage for high-priority zones
# MAGIC
# MAGIC **2. Agent Performance Evaluation:**
# MAGIC - Compare sales per agent-territory pair
# MAGIC - Normalize by workload percent (fair comparison)
# MAGIC - Identify underperforming territories for targeted support
# MAGIC
# MAGIC **3. Assignment Optimization:**
# MAGIC - Senior agents: 2-3 territories (Primary + Secondary)
# MAGIC - Mid-level agents: 2 territories (balanced 50/50)
# MAGIC - Junior agents: 1 territory (100% focus)
# MAGIC
# MAGIC **4. Temporal Workforce Planning:**
# MAGIC - Track agent tenure in territories (DaysInTerritory)
# MAGIC - Analyze reassignment patterns (frequent moves = instability)
# MAGIC - Plan succession (Backup agents ready to become Primary)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Many-to-Many Pattern
# MAGIC
# MAGIC **Complexity:**
# MAGIC - One agent covers multiple territories (1:N)
# MAGIC - One territory covered by multiple agents (N:1)
# MAGIC - Combined: Many-to-Many (M:N)
# MAGIC
# MAGIC **Why Bridge Table?**
# MAGIC - Cannot store in dim_agent (which territory?)
# MAGIC - Cannot store in dim_territory (which agent?)
# MAGIC - Bridge resolves the relationship
# MAGIC
# MAGIC **With Temporal Tracking:**
# MAGIC - IsCurrent flag (active vs historical)
# MAGIC - StartDate/EndDate (assignment duration)
# MAGIC - Enables point-in-time queries ("Who covered Territory X on March 15?")
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Production Considerations
# MAGIC
# MAGIC **Data Volume:**
# MAGIC - 200 agents x 2.5 territories avg = 500 current assignments
# MAGIC - Historical assignments: ~2,000 rows (4x turnover)
# MAGIC - Storage: ~1MB (lightweight)
# MAGIC
# MAGIC **Update Frequency:**
# MAGIC - Daily: Sales performance metrics (join to fact_sales)
# MAGIC - Ad-hoc: Assignment changes (resignations, promotions, territory splits)
# MAGIC - Monthly: Workload rebalancing (adjust WorkloadPercent)
# MAGIC
# MAGIC **Business Rules:**
# MAGIC - WorkloadPercent per agent must sum to <= 100% (no overallocation)
# MAGIC - Primary assignments must have >= 50% workload
# MAGIC - Each territory must have at least 1 Primary assignment
# MAGIC
# MAGIC **Validation Queries:**
# MAGIC ```sql
# MAGIC -- Check for overallocated agents
# MAGIC SELECT AgentSK, SUM(WorkloadPercent) as TotalWorkload
# MAGIC FROM bridge_agent_territory
# MAGIC WHERE IsCurrent = TRUE
# MAGIC GROUP BY AgentSK
# MAGIC HAVING SUM(WorkloadPercent) > 100;
# MAGIC
# MAGIC -- Check for territories with no Primary agent
# MAGIC SELECT t.TerritoryName
# MAGIC FROM dim_territory t
# MAGIC LEFT JOIN bridge_agent_territory b
# MAGIC   ON t.TerritorySK = b.TerritorySK
# MAGIC   AND b.IsCurrent = TRUE
# MAGIC   AND b.AssignmentType = 'Primary'
# MAGIC WHERE b.AgentSK IS NULL;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Comparison: All Three Bridge Patterns
# MAGIC
# MAGIC **1. Customer-Promotion Bridge (Factless Fact):**
# MAGIC - No measures (pure occurrence tracking)
# MAGIC - Use case: Exposure analysis
# MAGIC
# MAGIC **2. Product-Store Bridge (Bridge + Snapshot Fact):**
# MAGIC - Has measures (StockQuantity, ReorderLevel)
# MAGIC - Use case: Inventory tracking
# MAGIC
# MAGIC **3. Agent-Territory Bridge (Bridge + Temporal SCD):**
# MAGIC - Has temporal tracking (StartDate, EndDate, IsCurrent)
# MAGIC - Use case: Workforce assignment history
# MAGIC
# MAGIC **All three solve many-to-many relationships but with different nuances.**