# Databricks notebook source
# DBTITLE 1,Dimension: Agent (SCD Type 1)
# MAGIC %md
# MAGIC # DimAgent: Sales Agent Master Data
# MAGIC **SkillAIPath Data Warehouse - Silver Layer**
# MAGIC
# MAGIC **Pattern:** SCD Type 1 | **Source:** stg_sales | **Target:** dim_agent

# COMMAND ----------

# DBTITLE 1,Load Config
# MAGIC %run ../../../1_Canvas/1_Setup/1_Config

# COMMAND ----------

# DBTITLE 1,Setup Parameters
from datetime import datetime
from pyspark.sql.functions import col, lit, when, count, sum, avg, max, min, first, last, concat, concat_ws, trim, upper, lower, to_date, to_timestamp, date_format, year, month, day, row_number, rank, dense_rank, lag, lead, explode, array, struct, map_from_entries, coalesce, isnull, isnotnull, regexp_replace, substring
from pyspark.sql.window import Window

# Widget: Load type
dbutils.widgets.dropdown("load_type", "full", ["full", "incremental"], "Load Type")
load_type = dbutils.widgets.get("load_type")

# Widget: Load since (for full load)
dbutils.widgets.text("load_since", "2023-06-13", "Load Since (Full Load Only)")
load_since = dbutils.widgets.get("load_since")

# Get batch date: try history table first, fallback to incremental table
try:
    latest_batch = spark.sql(f"SELECT MAX(BatchDate) FROM {Config.TBL_STG_SALES}").collect()[0][0]
    batch_date = str(latest_batch) if latest_batch else "2023-06-13"
except:
    try:
        latest_batch = spark.sql(f"SELECT MAX(BatchDate) FROM {Config.TBL_STG_SALES_INCREMENTAL}").collect()[0][0]
        batch_date = str(latest_batch) if latest_batch else "2023-06-13"
    except:
        batch_date = "2023-06-13"

# Determine source table for logging based on load type
source_table = Config.TBL_STG_SALES_INCREMENTAL if load_type == "incremental" else Config.TBL_STG_SALES

print("="*70)
print("DIM_AGENT - SCD TYPE 1")
print("="*70)
print(f"Environment: {Config.ENVIRONMENT.upper()}")
print(f"Catalog: {Config.CATALOG}")
print(f"Batch Date: {batch_date}")
print(f"Load Type: {load_type}")
print(f"Source: {source_table}")
print(f"Target: {Config.TBL_DIM_AGENT}")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Extract Source Data
# Extract distinct agents with full attributes

if load_type == "full":
    print(f"FULL LOAD: Processing all agents since {load_since}")
    df_source = spark.read.table(Config.TBL_STG_SALES) \
        .filter(col(Config.COL_BATCH_DATE) >= load_since) \
        .select("AgentName", "AgentCity", "AgentContact", "AgentMail") \
        .distinct()
else:
    print(f"INCREMENTAL LOAD: Processing agents from batch {batch_date}")
    df_source = spark.read.table(Config.TBL_STG_SALES_INCREMENTAL) \
        .select("AgentName", "AgentCity", "AgentContact", "AgentMail") \
        .distinct()
    print(f"  Source: {Config.TBL_STG_SALES_INCREMENTAL} (incremental staging)")

window_spec = Window.orderBy("AgentName")
df_source = df_source.withColumn("AgentID", row_number().over(window_spec))

agent_count = df_source.count()
print(f"Found {agent_count:,} unique agents with full attributes")
print(f"  Attributes: Name, City, Contact, Email")

if agent_count > 30:
    print(f"WARNING: Agent count unexpectedly high: {agent_count:,}")
    print("Expected: ~15 agents (SkillAIPath V1 architecture)")
else:
    print(f"Agent count validated: {agent_count} agents within expected range")

# COMMAND ----------

# DBTITLE 1,Create Table & Unknown Record
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {Config.TBL_DIM_AGENT} (
        AgentSK BIGINT,
        AgentID BIGINT,
        AgentName STRING,
        AgentCity STRING,
        AgentContact STRING,
        AgentMail STRING
    )
    USING DELTA
""")

try:
    spark.sql(f"""
        INSERT INTO {Config.TBL_DIM_AGENT}
        SELECT -1 AS AgentSK, -1 AS AgentID, 'Unknown Agent' AS AgentName,
               'Unknown' AS AgentCity, 'Unknown' AS AgentContact, 'unknown@unknown.com' AS AgentMail
        WHERE NOT EXISTS (SELECT 1 FROM {Config.TBL_DIM_AGENT} WHERE AgentSK = -1)
    """)
    print("SUCCESS: Unknown record inserted")
except:
    print("SUCCESS: Unknown record already exists")

print("SUCCESS: DimAgent table ready (with Unknown record)")
print("  Schema: AgentSK, AgentID, AgentName, AgentCity, AgentContact, AgentMail")

# COMMAND ----------

# DBTITLE 1,Generate Surrogate Keys & MERGE
if load_type == "full":
    print("FULL LOAD MODE: Truncating existing dimension")

    df_final = df_source \
        .withColumn("AgentSK", row_number().over(Window.orderBy("AgentName"))) \
        .select(
            "AgentSK",
            "AgentID",
            "AgentName",
            "AgentCity",
            "AgentContact",
            "AgentMail"
        )

    final_count = df_final.count()

    df_unknown = spark.sql("""
        SELECT -1 AS AgentSK, -1 AS AgentID, 'Unknown Agent' AS AgentName,
               'Unknown' AS AgentCity, 'Unknown' AS AgentContact, 'unknown@unknown.com' AS AgentMail
    """)

    df_with_unknown = df_unknown.unionByName(df_final)

    df_with_unknown.write \
        .mode("overwrite") \
        .format("delta") \
        .option("overwriteSchema", "true") \
        .saveAsTable(Config.TBL_DIM_AGENT)

    print(f"\nFULL LOAD completed")
    print(f"  Records loaded: {final_count:,}")
    print(f"  Total (with Unknown): {final_count + 1:,}")
    print(f"  Attributes per agent: 6 (SK, ID, Name, City, Contact, Mail)")

    new_count = final_count
    existing_count = 0

else:
    print("INCREMENTAL MODE: Using MERGE for upsert")

    max_sk = spark.sql(f"SELECT COALESCE(MAX(AgentSK), 0) as max_sk FROM {Config.TBL_DIM_AGENT}").collect()[0]['max_sk']
    df_existing = spark.read.table(Config.TBL_DIM_AGENT).filter(col("AgentSK") != -1).select("AgentName", "AgentSK")

    df_new = df_source.join(df_existing, ["AgentName"], "left_anti")
    new_count = df_new.count()

    if new_count > 0:
        df_new_with_sk = df_new \
            .withColumn("AgentSK", row_number().over(Window.orderBy("AgentName")) + max_sk) \
            .select(
                "AgentSK",
                "AgentID",
                "AgentName",
                "AgentCity",
                "AgentContact",
                "AgentMail"
            )
        print(f"Assigned AgentSK range: {max_sk + 1} to {max_sk + new_count}")
    else:
        df_new_with_sk = spark.createDataFrame(
            [],
            schema="AgentSK BIGINT, AgentID BIGINT, AgentName STRING, AgentCity STRING, AgentContact STRING, AgentMail STRING"
        )

    df_existing_agt = df_source.join(df_existing, ["AgentName"], "inner") \
        .select(
            col("AgentSK"),
            col("AgentID"),
            col("AgentName"),
            col("AgentCity"),
            col("AgentContact"),
            col("AgentMail")
        ).distinct()

    existing_count = df_existing_agt.count()

    df_merge_source = df_new_with_sk.unionByName(df_existing_agt)

    window_dedup = Window.partitionBy("AgentName").orderBy("AgentSK")
    df_merge_source = df_merge_source \
        .withColumn("row_num", row_number().over(window_dedup)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")

    print(f"MERGE source prepared:")
    print(f"  New agents (INSERT): {new_count:,}")
    print(f"  Existing agents (UPDATE): {existing_count:,}")

    df_merge_source.createOrReplaceTempView("agent_source")

    spark.sql(f"""
        MERGE INTO {Config.TBL_DIM_AGENT} AS tgt
        USING agent_source AS src
        ON tgt.AgentName = src.AgentName
        WHEN MATCHED THEN UPDATE SET
            tgt.AgentID = src.AgentID,
            tgt.AgentCity = src.AgentCity,
            tgt.AgentContact = src.AgentContact,
            tgt.AgentMail = src.AgentMail
        WHEN NOT MATCHED THEN INSERT (
            AgentSK, AgentID, AgentName, AgentCity, AgentContact, AgentMail
        ) VALUES (
            src.AgentSK, src.AgentID, src.AgentName, src.AgentCity, src.AgentContact, src.AgentMail
        )
    """)

    print("\nMERGE completed (SCD Type 1)")
    print(f"  Records inserted: {new_count:,}")
    print(f"  Records updated: {existing_count:,}")

print("\nPattern Summary:")
if load_type == "full":
    print("  FULL LOAD = Complete refresh (TRUNCATE + RELOAD)")
else:
    print("  INCREMENTAL = Delta updates (MERGE: UPDATE + INSERT)")
print("  Agent attributes: Name, City, Contact, Email")

# COMMAND ----------

# DBTITLE 1,Validation
# Validate dim_agent records with full attributes
validation_query = f"""
SELECT
    AgentSK,
    AgentID,
    AgentName,
    AgentCity,
    AgentContact,
    AgentMail
FROM {Config.TBL_DIM_AGENT}
WHERE AgentSK != -1
ORDER BY AgentSK
LIMIT 10
"""

df_validation = spark.sql(validation_query)
display(df_validation)

print(f"\nValidation: Showing first 10 agent records with full attributes from {Config.TBL_DIM_AGENT}")

# COMMAND ----------

# DBTITLE 1,Return Summary
final_count = spark.read.table(Config.TBL_DIM_AGENT).filter(col("AgentSK") != -1).count()

summary = {
    "status": "success",
    "environment": Config.ENVIRONMENT,
    "catalog": Config.CATALOG,
    "batch_date": batch_date,
    "load_since": load_since,
    "load_type": load_type,
    "dimension": "DimAgent",
    "scd_type": "Type 1",
    "new_agents": new_count,
    "updated_agents": existing_count,
    "total_agents": final_count,
    "table": Config.TBL_DIM_AGENT
}

print("\n" + "="*70)
print("DIM_AGENT (SCD-1) PROCESSING COMPLETE")
print("="*70)
for key, value in summary.items():
    print(f"  {key}: {value}")
print("="*70)

if final_count > 30:
    print(f"\nWARNING: Agent count ({final_count:,}) exceeds expected (~15)")

dbutils.notebook.exit(str(summary))