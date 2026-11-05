# Databricks notebook source
# DBTITLE 1,📋 Checkpoint Framework for Pipeline Orchestration
# MAGIC %md
# MAGIC # 📋 Checkpoint Framework - Smart Resume from Failures
# MAGIC
# MAGIC ## Purpose
# MAGIC Provides state tracking and resume capability for multi-stage pipeline orchestration.
# MAGIC
# MAGIC ## Features
# MAGIC - ✅ **Skip Completed Stages** - Only re-run failed or not-yet-executed stages
# MAGIC - ✅ **State Persistence** - Tracks completion status in Unity Catalog table
# MAGIC - ✅ **Run Isolation** - Each execution has unique run_id
# MAGIC - ✅ **Force Override** - Optional parameter to force re-run of completed stages
# MAGIC - ✅ **Detailed Logging** - Tracks start time, end time, duration, status per stage
# MAGIC
# MAGIC ## Integration
# MAGIC ```python
# MAGIC # Load checkpoint framework
# MAGIC %run ./checkpoint_framework
# MAGIC
# MAGIC # Initialize new run
# MAGIC run_id = initialize_checkpoint_run("Start_SkillAI")
# MAGIC
# MAGIC # Check before each stage
# MAGIC if should_run_stage(run_id, "DBT_FULL"):
# MAGIC     # Execute DBT
# MAGIC     mark_stage_complete(run_id, "DBT_FULL", "success")
# MAGIC else:
# MAGIC     print("⏭️  Skipping DBT_FULL - already completed")
# MAGIC ```
# MAGIC
# MAGIC ## Stages Tracked
# MAGIC 1. DBT_FULL
# MAGIC 2. STREAMING_CDC
# MAGIC 3. MLOPS
# MAGIC 4. APP_DEPLOY
# MAGIC 5. VALIDATION

# COMMAND ----------

# DBTITLE 1,⚙️ Configuration & State Table Setup
# Configuration
import time
from datetime import datetime
from pyspark.sql.functions import col, lit, current_timestamp, max as spark_max
import uuid

# State tracking table
STATE_CATALOG = "sap_dev"
STATE_SCHEMA = "bronze"
STATE_TABLE = f"{STATE_CATALOG}.{STATE_SCHEMA}.pipeline_checkpoints"

print(f"Checkpoint State Table: {STATE_TABLE}")

# ============================================================================
# CREATE STATE TABLE IF NOT EXISTS
# ============================================================================

def _ensure_state_table_exists():
    """Create checkpoint state table if it doesn't exist"""
    
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {STATE_TABLE} (
        run_id STRING NOT NULL,
        pipeline_name STRING NOT NULL,
        stage_name STRING NOT NULL,
        status STRING NOT NULL,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        duration_seconds DOUBLE,
        error_message STRING,
        metadata STRING,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.autoOptimize.optimizeWrite' = 'true'
    )
    COMMENT 'Checkpoint state tracking for pipeline orchestration'
    """
    
    spark.sql(create_sql)
    print(f"✅ State table ready: {STATE_TABLE}")

# Initialize on import
_ensure_state_table_exists()

# COMMAND ----------

# DBTITLE 1,🔄 Core Checkpoint Functions
# ============================================================================
# CHECKPOINT MANAGEMENT FUNCTIONS
# ============================================================================

def initialize_checkpoint_run(pipeline_name, force_rerun=False):
    """
    Initialize a new checkpoint run or resume existing one.
    
    Args:
        pipeline_name: Name of the pipeline (e.g., "Start_SkillAI")
        force_rerun: If True, creates new run_id even if incomplete run exists
    
    Returns:
        run_id: Unique identifier for this execution
    """
    
    if not force_rerun:
        # Check if there's an incomplete run from last 24 hours
        incomplete_run = spark.sql(f"""
            SELECT run_id, MAX(created_at) as last_activity
            FROM {STATE_TABLE}
            WHERE pipeline_name = '{pipeline_name}'
              AND created_at >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
            GROUP BY run_id
            HAVING SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) < 5  -- Not all 5 stages complete
            ORDER BY last_activity DESC
            LIMIT 1
        """).collect()
        
        if incomplete_run:
            run_id = incomplete_run[0]['run_id']
            print(f"🔄 Resuming incomplete run: {run_id}")
            print(f"   Last activity: {incomplete_run[0]['last_activity']}")
            return run_id
    
    # Create new run
    run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
    print(f"🆕 Starting new run: {run_id}")
    return run_id


def should_run_stage(run_id, stage_name, force_rerun=False):
    """
    Check if a stage should be executed.
    
    Args:
        run_id: Current run identifier
        stage_name: Name of the stage to check
        force_rerun: If True, always returns True
    
    Returns:
        Boolean: True if stage should run, False if already completed
    """
    
    if force_rerun:
        return True
    
    # Check if stage already completed in this run
    result = spark.sql(f"""
        SELECT status, end_time, duration_seconds
        FROM {STATE_TABLE}
        WHERE run_id = '{run_id}'
          AND stage_name = '{stage_name}'
          AND status = 'success'
        ORDER BY created_at DESC
        LIMIT 1
    """).collect()
    
    if result:
        completion_time = result[0]['end_time']
        duration = result[0]['duration_seconds']
        print(f"⏭️  Skipping {stage_name} - already completed at {completion_time}")
        print(f"   Duration: {duration:.1f}s ({duration/60:.1f} min)")
        return False
    
    return True


def mark_stage_started(run_id, pipeline_name, stage_name):
    """
    Mark a stage as started.
    
    Args:
        run_id: Current run identifier
        pipeline_name: Name of the pipeline
        stage_name: Name of the stage
    
    Returns:
        start_time: Timestamp when stage started
    """
    
    start_time = time.time()
    
    spark.sql(f"""
        INSERT INTO {STATE_TABLE}
        (run_id, pipeline_name, stage_name, status, start_time)
        VALUES (
            '{run_id}',
            '{pipeline_name}',
            '{stage_name}',
            'running',
            CURRENT_TIMESTAMP()
        )
    """)
    
    print(f"▶️  {stage_name} started at {datetime.now().strftime('%H:%M:%S')}")
    return start_time


def mark_stage_complete(run_id, stage_name, status='success', error_message=None, start_time=None):
    """
    Mark a stage as completed.
    
    Args:
        run_id: Current run identifier
        stage_name: Name of the stage
        status: 'success' or 'failed'
        error_message: Optional error message if failed
        start_time: Start time from mark_stage_started (for duration calc)
    """
    
    duration = time.time() - start_time if start_time else None
    error_msg_sql = f"'{error_message}'" if error_message else "NULL"
    duration_sql = f"{duration}" if duration else "NULL"
    
    spark.sql(f"""
        INSERT INTO {STATE_TABLE}
        (run_id, pipeline_name, stage_name, status, end_time, duration_seconds, error_message)
        VALUES (
            '{run_id}',
            'Start_SkillAI',
            '{stage_name}',
            '{status}',
            CURRENT_TIMESTAMP(),
            {duration_sql},
            {error_msg_sql}
        )
    """)
    
    if status == 'success':
        icon = "✅"
        msg = f"{stage_name} completed successfully"
    else:
        icon = "❌"
        msg = f"{stage_name} failed"
    
    if duration:
        msg += f" ({duration:.1f}s / {duration/60:.1f} min)"
    
    print(f"{icon} {msg}")


def get_run_status(run_id):
    """
    Get the current status of all stages in a run.
    
    Args:
        run_id: Run identifier to check
    
    Returns:
        DataFrame with stage status summary
    """
    
    return spark.sql(f"""
        SELECT 
            stage_name,
            status,
            start_time,
            end_time,
            duration_seconds,
            error_message
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY stage_name ORDER BY created_at DESC) as rn
            FROM {STATE_TABLE}
            WHERE run_id = '{run_id}'
        )
        WHERE rn = 1
        ORDER BY start_time
    """)

# COMMAND ----------

# DBTITLE 1,📊 Reporting Functions
# ============================================================================
# REPORTING & MONITORING
# ============================================================================

def print_run_summary(run_id):
    """
    Print a formatted summary of the current run status.
    
    Args:
        run_id: Run identifier to summarize
    """
    
    status_df = get_run_status(run_id)
    
    print("\n" + "="*80)
    print(f"CHECKPOINT RUN SUMMARY - {run_id}")
    print("="*80)
    
    for row in status_df.collect():
        stage = row['stage_name']
        status = row['status']
        duration = row['duration_seconds']
        
        if status == 'success':
            icon = "✅"
        elif status == 'failed':
            icon = "❌"
        else:
            icon = "⏳"
        
        line = f"{icon} {stage:20s} {status:10s}"
        
        if duration:
            line += f" {duration:6.1f}s ({duration/60:4.1f} min)"
        
        print(line)
        
        if row['error_message']:
            print(f"   Error: {row['error_message']}")
    
    print("="*80 + "\n")


def get_recent_runs(pipeline_name="Start_SkillAI", limit=10):
    """
    Get recent pipeline runs with completion status.
    
    Args:
        pipeline_name: Pipeline to filter on
        limit: Number of recent runs to return
    
    Returns:
        DataFrame with run summary
    """
    
    return spark.sql(f"""
        SELECT 
            run_id,
            MIN(start_time) as run_start,
            MAX(end_time) as run_end,
            COUNT(DISTINCT stage_name) as stages_attempted,
            SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as stages_succeeded,
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as stages_failed,
            MAX(CASE WHEN status = 'success' THEN created_at END) as last_success_time
        FROM {STATE_TABLE}
        WHERE pipeline_name = '{pipeline_name}'
        GROUP BY run_id
        ORDER BY run_start DESC
        LIMIT {limit}
    """)


def clear_run_checkpoints(run_id):
    """
    Clear all checkpoints for a specific run (use for manual reset).
    
    Args:
        run_id: Run identifier to clear
    """
    
    spark.sql(f"""
        DELETE FROM {STATE_TABLE}
        WHERE run_id = '{run_id}'
    """)
    
    print(f"🗑️  Cleared all checkpoints for run: {run_id}")


print("✅ Checkpoint framework loaded successfully")
print(f"   State table: {STATE_TABLE}")
print("   Available functions: initialize_checkpoint_run, should_run_stage, mark_stage_started, mark_stage_complete")

# COMMAND ----------

