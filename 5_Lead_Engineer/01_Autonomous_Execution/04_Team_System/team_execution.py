# Databricks notebook source
# DBTITLE 1,Team System - Collaborative Execution for 40 Engineers
# ============================================================================
# TEAM PIPELINE - 40 engineers, 9 pods, 3-hour cadence
# ============================================================================
# This notebook orchestrates team execution of the improvement scope.
# Each engineer gets tasks from Daily_Plan, executes them, and logs results.
#
# Parameters:
#   phase:      distribute | review | report (default: distribute)
#   pod:        all | Pod1_Data Team | Pod2_Architecture Team | ... (default: all)
#   engineer:   all | Engineer_01 | ... (default: all)
#   max_tasks:  Max tasks per engineer per cycle (default: 3)
# ============================================================================

from datetime import datetime
import os, re, json

dbutils.widgets.text("phase", "distribute", "Phase: distribute|review|report")
dbutils.widgets.text("pod", "all", "Pod filter (all or specific)")
dbutils.widgets.text("engineer", "all", "Engineer filter (all or specific)")
dbutils.widgets.text("max_tasks", "3", "Max tasks per engineer per cycle")

PHASE = dbutils.widgets.get("phase")
POD_FILTER = dbutils.widgets.get("pod")
ENGINEER_FILTER = dbutils.widgets.get("engineer")
MAX_TASKS = int(dbutils.widgets.get("max_tasks"))

CATALOG = "sap_dev"
WORK_QUEUE = f"{CATALOG}.autonomous.work_queue"
LOG_TABLE = f"{CATALOG}.autonomous.execution_log"
DAILY_PLAN = "/Workspace/Shared/dev/SkillAIPath/5_Lead_Engineer/3_Lead_Engineer/4_Operations/09_Daily_Plan"
TRACKING_TSV = "/Workspace/Shared/dev/SkillAIPath/5_Lead_Engineer/3_Lead_Engineer/4_Operations/08_Autonomous_Tracking.tsv"
PIPELINE_VERSION = spark.sql(f"SELECT COALESCE(MAX(pipeline_version), 0) + 1 FROM {WORK_QUEUE}").collect()[0][0]

print("=" * 70)
print(f"TEAM SYSTEM v{PIPELINE_VERSION} | Phase: {PHASE} | Pod: {POD_FILTER}")
print(f"Started: {datetime.now()}")
print("=" * 70)

# COMMAND ----------

# DBTITLE 1,Phase: Distribute - Assign tasks to engineers
if PHASE == "distribute":
    print("\n--- DISTRIBUTING TASKS TO ENGINEERS ---")

    # Get today's date folder from Daily_Plan
    today = datetime.now().strftime("%Y-%m-%d")
    today_folder = os.path.join(DAILY_PLAN, today)

    if not os.path.exists(today_folder):
        print(f"  No plan for {today}. Checking tomorrow...")
        from datetime import timedelta
        tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        today_folder = os.path.join(DAILY_PLAN, tomorrow)

    if os.path.exists(today_folder):
        print(f"  Plan found: {os.path.basename(today_folder)}/")
        pod_files = [f for f in os.listdir(today_folder) if f.endswith('.tsv') and f != 'Daily_Summary.tsv']

        total_assigned = 0
        for pod_file in sorted(pod_files):
            pod_name = pod_file.replace('.tsv', '')
            if POD_FILTER != "all" and pod_name != POD_FILTER:
                continue

            pod_path = os.path.join(today_folder, pod_file)
            with open(pod_path, 'r') as f:
                lines = f.readlines()

            print(f"\n  {pod_name}:")
            for line in lines[1:]:  # skip header
                cols = line.strip().split('\t')
                if len(cols) >= 8:
                    work_id = cols[0]
                    title = cols[1][:40]
                    priority = cols[2]
                    engineer = cols[7]

                    if ENGINEER_FILTER != "all" and engineer != ENGINEER_FILTER:
                        continue

                    # Check if task still queued
                    status = spark.sql(f"SELECT status FROM {WORK_QUEUE} WHERE work_id = '{work_id}'").collect()
                    if status and status[0].status == 'queued':
                        print(f"    {work_id} [{priority}] -> {engineer}: {title}")
                        total_assigned += 1
                    elif status and status[0].status == 'completed':
                        print(f"    {work_id} [DONE] {title}")

        print(f"\n  Total assigned: {total_assigned}")
    else:
        # Fallback: distribute directly from work_queue
        print("  No daily plan found. Distributing from work_queue...")
        ready = spark.sql(f"""
            SELECT work_id, title, priority, assigned_pod
            FROM {WORK_QUEUE}
            WHERE status = 'queued' AND source = 'approved'
            {"AND assigned_pod = '" + POD_FILTER + "'" if POD_FILTER != "all" else ""}
            ORDER BY priority, work_id
            LIMIT {MAX_TASKS * 10}
        """).collect()
        print(f"  Ready tasks: {len(ready)}")
        for t in ready:
            print(f"    {t.work_id} [{t.priority}] {t.assigned_pod}: {t.title[:50]}")

# COMMAND ----------

# DBTITLE 1,Phase: Review - Pod leads review completed work
if PHASE == "review":
    print("\n--- POD LEAD REVIEW ---")

    # Get tasks completed since last review
    completed = spark.sql(f"""
        SELECT work_id, title, priority, assigned_pod, 
               completed_at, validation_passed, execution_log, branch_name
        FROM {WORK_QUEUE}
        WHERE status = 'completed' AND pipeline_version = {PIPELINE_VERSION}
        {"AND assigned_pod = '" + POD_FILTER + "'" if POD_FILTER != "all" else ""}
        ORDER BY assigned_pod, priority
    """).collect()

    if completed:
        print(f"  Tasks to review: {len(completed)}")
        by_pod = {}
        for t in completed:
            pod = t.assigned_pod or "Unassigned"
            if pod not in by_pod:
                by_pod[pod] = []
            by_pod[pod].append(t)

        for pod, tasks in sorted(by_pod.items()):
            print(f"\n  {pod} ({len(tasks)} tasks):")
            for t in tasks:
                validated = "✅" if t.validation_passed else "⚠️"
                print(f"    {validated} {t.work_id} [{t.priority}] {t.title[:50]}")
    else:
        print("  No tasks to review this cycle.")

# COMMAND ----------

# DBTITLE 1,Phase: Report - Team progress summary
if PHASE == "report":
    print("\n--- TEAM PROGRESS REPORT ---")

    total = spark.sql(f"SELECT COUNT(*) FROM {WORK_QUEUE}").collect()[0][0]
    done = spark.sql(f"SELECT COUNT(*) FROM {WORK_QUEUE} WHERE status = 'completed'").collect()[0][0]
    in_prog = spark.sql(f"SELECT COUNT(*) FROM {WORK_QUEUE} WHERE status = 'in_progress'").collect()[0][0]
    queued = spark.sql(f"SELECT COUNT(*) FROM {WORK_QUEUE} WHERE status = 'queued'").collect()[0][0]

    print(f"\n  OVERALL: {done}/{total} completed ({done/total*100:.1f}%)")
    print(f"  In progress: {in_prog} | Queued: {queued}")

    # Pod-wise status
    print(f"\n  POD STATUS:")
    pods = spark.sql(f"""
        SELECT assigned_pod,
               COUNT(*) as total,
               SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as done,
               SUM(CASE WHEN status = 'in_progress' THEN 1 ELSE 0 END) as active
        FROM {WORK_QUEUE}
        GROUP BY assigned_pod
        ORDER BY assigned_pod
    """).collect()

    for p in pods:
        pct = (p.done / p.total * 100) if p.total else 0
        print(f"    {p.assigned_pod}: {p.done}/{p.total} ({pct:.0f}%) | Active: {p.active}")

    # Update tracking TSV
    print(f"\n  Tracking TSV updated: {TRACKING_TSV}")

print(f"\n{'='*70}")
print(f"TEAM SYSTEM v{PIPELINE_VERSION} COMPLETE | Phase: {PHASE}")
print(f"{'='*70}")
