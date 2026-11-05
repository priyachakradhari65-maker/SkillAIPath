# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Config + Parameters
# ============================================================================
# AUTONOMOUS PIPELINE - One Generic Job for Whatever You Want
# ============================================================================
# This notebook replaces 4 separate pipeline notebooks + Remediation_Executor.
# It runs as a single Job task in databricks.yml (Job 31_Autonomous).
#
# CONTROL MODEL (embedded for portability):
#   User owns:      Live Apps, Genie Spaces, Genie Code, Jobs (interface & orchestration)
#   Assistant owns: Pipeline, ML, DBT, source code, intelligence layer (autonomous execution)
#   Sync point:     User directs via App/Genie/Job -> work_queue -> this pipeline executes
#
# Parameters (from Job or App/Genie/BI interface):
#   phase:      all | idea | approval | creation | storytelling (default: all)
#   direction:  Free-text instruction from user (e.g. "Fix T28 RAG", "Scan for issues")
#   priority:   P0 | P1 | P2 | all (default: all)
#   max_tasks:  Max tasks per run (default: 5)
#   environment: dev | main (default: dev)
#
# Interface: User feeds direction from Databricks App, Genie Code, or BI dashboard.
#            This notebook interprets the direction and executes autonomously.
# Training:  User provides all training agents (SkillAiEngineer + Tech_Agents/).
# ============================================================================

from datetime import datetime
import re, json

# Define widgets (Job base_parameters override these; manual runs use defaults)
dbutils.widgets.text("phase", "all", "Phase: all|idea|approval|creation|storytelling")
dbutils.widgets.text("direction", "", "Free-text direction from App/Genie/BI")
dbutils.widgets.text("priority", "all", "Priority filter: P0|P1|P2|all")
dbutils.widgets.text("max_tasks", "5", "Max tasks per run")
dbutils.widgets.text("environment", "dev", "Environment: dev|main")

# Read parameters (Job base_parameters override widget defaults)
PHASE = dbutils.widgets.get("phase")
DIRECTION = dbutils.widgets.get("direction")
PRIORITY = dbutils.widgets.get("priority")
MAX_TASKS = int(dbutils.widgets.get("max_tasks"))
ENVIRONMENT = dbutils.widgets.get("environment")

# Map environment to catalog
CATALOG = "sap_dev" if ENVIRONMENT == "dev" else "sap_main"

# Constants (environment-aware)
WORK_QUEUE = f"{CATALOG}.autonomous.work_queue"
LOG_TABLE = f"{CATALOG}.autonomous.execution_log"
PROMPT_FILE = f"/Workspace/Shared/{ENVIRONMENT}/SkillAIPath/5_Lead_Engineer/3_Lead_Engineer/5_Improvements/PRODUCTION_REMEDIATION_PLAYBOOK.md"
SCOPE_FILE = f"/Workspace/Shared/{ENVIRONMENT}/SkillAIPath/5_Lead_Engineer/3_Lead_Engineer/5_Improvements/IMPROVEMENT_SCOPE.md"

# Ensure schema + tables exist (auto-create on first run in new env)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.autonomous")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {WORK_QUEUE} (
        work_id STRING,
        title STRING,
        description STRING,
        layer STRING,
        priority STRING,
        assigned_pod STRING,
        depends_on ARRAY<STRING>,
        status STRING,
        source STRING,
        business_impact STRING,
        pipeline_version INT,
        idea_source STRING,
        started_at TIMESTAMP,
        completed_at TIMESTAMP,
        execution_log STRING,
        branch_name STRING,
        validation_passed BOOLEAN,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    ) USING DELTA
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {LOG_TABLE} (
        work_id STRING, executor STRING, action STRING,
        detail STRING, timestamp TIMESTAMP, success BOOLEAN
    ) USING DELTA
""")

# Pipeline version (increments each run)
PIPELINE_VERSION = spark.sql(f"SELECT COALESCE(MAX(pipeline_version), 0) + 1 FROM {WORK_QUEUE}").collect()[0][0]

print("=" * 70)
print(f"AUTONOMOUS PIPELINE v{PIPELINE_VERSION} | Phase: {PHASE} | Priority: {PRIORITY}")
if DIRECTION:
    print(f"Direction: {DIRECTION}")
print(f"Started: {datetime.now()} | Max tasks: {MAX_TASKS}")
print("=" * 70)

spark.sql(f"""
    INSERT INTO {LOG_TABLE}
    VALUES ('PIPELINE_{PIPELINE_VERSION}', 'AutonomousPipeline', 'start',
            'Phase: {PHASE}, Direction: {DIRECTION[:50]}, Priority: {PRIORITY}',
            CURRENT_TIMESTAMP(), true)
""")

# COMMAND ----------

# DBTITLE 1,Phase: Idea
# ============================================================================
# PHASE: IDEA  (runs if phase in ['all', 'idea'] OR direction is provided)
# ============================================================================
# Discovers what to build next. Sources:
# 1. User direction (from App/Genie/BI) -> creates task in work_queue
# 2. System scan (empty tables, DQ gaps, undeployed dashboards)
# 3. ImprovementScope.md (43 known bugs)
# ============================================================================

if PHASE in ('all', 'idea') or DIRECTION:
    print("\n--- IDEA ENGINE ---")

    # 1. Process user direction if provided
    if DIRECTION:
        print(f"  Processing user direction: {DIRECTION[:80]}")

        # Check if direction references a specific task (e.g. "Fix T28")
        task_match = re.search(r'T(\d{2})', DIRECTION, re.IGNORECASE)
        if task_match:
            task_id = f"T{task_match.group(1)}"
            existing = spark.sql(f"SELECT work_id FROM {WORK_QUEUE} WHERE work_id = '{task_id}'").collect()
            if existing:
                # Elevate priority of existing task and reset to queued for re-processing
                spark.sql(f"""
                    UPDATE {WORK_QUEUE}
                    SET source = 'approved', priority = 'P0', status = 'queued',
                        pipeline_version = {PIPELINE_VERSION},
                        updated_at = CURRENT_TIMESTAMP()
                    WHERE work_id = '{task_id}'
                """)
                print(f"  -> Elevated {task_id} to P0, status=queued (user-directed)")
            else:
                print(f"  -> {task_id} not in work_queue. Creating new task.")
                spark.sql(f"""
                    INSERT INTO {WORK_QUEUE}
                    (work_id, title, description, layer, priority, assigned_pod,
                     depends_on, status, source, business_impact, pipeline_version,
                     idea_source, created_at, updated_at)
                    VALUES ('{task_id}', 'User-directed: {DIRECTION[:60]}',
                            '{DIRECTION}', 'Auto', 'P0', 'Pod9_Ops Team',
                            array(), 'queued', 'approved', '{DIRECTION[:100]}',
                            {PIPELINE_VERSION}, 'human',
                            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
                """)
                print(f"  -> Created {task_id} from user direction")
        else:
            # Generic direction - create as new task
            new_id = f"U{datetime.now().strftime('%H%M%S')}"
            spark.sql(f"""
                INSERT INTO {WORK_QUEUE}
                (work_id, title, description, layer, priority, assigned_pod,
                 depends_on, status, source, business_impact, pipeline_version,
                 idea_source, created_at, updated_at)
                VALUES ('{new_id}', 'User direction: {DIRECTION[:60]}',
                        '{DIRECTION}', 'Auto', 'P0', 'Pod9_Ops Team',
                        array(), 'queued', 'approved', '{DIRECTION[:100]}',
                        {PIPELINE_VERSION}, 'human',
                        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
            """)
            print(f"  -> Created {new_id} from user direction")

    # 2. System health scan — auto-discover issues
    print("  Scanning system health...")
    discovered = []

    def discover_task(task_id, title, layer, priority, pod):
        """Create a task if it doesn't already exist."""
        existing = spark.sql(f"SELECT work_id FROM {WORK_QUEUE} WHERE work_id = '{task_id}'").collect()
        if existing:
            return False
        title_esc = title.replace("'", "''")[:200]
        spark.sql(f"""
            INSERT INTO {WORK_QUEUE}
            (work_id, title, description, layer, priority, assigned_pod,
             depends_on, status, source, business_impact, pipeline_version,
             idea_source, created_at, updated_at)
            VALUES ('{task_id}', '{title_esc}', '{title_esc}', '{layer}', '{priority}', '{pod}',
                    array(), 'queued', 'system_scan', '{layer}',
                    {PIPELINE_VERSION}, 'auto_discover',
                    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
        """)
        discovered.append(task_id)
        print(f"    Discovered {task_id} [{priority}] {title_esc[:50]}")
        return True

    try:
        # 2a. Empty tables (0 rows)
        print("  [A] Scanning for empty tables...")
        all_tables = spark.sql(f"""
            SELECT table_schema, table_name
            FROM {CATALOG}.information_schema.tables
            WHERE table_schema IN ('gold', 'silver', 'bronze', 'staging', 'reference', 'content_studio')
            AND table_type = 'MANAGED'
            ORDER BY table_schema, table_name
        """).collect()

        for tbl in all_tables:
            try:
                rc = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{tbl.table_schema}.{tbl.table_name}").collect()[0][0]
                if rc == 0:
                    discover_task(
                        f"SYS_EMPTY_{tbl.table_schema}_{tbl.table_name}",
                        f"Empty table: {tbl.table_schema}.{tbl.table_name} (0 rows)",
                        tbl.table_schema.capitalize(), 'P1', 'Pod1_DataHunters'
                    )
            except Exception:
                pass

        # 2b. Tables with no column comments
        print("  [B] Scanning for missing column comments...")
        no_comments = spark.sql(f"""
            SELECT table_schema, table_name, COUNT(*) as col_count
            FROM {CATALOG}.information_schema.columns
            WHERE table_schema IN ('gold', 'silver', 'bronze')
              AND comment IS NULL
            GROUP BY table_schema, table_name
            ORDER BY col_count DESC
            LIMIT 5
        """).collect()

        for tbl in no_comments:
            discover_task(
                f"SYS_NOCOMMENT_{tbl.table_schema}_{tbl.table_name}",
                f"No column comments: {tbl.table_schema}.{tbl.table_name} ({tbl.col_count} cols)",
                'Governance', 'P2', 'Pod8_Gatekeepers'
            )

        # 2c. Empty schemas (0 tables)
        print("  [C] Scanning for empty schemas...")
        empty_schemas = spark.sql(f"""
            SELECT schema_name
            FROM {CATALOG}.information_schema.schemata
            WHERE schema_name NOT IN (
                SELECT DISTINCT table_schema FROM {CATALOG}.information_schema.tables
            )
            AND schema_name NOT IN ('information_schema')
        """).collect()

        for sch in empty_schemas:
            discover_task(
                f"SYS_EMPTYSCHEMA_{sch.schema_name}",
                f"Empty schema: {sch.schema_name} (0 tables)",
                'Catalog', 'P3', 'Pod8_Gatekeepers'
            )

        print(f"  Discovered {len(discovered)} new issues from system scan")
    except Exception as e:
        print(f"  System scan error: {str(e)[:80]}")

    # 3. Auto-seed from IMPROVEMENT_SCOPE.md (43 known issues)
    print("  Checking IMPROVEMENT_SCOPE.md for tasks to seed...")
    try:
        with open(SCOPE_FILE, 'r') as f:
            scope_content = f.read()

        layer_to_pod = {
            'gold': 'Pod6_StoryBuilders', 'dash': 'Pod6_StoryBuilders',
            'ml': 'Pod5_ModelTrainers', 'rag': 'Pod4_AgentMakers',
            'edufin': 'Pod2_Architects', 'bronze': 'Pod1_DataHunters',
            'silver': 'Pod2_Architects', 'all': 'Pod9_Conductors',
            'devops': 'Pod3_QualityGuardians', 'catalog': 'Pod8_Gatekeepers',
            'reference': 'Pod8_Gatekeepers', 'platform': 'Pod8_Gatekeepers',
            'dbt': 'Pod2_Architects',
        }

        seeded = 0
        skipped = 0

        # P0 and P1: 3-column tables (| T01 | issue | layer |)
        for prio, pat in [('P0', r'## P0 CRITICAL.*?\n(.*?)(?=\n## )'),
                         ('P1', r'## P1 HIGH.*?\n(.*?)(?=\n## )')]:
            m = re.search(pat, scope_content, re.DOTALL)
            if not m:
                continue
            for tid, issue, layer in re.findall(r'\|\s*(T\d{2})\s*\|\s*(.+?)\s*\|\s*(.+?)\s*\|', m.group(1)):
                if tid == 'ID':
                    continue
                existing = spark.sql(f"SELECT work_id FROM {WORK_QUEUE} WHERE work_id = '{tid}'").collect()
                if existing:
                    skipped += 1
                    continue
                issue_esc = issue.replace("'", "''").strip()[:200]
                layer_clean = layer.strip()
                pod = 'Pod9_Conductors'
                for lk, pv in layer_to_pod.items():
                    if lk in layer_clean.lower():
                        pod = pv
                        break
                spark.sql(f"""
                    INSERT INTO {WORK_QUEUE}
                    (work_id, title, description, layer, priority, assigned_pod,
                     depends_on, status, source, business_impact, pipeline_version,
                     idea_source, created_at, updated_at)
                    VALUES ('{tid}', '{issue_esc}', '{issue_esc}', '{layer_clean}', '{prio}', '{pod}',
                            array(), 'queued', 'improvement_scope', '{layer_clean}',
                            {PIPELINE_VERSION}, 'scope_file',
                            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
                """)
                seeded += 1
                print(f"    Seeded {tid} [{prio}] {issue_esc[:50]}")

        # P2: 2-column table (| T08 | issue |)
        m = re.search(r'## P2 MEDIUM.*?\n(.*?)(?=\n## )', scope_content, re.DOTALL)
        if m:
            for tid, issue in re.findall(r'\|\s*(T\d{2})\s*\|\s*(.+?)\s*\|', m.group(1)):
                if tid == 'ID':
                    continue
                existing = spark.sql(f"SELECT work_id FROM {WORK_QUEUE} WHERE work_id = '{tid}'").collect()
                if existing:
                    skipped += 1
                    continue
                issue_esc = issue.replace("'", "''").strip()[:200]
                spark.sql(f"""
                    INSERT INTO {WORK_QUEUE}
                    (work_id, title, description, layer, priority, assigned_pod,
                     depends_on, status, source, business_impact, pipeline_version,
                     idea_source, created_at, updated_at)
                    VALUES ('{tid}', '{issue_esc}', '{issue_esc}', 'All', 'P2', 'Pod9_Conductors',
                            array(), 'queued', 'improvement_scope', 'Low',
                            {PIPELINE_VERSION}, 'scope_file',
                            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
                """)
                seeded += 1
                print(f"    Seeded {tid} [P2] {issue_esc[:50]}")

        # P3: inline format (T15 description | T16 description | ...)
        m = re.search(r'## P3 LOW.*?\n(.+)', scope_content, re.DOTALL)
        if m:
            for tid, desc in re.findall(r'(T\d{2})\s+([^|]+)', m.group(1)):
                existing = spark.sql(f"SELECT work_id FROM {WORK_QUEUE} WHERE work_id = '{tid}'").collect()
                if existing:
                    skipped += 1
                    continue
                desc_esc = desc.strip().replace("'", "''")[:200]
                spark.sql(f"""
                    INSERT INTO {WORK_QUEUE}
                    (work_id, title, description, layer, priority, assigned_pod,
                     depends_on, status, source, business_impact, pipeline_version,
                     idea_source, created_at, updated_at)
                    VALUES ('{tid}', '{desc_esc}', '{desc_esc}', 'All', 'P3', 'Pod9_Conductors',
                            array(), 'queued', 'improvement_scope', 'Low',
                            {PIPELINE_VERSION}, 'scope_file',
                            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
                """)
                seeded += 1
                print(f"    Seeded {tid} [P3] {desc_esc[:50]}")

        print(f"  Seeded {seeded} new tasks (skipped {skipped} existing)")
    except FileNotFoundError:
        print(f"  SCOPE_FILE not found: {SCOPE_FILE}")
    except Exception as e:
        print(f"  Seeding error: {e}")

    # 4. Queue status
    queue_stats = spark.sql(f"""
        SELECT priority, status, COUNT(*) as cnt
        FROM {WORK_QUEUE}
        GROUP BY priority, status
        ORDER BY priority, status
    """).collect()

    print("  Queue status:")
    for s in queue_stats:
        print(f"    {s.priority} | {s.status}: {s.cnt}")

    total = sum(s.cnt for s in queue_stats)
    done = sum(s.cnt for s in queue_stats if s.status == 'completed')
    print(f"  Completion: {done}/{total} ({(done/total*100) if total else 0:.1f}%)")

    spark.sql(f"""
        INSERT INTO {LOG_TABLE}
        VALUES ('PIPELINE_{PIPELINE_VERSION}', 'IdeaEngine', 'complete',
                'Queue: {done}/{total} done. Direction: {"yes" if DIRECTION else "none"}',
                CURRENT_TIMESTAMP(), true)
    """)
    print("  Idea Engine complete.")

# COMMAND ----------

# DBTITLE 1,Phase: Approval
# ============================================================================
# PHASE: APPROVAL  (runs if phase in ['all', 'approval'])
# ============================================================================
# Auto-approve low-risk tasks, flag high-risk for human review.
# Rules:
#   - DevOps/Governance layer -> always HUMAN
#   - Blocked by incomplete deps -> stays pending
#   - Everything else -> AUTO-APPROVE (source='approved')
# ============================================================================

if PHASE in ('all', 'approval'):
    print("\n--- APPROVAL GATE ---")

    pending = spark.sql(f"""
        SELECT work_id, title, priority, layer, assigned_pod, depends_on
        FROM {WORK_QUEUE}
        WHERE status = 'queued'
          AND source IN ('improvement_scope', 'idea', 'system_scan')
        ORDER BY priority, work_id
    """).collect()

    auto_ok = 0
    human = 0

    for task in pending:
        wid = task.work_id
        deps = task.depends_on if task.depends_on else []
        layer = task.layer or ''

        can_auto = True
        reason = ''

        # Check dependencies
        if deps:
            incomplete = spark.sql(f"""
                SELECT COUNT(*) FROM {WORK_QUEUE}
                WHERE work_id IN ({','.join([f"'{d}'" for d in deps])})
                  AND status != 'completed'
            """).collect()[0][0]
            if incomplete > 0:
                can_auto = False
                reason = f'blocked by {incomplete} deps'

        # DevOps/Governance always human
        if layer in ('DevOps', 'Governance'):
            can_auto = False
            reason = f'{layer} needs human'

        if can_auto:
            spark.sql(f"""
                UPDATE {WORK_QUEUE}
                SET source = 'approved', pipeline_version = {PIPELINE_VERSION},
                    updated_at = CURRENT_TIMESTAMP()
                WHERE work_id = '{wid}'
            """)
            auto_ok += 1
        else:
            spark.sql(f"""
                UPDATE {WORK_QUEUE}
                SET source = 'pending_approval', pipeline_version = {PIPELINE_VERSION},
                    updated_at = CURRENT_TIMESTAMP()
                WHERE work_id = '{wid}'
            """)
            human += 1

    print(f"  Auto-approved: {auto_ok}")
    print(f"  Human review: {human}")
    spark.sql(f"""
        INSERT INTO {LOG_TABLE}
        VALUES ('PIPELINE_{PIPELINE_VERSION}', 'ApprovalGate', 'complete',
                'Auto: {auto_ok}, Human: {human}', CURRENT_TIMESTAMP(), true)
    """)
    print("  Approval Gate complete.")

# COMMAND ----------

# DBTITLE 1,Phase: Creation
# ============================================================================
# PHASE: CREATION  (runs if phase in ['all', 'creation'])
# ============================================================================
# Claims approved tasks, checks if issue still exists, loads prompts,
# runs verification SQL. No fake completions.
# Training agents (SkillAiEngineer + Tech_Agents/) provide intelligence.
# ============================================================================

if PHASE in ('all', 'creation'):
    print("\n--- CREATION EXECUTOR ---")

    # Load PRODUCTION_REMEDIATION_PLAYBOOK.md for prompts + verification SQL
    remediation_content = ''
    try:
        with open(PROMPT_FILE, 'r') as f:
            remediation_content = f.read()
        print(f"  Loaded PRODUCTION_REMEDIATION_PLAYBOOK.md ({len(remediation_content)} chars)")
    except Exception as e:
        print(f"  WARNING: Cannot read prompt file: {e}")

    # Get approved tasks
    priority_filter = f"AND priority = '{PRIORITY}'" if PRIORITY != 'all' else ''
    ready = spark.sql(f"""
        SELECT work_id, title, layer, description, assigned_pod, business_impact, depends_on
        FROM {WORK_QUEUE}
        WHERE status = 'queued' AND source = 'approved' {priority_filter}
        ORDER BY priority, work_id
        LIMIT {MAX_TASKS}
    """).collect()

    print(f"  Ready tasks: {len(ready)}")
    results = []

    for task in ready:
        wid = task.work_id
        layer = task.layer or 'unknown'
        branch = f"auto/{wid}/{layer.lower()}-fix"

        print(f"\n  EXECUTING: {wid} - {task.title[:60]}")
        if task.business_impact:
            print(f"    Impact: {task.business_impact[:80]}")

        # Atomic claim
        spark.sql(f"""
            UPDATE {WORK_QUEUE}
            SET status = 'in_progress', started_at = CURRENT_TIMESTAMP(),
                source = 'executed', pipeline_version = {PIPELINE_VERSION}
            WHERE work_id = '{wid}' AND status = 'queued'
        """)
        spark.sql(f"""
            INSERT INTO {LOG_TABLE}
            VALUES ('{wid}', 'CreationExecutor', 'claim', 'Task claimed', CURRENT_TIMESTAMP(), true)
        """)

        # Parse PROMPT TO FIRE + Verification Query from PRODUCTION_REMEDIATION_PLAYBOOK.md
        check_sql = fix_text = verify_sql = None
        if remediation_content:
            # Match task section: ### T01: ... until next ### T\d{2}: or --- or end
            match = re.search(rf'### {wid}:\s*(.+?)(?=### T\d{{2}}:|\n---\n## |\Z)', remediation_content, re.DOTALL)
            if match:
                body = match.group(1)
                # PROMPT TO FIRE → fix instructions (inside code block)
                fm = re.search(r'\*\*PROMPT TO FIRE\*\*:\s*```\s*\n(.+?)```', body, re.DOTALL)
                if fm: fix_text = fm.group(1).strip()
                # Verification Query → verify SQL (inside ```sql code block)
                vm = re.search(r'\*\*Verification Query\*\*:\s*```sql\s*\n(.+?)```', body, re.DOTALL)
                if vm: verify_sql = vm.group(1).strip()

        # Check if issue still exists
        issue_exists = True
        if check_sql:
            try:
                r = spark.sql(check_sql).collect()
                if len(r) == 0:
                    issue_exists = False
                    print(f"    -> Already resolved (check returned 0 rows)")
            except Exception as e:
                print(f"    Check error: {str(e)[:80]}")

        if not issue_exists:
            spark.sql(f"""
                UPDATE {WORK_QUEUE}
                SET status = 'completed', completed_at = CURRENT_TIMESTAMP(),
                    validation_passed = true, source = 'completed',
                    branch_name = '{branch}',
                    execution_log = 'Already resolved',
                    pipeline_version = {PIPELINE_VERSION}
                WHERE work_id = '{wid}'
            """)
            results.append(('completed', wid))
            print(f"    COMPLETED: Already resolved")
        else:
            # Issue exists - load fix instructions
            print(f"    Fix: {fix_text[:100] if fix_text else task.description[:100]}")
            if verify_sql:
                print(f"    Verify: {verify_sql[:100]}")

            # Try verification to see if fix is already applied
            if verify_sql:
                try:
                    vr = spark.sql(verify_sql).collect()
                    print(f"    Verification: {len(vr)} rows (fix may be applied)")
                    spark.sql(f"""
                        UPDATE {WORK_QUEUE}
                        SET execution_log = 'Fix may be applied. Verify manually.',
                               branch_name = '{branch}',
                               pipeline_version = {PIPELINE_VERSION}
                        WHERE work_id = '{wid}'
                    """)
                    results.append(('verify_pass', wid))
                except Exception:
                    print(f"    Verification failed = issue confirmed, fix needed")
                    spark.sql(f"""
                        UPDATE {WORK_QUEUE}
                        SET execution_log = 'Issue confirmed. Fix needed.',
                               branch_name = '{branch}',
                               pipeline_version = {PIPELINE_VERSION}
                        WHERE work_id = '{wid}'
                    """)
                    results.append(('fix_needed', wid))
            else:
                results.append(('no_verify', wid))
                print(f"    No verification SQL available")

    done = sum(1 for r in results if r[0] == 'completed')
    fix = sum(1 for r in results if r[0] in ('fix_needed', 'verify_pass'))
    spark.sql(f"""
        INSERT INTO {LOG_TABLE}
        VALUES ('PIPELINE_{PIPELINE_VERSION}', 'CreationExecutor', 'complete',
                'Processed {len(results)}. Done: {done}. Fix needed: {fix}',
                CURRENT_TIMESTAMP(), true)
    """)
    print(f"\n  Creation batch: {len(results)} tasks (done: {done}, fix: {fix})")
    print("  Creation Executor complete.")

# COMMAND ----------

# DBTITLE 1,Phase: StoryTelling
# ============================================================================
# PHASE: STORYTELLING  (runs if phase in ['all', 'storytelling'])
# ============================================================================
# Generates business impact report. Communicates in SkillAIPath terms.
# Output: printed report + execution_log entries.
# ============================================================================

if PHASE in ('all', 'storytelling'):
    print("\n--- STORYTELLER ---")

    total_all = spark.sql(f"SELECT COUNT(*) FROM {WORK_QUEUE}").collect()[0][0]
    total_done = spark.sql(f"SELECT COUNT(*) FROM {WORK_QUEUE} WHERE status = 'completed'").collect()[0][0]
    pct = (total_done / total_all * 100) if total_all else 0

    print(f"\n{'='*70}")
    print(f"BUSINESS IMPACT REPORT - SkillAIPath Autonomous Pipeline v{PIPELINE_VERSION}")
    print(f"{'='*70}")

    # Completed this run
    run_tasks = spark.sql(f"""
        SELECT work_id, title, priority, status, assigned_pod, business_impact, branch_name
        FROM {WORK_QUEUE}
        WHERE pipeline_version = {PIPELINE_VERSION}
        ORDER BY priority, work_id
    """).collect()

    completed = [t for t in run_tasks if t.status == 'completed']
    in_progress = [t for t in run_tasks if t.status == 'in_progress']
    fix_needed = [t for t in run_tasks if t.status == 'queued' and t.business_impact]

    if completed:
        print(f"\n  COMPLETED THIS RUN ({len(completed)}):")
        for t in completed:
            print(f"    {t.work_id} [{t.priority}] {t.title[:55]}")
            if t.business_impact:
                print(f"      -> {t.business_impact[:90]}")

    if fix_needed:
        print(f"\n  FIXES NEEDED ({len(fix_needed)}):")
        for t in fix_needed[:5]:
            print(f"    {t.work_id} [{t.priority}] {t.title[:55]}")
            if t.business_impact:
                print(f"      -> {t.business_impact[:90]}")

    # Pod-wise status
    print(f"\n  POD STATUS:")
    pods = spark.sql(f"""
        SELECT assigned_pod,
               COUNT(*) as total,
               SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as done
        FROM {WORK_QUEUE}
        GROUP BY assigned_pod
        ORDER BY assigned_pod
    """).collect()
    for p in pods:
        pr = (p.done / p.total * 100) if p.total else 0
        print(f"    {p.assigned_pod}: {p.done}/{p.total} ({pr:.0f}%)")

    print(f"\n  OVERALL: {total_done}/{total_all} ({pct:.1f}%) | Target: 90% | v{PIPELINE_VERSION}")
    print(f"  Next run: v{PIPELINE_VERSION + 1}")

    spark.sql(f"""
        INSERT INTO {LOG_TABLE}
        VALUES ('PIPELINE_{PIPELINE_VERSION}', 'StoryTeller', 'complete',
                'v{PIPELINE_VERSION}: {total_done}/{total_all} ({pct:.1f}%)',
                CURRENT_TIMESTAMP(), true)
    """)
    print("\n  StoryTeller complete.")

print(f"\n{'='*70}")
print(f"AUTONOMOUS PIPELINE v{PIPELINE_VERSION} COMPLETE")
print(f"{'='*70}")

# COMMAND ----------

