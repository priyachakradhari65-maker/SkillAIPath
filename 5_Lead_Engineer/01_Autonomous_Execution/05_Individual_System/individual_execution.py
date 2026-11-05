# Databricks notebook source
# DBTITLE 1,Individual System - Self-Paced Solo Execution
# ============================================================================
# INDIVIDUAL PIPELINE - One engineer, entire platform, self-paced
# ============================================================================
# This notebook guides a solo engineer through all 119 tasks.
# No team, no cadence. Work at your own pace.
#
# Parameters:
#   action:    next | status | goto | complete (default: next)
#   task_id:   Specific task ID (for goto/complete)
# ============================================================================

from datetime import datetime
import os, csv

dbutils.widgets.text("action", "next", "Action: next|status|goto|complete")
dbutils.widgets.text("task_id", "", "Task ID for goto/complete")

ACTION = dbutils.widgets.get("action")
TASK_ID = dbutils.widgets.get("task_id").strip().upper()

TEAM_TASKS = "/Workspace/Shared/dev/SkillAIPath/5_Lead_Engineer/3_Lead_Engineer/4_Operations/02_TeamTasks.tsv"
TASK_PATTERNS = "/Workspace/Shared/dev/SkillAIPath/5_Lead_Engineer/3_Lead_Engineer/2_TaskPatterns"
PROMPT_LIBRARY = "/Workspace/Shared/dev/SkillAIPath/5_Lead_Engineer/3_Lead_Engineer/1_PromptLibrary"
TRACKING_TSV = "/Workspace/Shared/dev/SkillAIPath/3_AI_Platform/3_AI_Agents/01_Autonomous_Execution/05_Individual_System/Individual_Tracking.tsv"

print("=" * 70)
print(f"INDIVIDUAL SYSTEM | Action: {ACTION} | {datetime.now()}")
print("=" * 70)

# COMMAND ----------

# DBTITLE 1,Load all 119 tasks from TeamTasks.tsv
def load_tasks():
    tasks = []
    with open(TEAM_TASKS, 'r') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            tasks.append(row)
    return tasks

def load_tracking():
    completed = set()
    if os.path.exists(TRACKING_TSV):
        with open(TRACKING_TSV, 'r') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                if row.get('Status') == 'completed':
                    completed.add(row.get('Task_ID', ''))
    return completed

def save_tracking_entry(task_id, title, pod, status, session, file_path, notes=""):
    header = "Task_ID\tTitle\tPod\tModule\tStatus\tSession_Ref\tFile_Path\tCompleted_At\tNotes\n"
    entry = f"{task_id}\t{title}\t{pod}\t\t{status}\t{session}\t{file_path}\t{datetime.now().strftime('%Y-%m-%d %H:%M')}\t{notes}\n"
    
    # Append to tracking file
    if not os.path.exists(TRACKING_TSV):
        with open(TRACKING_TSV, 'w') as f:
            f.write(header)
    with open(TRACKING_TSV, 'a') as f:
        f.write(entry)

all_tasks = load_tasks()
completed_tasks = load_tracking()

print(f"\n  Total tasks: {len(all_tasks)}")
print(f"  Completed: {len(completed_tasks)}")
print(f"  Remaining: {len(all_tasks) - len(completed_tasks)}")
print(f"  Progress: {len(completed_tasks)/len(all_tasks)*100:.1f}%")

# COMMAND ----------

# DBTITLE 1,Action: next - Show next uncompleted task
if ACTION == "next":
    print("\n--- YOUR NEXT TASK ---")
    
    for task in all_tasks:
        task_id = task.get('Task_ID', '')
        if task_id not in completed_tasks:
            title = task.get('Task', '')[:60]
            pod = task.get('Pod', '')
            module = task.get('Module', '')
            objective = task.get('Objective', '')[:80]
            file_path = task.get('File_Path', '')
            priority = task.get('Priority', '')
            blocks = task.get('Blocks_Task_IDs', '')
            
            print(f"\n  Task: {task_id}")
            print(f"  Title: {title}")
            print(f"  Pod: {pod} | Module: {module} | Priority: {priority}")
            print(f"  Objective: {objective}")
            if blocks:
                print(f"  Depends on: {blocks}")
            
            # Find task pattern file
            pattern_num = task_id.split('-')[-1] if '-' in task_id else ''
            pattern_file = None
            if os.path.exists(TASK_PATTERNS):
                for f in os.listdir(TASK_PATTERNS):
                    if f.startswith(f"Task_{pattern_num}_") or pattern_num in f:
                        pattern_file = os.path.join(TASK_PATTERNS, f)
                        break
            
            if pattern_file:
                print(f"  Pattern: {os.path.basename(pattern_file)}")
            if file_path:
                print(f"  Source: {file_path}")
            
            print(f"\n  To complete: run with action=complete, task_id={task_id}")
            break
    else:
        print("\n  🎉 ALL 119 TASKS COMPLETE! Platform mastery achieved.")

# COMMAND ----------

# DBTITLE 1,Action: status - Full progress dashboard
if ACTION == "status":
    print("\n--- PROGRESS DASHBOARD ---")
    
    # Group by pod
    pod_progress = {}
    for task in all_tasks:
        pod = task.get('Pod', 'Unknown')
        if pod not in pod_progress:
            pod_progress[pod] = {'total': 0, 'done': 0}
        pod_progress[pod]['total'] += 1
        if task.get('Task_ID', '') in completed_tasks:
            pod_progress[pod]['done'] += 1
    
    print(f"\n  {'Pod':<25} {'Done':>6} {'Total':>6} {'Pct':>6}")
    print(f"  {'-'*25} {'-'*6} {'-'*6} {'-'*6}")
    for pod in sorted(pod_progress.keys()):
        p = pod_progress[pod]
        pct = p['done']/p['total']*100 if p['total'] else 0
        bar = '█' * int(pct/10) + '░' * (10 - int(pct/10))
        print(f"  {pod:<25} {p['done']:>6} {p['total']:>6} {pct:>5.0f}% {bar}")
    
    total_done = len(completed_tasks)
    total_all = len(all_tasks)
    overall = total_done/total_all*100 if total_all else 0
    print(f"\n  {'OVERALL':<25} {total_done:>6} {total_all:>6} {overall:>5.1f}%")
    
    # Next 5 tasks
    print(f"\n  NEXT 5 TASKS:")
    shown = 0
    for task in all_tasks:
        tid = task.get('Task_ID', '')
        if tid not in completed_tasks:
            print(f"    {tid}: {task.get('Task', '')[:55]}")
            shown += 1
            if shown >= 5:
                break

# COMMAND ----------

# DBTITLE 1,Action: goto - Jump to specific task
if ACTION == "goto" and TASK_ID:
    print(f"\n--- TASK: {TASK_ID} ---")
    for task in all_tasks:
        if task.get('Task_ID', '') == TASK_ID:
            print(f"  Title: {task.get('Task', '')}")
            print(f"  Pod: {task.get('Pod', '')}")
            print(f"  Module: {task.get('Module', '')}")
            print(f"  Objective: {task.get('Objective', '')}")
            print(f"  Priority: {task.get('Priority', '')}")
            print(f"  File: {task.get('File_Path', '')}")
            
            status = "✅ Completed" if TASK_ID in completed_tasks else "⬜ Not started"
            print(f"  Status: {status}")
            break
    else:
        print(f"  Task {TASK_ID} not found in 02_TeamTasks.tsv")

# COMMAND ----------

# DBTITLE 1,Action: complete - Mark task as done
if ACTION == "complete" and TASK_ID:
    print(f"\n--- COMPLETING: {TASK_ID} ---")
    
    for task in all_tasks:
        if task.get('Task_ID', '') == TASK_ID:
            title = task.get('Task', '')[:80]
            pod = task.get('Pod', '')
            file_path = task.get('File_Path', '')
            
            save_tracking_entry(TASK_ID, title, pod, "completed", "", file_path)
            
            # Count remaining
            new_completed = completed_tasks | {TASK_ID}
            remaining = len(all_tasks) - len(new_completed)
            pct = len(new_completed)/len(all_tasks)*100
            
            print(f"  ✅ {TASK_ID} marked complete: {title}")
            print(f"  Progress: {len(new_completed)}/{len(all_tasks)} ({pct:.1f}%)")
            print(f"  Remaining: {remaining} tasks")
            
            # Show next task
            for next_task in all_tasks:
                if next_task.get('Task_ID', '') not in new_completed:
                    print(f"\n  Next: {next_task.get('Task_ID', '')} - {next_task.get('Task', '')[:55]}")
                    break
            else:
                print("\n  🎉 ALL 119 TASKS COMPLETE!")
            break
    else:
        print(f"  Task {TASK_ID} not found.")

print(f"\n{'='*70}")
print(f"INDIVIDUAL SYSTEM COMPLETE | Action: {ACTION}")
print(f"{'='*70}")
