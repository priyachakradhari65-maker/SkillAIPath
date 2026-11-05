"""
Context Store - Training Data Management for PR Reviewer

Handles storage and retrieval of team context including:
- Discord updates
- Sprint plans
- Task allocations
- Growth accelerator roadmap
- Developer status tracking
"""

import uuid
import re
import os
import json
from datetime import datetime, date
from typing import Dict, List, Optional

try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service import sql as sdk_sql
    SDK_AVAILABLE = True
except ImportError:
    SDK_AVAILABLE = False


class ContextStore:
    """Manages all context storage and retrieval operations
    
    Refactored to use Databricks SDK instead of SparkSession for Apps V2 compatibility.
    Storage failures never block PR reviews.
    """
    
    def __init__(self):
        self.catalog = "sap_dev"
        self.schema = "pr_reviewer"
        self.enabled = False
        self.w = None
        self.warehouse_id = None
        
        if not SDK_AVAILABLE:
            print("⚠️ ContextStore: Databricks SDK not available, storage disabled")
            return
        
        try:
            self.w = WorkspaceClient()
            # Try to get warehouse ID from environment or use first available
            self.warehouse_id = os.environ.get('DATABRICKS_WAREHOUSE_ID')
            if not self.warehouse_id:
                warehouses = list(self.w.warehouses.list())
                if warehouses:
                    self.warehouse_id = warehouses[0].id
                    print(f"ℹ️ ContextStore: Using warehouse {self.warehouse_id}")
            
            if self.warehouse_id:
                self.enabled = True
                print("✅ ContextStore: Initialized successfully")
            else:
                print("⚠️ ContextStore: No SQL warehouse available, storage disabled")
        except Exception as e:
            print(f"⚠️ ContextStore: Initialization failed: {str(e)[:200]}")
            self.enabled = False
    
    def _execute_sql(self, sql: str, timeout: int = 30) -> List[Dict]:
        """Execute SQL via SDK and return results as list of dicts"""
        if not self.enabled:
            return []
        
        try:
            result = self.w.statement_execution.execute_statement(
                warehouse_id=self.warehouse_id,
                statement=sql,
                wait_timeout=f"{timeout}s"
            )
            
            if not result.result or not result.result.data_array:
                return []
            
            # Convert to list of dicts
            columns = [col.name for col in result.manifest.schema.columns] if result.manifest and result.manifest.schema else []
            rows = []
            for row_data in result.result.data_array:
                row_dict = {}
                for i, col_name in enumerate(columns):
                    row_dict[col_name] = row_data[i] if i < len(row_data) else None
                rows.append(row_dict)
            return rows
        except Exception as e:
            print(f"⚠️ SQL execution error: {str(e)[:200]}")
            return []
    
    def _insert_json_row(self, table: str, data: Dict) -> bool:
        """Insert a row into a table using JSON"""
        if not self.enabled:
            return False
        
        try:
            # Build INSERT statement
            columns = list(data.keys())
            values = []
            for key in columns:
                val = data[key]
                if val is None:
                    values.append("NULL")
                elif isinstance(val, str):
                    # Escape single quotes
                    escaped = val.replace("'", "''").replace("\\", "\\\\")
                    values.append(f"'{escaped}'")
                elif isinstance(val, (datetime, date)):
                    values.append(f"'{val.isoformat()}'")
                elif isinstance(val, bool):
                    values.append("TRUE" if val else "FALSE")
                elif isinstance(val, (list, dict)):
                    # Store as JSON string
                    json_str = json.dumps(val).replace("'", "''")
                    values.append(f"'{json_str}'")
                else:
                    values.append(str(val))
            
            sql = f"INSERT INTO {self.catalog}.{self.schema}.{table} ({', '.join(columns)}) VALUES ({', '.join(values)})"
            self._execute_sql(sql)
            return True
        except Exception as e:
            print(f"⚠️ Insert error for {table}: {str(e)[:200]}")
            return False
    
    def store_discord_updates(self, discord_text: str, updated_by: str = "system") -> str:
        """Store Discord channel updates and parse developer statuses"""
        try:
            context_id = str(uuid.uuid4())
            self.spark.createDataFrame([{
                "context_id": context_id,
                "context_type": "discord",
                "raw_content": discord_text,
                "updated_at": datetime.now(),
                "updated_by": updated_by
            }]).write.mode("append").saveAsTable(f"{self.catalog}.{self.schema}.team_context")
            
            developers = self._parse_discord_updates(discord_text)
            self._update_developer_status(developers)
            
            return f"✅ Stored Discord updates. Parsed {len(developers)} developer updates."
        except Exception as e:
            return f"❌ Error: {str(e)}"
    
    def _parse_discord_updates(self, discord_text: str) -> Dict[str, Dict]:
        """Parse Discord text to extract developer updates"""
        developers = {}
        update_pattern = r"([A-Za-z ]+) — (\d{1,2}/\d{1,2}/\d{2,4}.*?)\n(.+?)(?=(?:[A-Za-z ]+ — \d{1,2}/\d{1,2}|$))"
        matches = re.findall(update_pattern, discord_text, re.DOTALL)
        
        for name, timestamp, content in matches:
            name = name.strip()
            if name not in developers:
                developers[name] = {"tasks": [], "priorities": [], "last_update": "", "pr_urls": []}
            
            developers[name]["priorities"].extend(re.findall(r"P[0-2]", content))
            developers[name]["tasks"].extend([t.strip() for t in re.findall(r"P[0-2]\s*-\s*([^\n]+)", content)])
            developers[name]["pr_urls"].extend(re.findall(r"https://github\.com/[^\s]+", content))
            
            if content.strip():
                developers[name]["last_update"] = content.strip()[:500]
        
        return developers
    
    def _update_developer_status(self, developers: Dict[str, Dict]):
        """Update developer_status table"""
        for dev_name, dev_data in developers.items():
            self.spark.sql(f"DELETE FROM {self.catalog}.{self.schema}.developer_status WHERE developer_name = '{dev_name}'")
            self.spark.createDataFrame([{
                "developer_name": dev_name,
                "current_tasks": dev_data["tasks"],
                "priorities": dev_data["priorities"],
                "deadlines": [],
                "last_discord_update": dev_data["last_update"],
                "last_update_timestamp": datetime.now()
            }]).write.mode("append").saveAsTable(f"{self.catalog}.{self.schema}.developer_status")
    
    def store_sprint_plan(self, sprint_data: str, updated_by: str = "system") -> str:
        """Store sprint plan from Google Sheet"""
        try:
            context_id = str(uuid.uuid4())
            self.spark.createDataFrame([{
                "context_id": context_id,
                "context_type": "sprint_plan",
                "raw_content": sprint_data,
                "updated_at": datetime.now(),
                "updated_by": updated_by
            }]).write.mode("append").saveAsTable(f"{self.catalog}.{self.schema}.team_context")
            
            tasks = self._parse_sprint_plan(sprint_data)
            self._store_task_allocations(tasks, source="google_sheet")
            
            return f"✅ Stored sprint plan. Parsed {len(tasks)} tasks."
        except Exception as e:
            return f"❌ Error: {str(e)}"
    
    def _parse_sprint_plan(self, sprint_data: str) -> List[Dict]:
        """Parse sprint plan CSV/TSV format"""
        tasks = []
        lines = sprint_data.strip().split("\n")
        
        for line in lines[1:]:
            if not line.strip():
                continue
            parts = line.split("\t") if "\t" in line else (line.split("|") if "|" in line else line.split(","))
            
            if len(parts) >= 4:
                tasks.append({
                    "task_id": str(uuid.uuid4()),
                    "developer_name": parts[0].strip(),
                    "task_description": parts[1].strip(),
                    "priority": parts[2].strip(),
                    "status": parts[3].strip(),
                    "due_date": self._parse_date(parts[4].strip()) if len(parts) > 4 else None
                })
        return tasks
    
    def _parse_date(self, date_str: str) -> Optional[date]:
        """Parse various date formats"""
        if not date_str:
            return None
        for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%Y/%m/%d"]:
            try:
                return datetime.strptime(date_str, fmt).date()
            except:
                continue
        return None
    
    def _store_task_allocations(self, tasks: List[Dict], source: str):
        """Store parsed tasks"""
        if not tasks:
            return
        for task in tasks:
            task["source"] = source
            task["created_at"] = datetime.now()
        self.spark.createDataFrame(tasks).write.mode("append").saveAsTable(f"{self.catalog}.{self.schema}.task_allocations")
    
    def store_roadmap(self, roadmap_data: str, category: str = "Overall Roadmap", updated_by: str = "system") -> str:
        """Store roadmap/growth accelerator items"""
        try:
            context_id = str(uuid.uuid4())
            self.spark.createDataFrame([{
                "context_id": context_id,
                "context_type": "roadmap",
                "raw_content": roadmap_data,
                "updated_at": datetime.now(),
                "updated_by": updated_by
            }]).write.mode("append").saveAsTable(f"{self.catalog}.{self.schema}.team_context")
            
            items = self._parse_roadmap(roadmap_data, category)
            if items:
                self.spark.createDataFrame(items).write.mode("append").saveAsTable(f"{self.catalog}.{self.schema}.roadmap_items")
            
            return f"✅ Stored roadmap. Parsed {len(items)} items."
        except Exception as e:
            return f"❌ Error: {str(e)}"
    
    def _parse_roadmap(self, roadmap_data: str, category: str) -> List[Dict]:
        """Parse roadmap items"""
        items = []
        for line in roadmap_data.strip().split("\n"):
            if not line.strip() or line.startswith("#"):
                continue
            parts = line.split("|")
            if len(parts) >= 3:
                items.append({
                    "roadmap_id": str(uuid.uuid4()),
                    "category": category,
                    "item_description": parts[0].strip(),
                    "assigned_to": parts[1].strip() if len(parts) > 1 else "Unassigned",
                    "target_date": self._parse_date(parts[2].strip()) if len(parts) > 2 else None,
                    "status": parts[3].strip() if len(parts) > 3 else "Not Started",
                    "created_at": datetime.now()
                })
        return items
    
    def get_developer_context(self, developer_name: str) -> Optional[Dict]:
        """Retrieve current context for a specific developer"""
        if not self.enabled:
            return None
        
        try:
            # Escape single quotes for SQL
            safe_name = developer_name.replace("'", "''")
            sql = f"""
                SELECT 
                    developer_name,
                    current_tasks,
                    priorities,
                    last_discord_update,
                    last_update_timestamp
                FROM {self.catalog}.{self.schema}.developer_status 
                WHERE developer_name = '{safe_name}'
                LIMIT 1
            """
            rows = self._execute_sql(sql)
            
            if not rows:
                return None
            
            row = rows[0]
            # Parse JSON arrays if stored as strings
            current_tasks = row.get('current_tasks', [])
            if isinstance(current_tasks, str):
                try:
                    current_tasks = json.loads(current_tasks)
                except:
                    current_tasks = []
            
            priorities = row.get('priorities', [])
            if isinstance(priorities, str):
                try:
                    priorities = json.loads(priorities)
                except:
                    priorities = []
            
            return {
                "current_tasks": current_tasks,
                "priorities": priorities,
                "last_update": row.get('last_discord_update', ''),
                "last_update_time": row.get('last_update_timestamp')
            }
        except Exception as e:
            print(f"⚠️ Error retrieving developer context: {str(e)[:200]}")
            return None
    
    def get_team_summary(self) -> Dict:
        """Get high-level summary of team status"""
        try:
            dev_count = self.spark.sql(f"SELECT COUNT(DISTINCT developer_name) as cnt FROM {self.catalog}.{self.schema}.developer_status").first().cnt
            task_count = self.spark.sql(f"SELECT COUNT(*) as cnt FROM {self.catalog}.{self.schema}.task_allocations WHERE status IN ('In Progress', 'Not Started')").first().cnt
            last_update = self.spark.sql(f"SELECT MAX(updated_at) as last_update FROM {self.catalog}.{self.schema}.team_context").first().last_update
            return {"developers": dev_count or 0, "active_tasks": task_count or 0, "last_update": last_update}
        except Exception as e:
            return {"developers": 0, "active_tasks": 0, "last_update": None, "error": str(e)}


def store_context(discord_text: str = "", sprint_plan: str = "", roadmap: str = "") -> str:
    """Main entry point for storing all context"""
    store = ContextStore()
    results = []
    
    if discord_text.strip():
        results.append(store.store_discord_updates(discord_text))
    if sprint_plan.strip():
        results.append(store.store_sprint_plan(sprint_plan))
    if roadmap.strip():
        results.append(store.store_roadmap(roadmap, category="Growth Accelerator"))
    
    if not results:
        return "⚠️ No data provided to store"
    
    return "\n\n".join(results)


def get_context_for_review(pr_author: str, pr_files: List[str]) -> str:
    """Get relevant context for a PR review - friendly tone like a teammate
    
    Wrapped in try/except to ensure context retrieval failures never block PR reviews.
    """
    try:
        store = ContextStore()
        if not store.enabled:
            return ""
        
        context = store.get_developer_context(pr_author)
        
        if not context:
            return ""
        
        notes = []
        
        if context.get("current_tasks"):
            tasks_str = ", ".join(context["current_tasks"][:3])
            notes.append(f"📋 **I see you're working on**: {tasks_str}")
        
        if context.get("priorities"):
            p0_count = context["priorities"].count("P0")
            if p0_count > 0:
                notes.append(f"🔥 **Heads up**: You've got {p0_count} P0 task(s) on your plate right now")
        
        if context.get("last_update"):
            update_preview = context["last_update"][:150]
            notes.append(f"💬 **Last I heard from you**: {update_preview}...")
        
        if notes:
            return "\n\n---\n\n### 🤝 Quick Context Check\n\n" + "\n\n".join(notes)
        
        return ""
    except Exception as e:
        print(f"⚠️ Context retrieval failed (non-blocking): {str(e)[:200]}")
        return ""


def get_team_summary_display() -> str:
    """Get formatted team summary for UI display"""
    store = ContextStore()
    summary = store.get_team_summary()
    
    if summary.get("error"):
        return f"⚠️ No context data yet. Add your first update above!"
    
    last_update_str = summary["last_update"].strftime("%Y-%m-%d %H:%M") if summary["last_update"] else "Never"
    
    return f"""
📊 **Team Status**
- 👥 Tracking: **{summary['developers']}** developers
- 📋 Active tasks: **{summary['active_tasks']}**
- 🕐 Last update: **{last_update_str}**
"""
