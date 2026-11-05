"""
User Context Management - Tracks user profiles, chat history, and activity
"""

import json
import os
from datetime import datetime
from typing import Dict, List, Optional
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


class UserContextManager:
    """Manages user profiles, chat history, and activity tracking in Unity Catalog"""
    
    def __init__(self, workspace_client: WorkspaceClient, catalog: str = "sap_dev", schema: str = "gold"):
        self.w = workspace_client
        self.catalog = catalog
        self.schema = schema
        # Resolve a real SQL warehouse. This was previously an empty string,
        # which made every Statement Execution API call fail with
        # "InvalidParameterValue: is not a valid endpoint id."
        self.warehouse_id = self._resolve_warehouse_id()
        self._ensure_tables_exist()

    def _resolve_warehouse_id(self) -> str:
        """WAREHOUSE_ID env var first, then first available warehouse."""
        warehouse_id = os.getenv("WAREHOUSE_ID", "").strip()
        if warehouse_id:
            return warehouse_id
        try:
            warehouses = list(self.w.warehouses.list())
        except Exception as e:
            raise RuntimeError(f"Failed to list SQL warehouses: {e}")
        if not warehouses:
            raise RuntimeError("No SQL warehouses available in this workspace")
        return warehouses[0].id

    def _ensure_tables_exist(self):
        """Create tracking tables if they don't exist"""
        
        # User Profiles Table
        user_profiles_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.catalog}.{self.schema}.skillai_user_profiles (
            user_email STRING,
            user_name STRING,
            role STRING,
            team STRING,
            first_seen TIMESTAMP,
            last_active TIMESTAMP,
            total_interactions INT,
            preferences STRING,
            context_data STRING
        )
        USING DELTA
        COMMENT 'User profiles and preferences for SkillAI Execution Support Agent'
        """
        
        # Chat History Table
        chat_history_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.catalog}.{self.schema}.skillai_chat_history (
            conversation_id STRING,
            user_email STRING,
            message_id STRING,
            timestamp TIMESTAMP,
            role STRING,
            content STRING,
            context STRING,
            metadata STRING
        )
        USING DELTA
        COMMENT 'Chat conversation history for SkillAI Execution Support Agent'
        """
        
        # User Activity Table
        user_activity_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.catalog}.{self.schema}.skillai_user_activity (
            activity_id STRING,
            user_email STRING,
            timestamp TIMESTAMP,
            activity_type STRING,
            sprint_plan STRING,
            action_items STRING,
            bugs STRING,
            deliverables STRING,
            status STRING
        )
        USING DELTA
        COMMENT 'User activity tracking: sprints, tasks, bugs, deliverables'
        """
        
        for sql in [user_profiles_sql, chat_history_sql, user_activity_sql]:
            try:
                self._execute_sql(sql)
            except Exception as e:
                print(f"Table creation error (may already exist): {e}")
    
    def _execute_sql(self, sql: str, timeout: int = 60) -> List[Dict]:
        """Execute SQL and return results"""
        statement = self.w.statement_execution.execute_statement(
            statement=sql,
            warehouse_id=self.warehouse_id,
            wait_timeout="50s"
        )
        
        state = statement.status.state
        if state == StatementState.SUCCEEDED:
            if statement.result and statement.result.data_array:
                columns = [col.name for col in statement.manifest.schema.columns]
                return [
                    dict(zip(columns, row))
                    for row in statement.result.data_array
                ]
            return []

        # Surface real errors instead of silently returning an empty result
        error = getattr(statement.status, "error", None)
        detail = getattr(error, "message", None) or str(state)
        raise RuntimeError(f"SQL statement failed ({state}): {detail}")
    
    def get_or_create_user(self, user_email: str, user_name: str = None) -> Dict:
        """Get existing user profile or create new one"""
        
        # Check if user exists
        check_sql = f"""
        SELECT * FROM {self.catalog}.{self.schema}.skillai_user_profiles
        WHERE user_email = '{user_email}'
        """
        
        result = self._execute_sql(check_sql)
        
        if result:
            # Update last_active
            update_sql = f"""
            UPDATE {self.catalog}.{self.schema}.skillai_user_profiles
            SET last_active = current_timestamp(),
                total_interactions = total_interactions + 1
            WHERE user_email = '{user_email}'
            """
            self._execute_sql(update_sql)
            
            user_data = result[0]
            return {
                "user_email": user_data.get("user_email"),
                "user_name": user_data.get("user_name"),
                "role": user_data.get("role", "Team Member"),
                "team": user_data.get("team", "Engineering"),
                "total_interactions": user_data.get("total_interactions", 0),
                "preferences": json.loads(user_data.get("preferences", "{}")),
                "context_data": json.loads(user_data.get("context_data", "{}"))
            }
        else:
            # Create new user
            insert_sql = f"""
            INSERT INTO {self.catalog}.{self.schema}.skillai_user_profiles
            VALUES (
                '{user_email}',
                '{user_name or user_email.split('@')[0]}',
                'Team Member',
                'Engineering',
                current_timestamp(),
                current_timestamp(),
                1,
                '{{}}',
                '{{}}'
            )
            """
            self._execute_sql(insert_sql)
            
            return {
                "user_email": user_email,
                "user_name": user_name or user_email.split('@')[0],
                "role": "Team Member",
                "team": "Engineering",
                "total_interactions": 1,
                "preferences": {},
                "context_data": {}
            }
    
    def save_message(self, conversation_id: str, user_email: str, role: str, content: str, context: Dict = None):
        """Save a chat message to history"""
        import uuid
        
        message_id = str(uuid.uuid4())
        context_json = json.dumps(context or {}).replace("'", "''")
        content_safe = content.replace("'", "''")
        
        insert_sql = f"""
        INSERT INTO {self.catalog}.{self.schema}.skillai_chat_history
        VALUES (
            '{conversation_id}',
            '{user_email}',
            '{message_id}',
            current_timestamp(),
            '{role}',
            '{content_safe}',
            '{context_json}',
            '{{}}'
        )
        """
        
        self._execute_sql(insert_sql)
    
    def get_chat_history(self, conversation_id: str, limit: int = 50) -> List[Dict]:
        """Retrieve chat history for a conversation"""
        
        query_sql = f"""
        SELECT role, content, timestamp
        FROM {self.catalog}.{self.schema}.skillai_chat_history
        WHERE conversation_id = '{conversation_id}'
        ORDER BY timestamp ASC
        LIMIT {limit}
        """
        
        return self._execute_sql(query_sql)
    
    def update_user_activity(self, user_email: str, activity_type: str, data: Dict):
        """Track user activity: sprints, tasks, bugs, deliverables"""
        import uuid
        
        activity_id = str(uuid.uuid4())
        
        sprint_plan = data.get('sprint_plan', '')
        action_items = json.dumps(data.get('action_items', [])).replace("'", "''")
        bugs = json.dumps(data.get('bugs', [])).replace("'", "''")
        deliverables = json.dumps(data.get('deliverables', [])).replace("'", "''")
        status = data.get('status', 'active')
        
        insert_sql = f"""
        INSERT INTO {self.catalog}.{self.schema}.skillai_user_activity
        VALUES (
            '{activity_id}',
            '{user_email}',
            current_timestamp(),
            '{activity_type}',
            '{sprint_plan}',
            '{action_items}',
            '{bugs}',
            '{deliverables}',
            '{status}'
        )
        """
        
        self._execute_sql(insert_sql)
    
    def get_user_activities(self, user_email: str, activity_type: str = None, limit: int = 20) -> List[Dict]:
        """Get user's recent activities"""
        
        where_clause = f"WHERE user_email = '{user_email}'"
        if activity_type:
            where_clause += f" AND activity_type = '{activity_type}'"
        
        query_sql = f"""
        SELECT *
        FROM {self.catalog}.{self.schema}.skillai_user_activity
        {where_clause}
        ORDER BY timestamp DESC
        LIMIT {limit}
        """
        
        return self._execute_sql(query_sql)
    
    def update_user_profile(self, user_email: str, updates: Dict):
        """Update user profile fields"""
        
        set_clauses = []
        
        if 'role' in updates:
            set_clauses.append(f"role = '{updates['role']}'")
        if 'team' in updates:
            set_clauses.append(f"team = '{updates['team']}'")
        if 'preferences' in updates:
            prefs_json = json.dumps(updates['preferences']).replace("'", "''")
            set_clauses.append(f"preferences = '{prefs_json}'")
        if 'context_data' in updates:
            context_json = json.dumps(updates['context_data']).replace("'", "''")
            set_clauses.append(f"context_data = '{context_json}'")
        
        if set_clauses:
            update_sql = f"""
            UPDATE {self.catalog}.{self.schema}.skillai_user_profiles
            SET {', '.join(set_clauses)}
            WHERE user_email = '{user_email}'
            """
            
            self._execute_sql(update_sql)
