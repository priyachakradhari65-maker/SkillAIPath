"""
RAG Query System for PR Reviewer
Queries Unity Catalog tables to provide context-aware, intelligent reviews

Refactored to use Databricks SDK instead of SparkSession for Apps V2 compatibility.
"""

import os
import json
from typing import List, Dict, Optional
from datetime import datetime, timedelta

try:
    from databricks.sdk import WorkspaceClient
    SDK_AVAILABLE = True
except ImportError:
    SDK_AVAILABLE = False


def get_knowledge_base():
    """Initialize knowledge base connection"""
    return KnowledgeBase()


class KnowledgeBase:
    """Queries Unity Catalog tables for PR review intelligence
    
    Uses Databricks SDK to execute SQL without requiring SparkSession.
    Storage failures never block PR reviews.
    """
    
    def __init__(self):
        self.enabled = False
        self.w = None
        self.warehouse_id = None
        
        if not SDK_AVAILABLE:
            print("⚠️ KnowledgeBase: Databricks SDK not available")
            return
        
        try:
            self.w = WorkspaceClient()
            self.warehouse_id = os.environ.get('DATABRICKS_WAREHOUSE_ID')
            if not self.warehouse_id:
                warehouses = list(self.w.warehouses.list())
                if warehouses:
                    self.warehouse_id = warehouses[0].id
                    print(f"ℹ️ KnowledgeBase: Using warehouse {self.warehouse_id}")
            
            if self.warehouse_id:
                self.enabled = True
                print("✅ KnowledgeBase: Initialized successfully")
            else:
                print("⚠️ KnowledgeBase: No SQL warehouse available")
        except Exception as e:
            print(f"⚠️ KnowledgeBase: Initialization failed: {str(e)[:200]}")
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
    
    def query_pr_history(self, file_path: str, issue_type: str, days_back: int = 90) -> List[Dict]:
        """
        Check if similar issue was flagged before in this file
        Returns list of past occurrences with PR IDs
        """
        if not self.enabled:
            return []
        
        try:
            cutoff = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
            # Escape single quotes for SQL
            safe_path = file_path.replace("'", "''")
            safe_type = issue_type.replace("'", "''")
            
            sql = f"""
                SELECT 
                    pr_id,
                    pr_title,
                    issue_text,
                    severity,
                    line_number,
                    author,
                    resolved,
                    resolution_notes,
                    timestamp
                FROM sap_dev.pr_reviewer.pr_history
                WHERE file_path = '{safe_path}'
                  AND issue_type = '{safe_type}'
                  AND timestamp >= '{cutoff}'
                ORDER BY timestamp DESC
                LIMIT 5
            """
            
            return self._execute_sql(sql)
        except Exception as e:
            print(f"⚠️ Could not query pr_history: {str(e)[:200]}")
            return []
    
    def query_codebase_patterns(self, domain: str = None, pattern_type: str = None) -> List[Dict]:
        """
        Get learned patterns from codebase
        Returns conventions, best practices, and architecture rules
        """
        if not self.enabled:
            return []
        
        try:
            where_clauses = ["frequency >= 5", "confidence_score >= 0.7"]
            
            if domain:
                safe_domain = domain.replace("'", "''")
                where_clauses.append(f"domain = '{safe_domain}'")
            
            if pattern_type:
                safe_type = pattern_type.replace("'", "''")
                where_clauses.append(f"pattern_type = '{safe_type}'")
            
            where_sql = " AND ".join(where_clauses)
            
            sql = f"""
                SELECT 
                    pattern_id,
                    pattern_type,
                    pattern_name,
                    pattern_text,
                    example_code,
                    frequency,
                    confidence_score,
                    domain
                FROM sap_dev.pr_reviewer.codebase_patterns
                WHERE {where_sql}
                ORDER BY frequency DESC, confidence_score DESC
                LIMIT 20
            """
            
            return self._execute_sql(sql)
        except Exception as e:
            print(f"⚠️ Could not query codebase_patterns: {str(e)[:200]}")
            return []
    
    def get_developer_history(self, developer: str) -> List[Dict]:
        """
        Get past feedback for this developer
        Used to personalize tone and avoid nagging
        """
        if not self.enabled:
            return []
        
        try:
            df = self.spark.sql(f"""
                SELECT 
                    issue_type,
                    times_flagged,
                    last_flagged,
                    resolved_count,
                    still_unresolved_count,
                    improvement_trend,
                    notes
                FROM sap_dev.pr_reviewer.developer_feedback
                WHERE developer = '{developer}'
                  AND times_flagged >= 2
                ORDER BY times_flagged DESC
            """)
            
            rows = df.collect()
            return [row.asDict() for row in rows]
        except Exception as e:
            print(f"Warning: Could not query developer_feedback: {e}")
            return []
    
    def check_pattern_violation(self, code: str, domain: str = None) -> List[Dict]:
        """
        Check if code violates learned architecture patterns
        Returns list of violated patterns with examples
        """
        if not self.enabled:
            return []
        
        try:
            patterns = self.query_codebase_patterns(domain=domain, pattern_type='anti_pattern')
            violations = []
            
            code_lower = code.lower()
            for pattern in patterns:
                # Simple substring check (could be enhanced with regex)
                pattern_indicators = pattern.get('pattern_text', '').lower().split()
                if any(indicator in code_lower for indicator in pattern_indicators[:5]):
                    violations.append(pattern)
            
            return violations
        except:
            return []
    
    def get_architecture_rules(self, domain: str = None) -> List[Dict]:
        """
        Get architecture-level rules learned from codebase
        Returns rules about layering, dependencies, data flow
        """
        if not self.enabled:
            return []
        
        return self.query_codebase_patterns(domain=domain, pattern_type='architecture_rule')
    
    def log_review(self, pr_id: str, pr_title: str, file_path: str, 
                   issue_type: str, issue_text: str, severity: str, 
                   line_number: int, author: str):
        """
        Log this review to pr_history for future learning
        Called after each review is completed
        
        CRITICAL: This must NEVER block PR review posting.
        Storage failures are logged but do not propagate.
        """
        if not self.enabled:
            return
        
        try:
            now = datetime.now()
            
            # Escape single quotes for SQL
            safe_pr_id = pr_id.replace("'", "''")
            safe_title = pr_title.replace("'", "''")[:500] if pr_title else ""
            safe_path = file_path.replace("'", "''")
            safe_type = issue_type.replace("'", "''")
            safe_text = issue_text.replace("'", "''")[:2000]  # Limit size
            safe_severity = severity.replace("'", "''")
            safe_author = author.replace("'", "''")
            
            # Ensure table exists (CREATE IF NOT EXISTS is idempotent)
            create_table_sql = """
                CREATE TABLE IF NOT EXISTS sap_dev.pr_reviewer.pr_history (
                    pr_id STRING NOT NULL,
                    pr_title STRING,
                    file_path STRING NOT NULL,
                    issue_type STRING NOT NULL,
                    issue_text STRING NOT NULL,
                    severity STRING NOT NULL,
                    line_number INT,
                    author STRING NOT NULL,
                    resolved BOOLEAN,
                    resolution_notes STRING,
                    timestamp TIMESTAMP NOT NULL,
                    review_date DATE NOT NULL
                )
                USING DELTA
                PARTITIONED BY (review_date)
                COMMENT 'PR review history for learning'
            """
            self._execute_sql(create_table_sql)
            
            # Insert the review
            insert_sql = f"""
                INSERT INTO sap_dev.pr_reviewer.pr_history 
                (pr_id, pr_title, file_path, issue_type, issue_text, severity, line_number, author, resolved, resolution_notes, timestamp, review_date)
                VALUES (
                    '{safe_pr_id}',
                    '{safe_title}',
                    '{safe_path}',
                    '{safe_type}',
                    '{safe_text}',
                    '{safe_severity}',
                    {line_number if line_number else 'NULL'},
                    '{safe_author}',
                    FALSE,
                    NULL,
                    '{now.isoformat()}',
                    '{now.date().isoformat()}'
                )
            """
            self._execute_sql(insert_sql)
            
        except Exception as e:
            print(f"⚠️ Could not log review (non-blocking): {str(e)[:300]}")
    
    def parse_pr_context(self, pr_description: str, pr_title: str) -> Dict[str, any]:
        """
        Parse PR description for context that should affect review
        Returns dict with: design_decisions, known_issues, team_agreements
        """
        context = {
            "design_decisions": [],
            "known_issues": [],
            "skip_patterns": []
        }
        
        if not pr_description:
            return context
        
        desc_lower = (pr_description + " " + pr_title).lower()
        
        # Common phrases that indicate intentional decisions
        skip_indicators = [
            ("intentionally", "design_decision"),
            ("by design", "design_decision"),
            ("known issue", "known_issue"),
            ("tech debt", "known_issue"),
            ("will fix later", "known_issue"),
            ("agreed with team", "team_agreement"),
            ("discussed offline", "team_agreement"),
            ("temporary", "temporary_code"),
            ("workaround for", "workaround"),
        ]
        
        for phrase, category in skip_indicators:
            if phrase in desc_lower:
                context["skip_patterns"].append({
                    "phrase": phrase,
                    "category": category,
                    "context": desc_lower[max(0, desc_lower.index(phrase)-50):desc_lower.index(phrase)+100]
                })
        
        return context
    
    def save_feedback(self, pr_id: str, pr_url: str, file_path: str, 
                     issue_type: str, original_comment: str, 
                     edited_comment: Optional[str], action: str, reviewer: str,
                     pr_context: str = ""):
        """
        Log what the reviewer did with the generated comment
        Actions: 'kept' (unchanged), 'edited' (modified), 'removed' (deleted)
        This is how we learn what suggestions are helpful vs noise
        """
        if not self.enabled:
            return
        
        try:
            from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DateType
            
            now = datetime.now()
            
            schema = StructType([
                StructField("pr_id", StringType(), False),
                StructField("pr_url", StringType(), True),
                StructField("file_path", StringType(), False),
                StructField("issue_type", StringType(), False),
                StructField("original_comment", StringType(), False),
                StructField("edited_comment", StringType(), True),
                StructField("action", StringType(), False),
                StructField("reviewer", StringType(), False),
                StructField("pr_context", StringType(), True),
                StructField("timestamp", TimestampType(), False),
                StructField("feedback_date", DateType(), False),
            ])
            
            data = [(
                pr_id,
                pr_url,
                file_path,
                issue_type,
                original_comment,
                edited_comment,
                action,
                reviewer,
                pr_context,
                now,
                now.date()
            )]
            
            # Ensure table exists
            self.spark.sql("""
                CREATE TABLE IF NOT EXISTS sap_dev.pr_reviewer.review_feedback_loop (
                    pr_id STRING NOT NULL,
                    pr_url STRING,
                    file_path STRING NOT NULL,
                    issue_type STRING NOT NULL,
                    original_comment STRING NOT NULL,
                    edited_comment STRING,
                    action STRING NOT NULL,
                    reviewer STRING NOT NULL,
                    pr_context STRING,
                    timestamp TIMESTAMP NOT NULL,
                    feedback_date DATE NOT NULL
                )
                USING DELTA
                PARTITIONED BY (feedback_date)
                COMMENT 'Tracks reviewer edits to learn what suggestions to skip'
            """)
            
            df = self.spark.createDataFrame(data, schema)
            df.write.mode("append").saveAsTable("sap_dev.pr_reviewer.review_feedback_loop")
            
        except Exception as e:
            print(f"Warning: Could not save feedback: {e}")
    
    def store_completed_review(self, pr_url: str, pr_number: int, repo: str, 
                               author: str, changed_files: List[str], 
                               review_feedback: str, review_status: str,
                               inline_comments_count: int) -> bool:
        """
        Store the complete PR review AFTER successful GitHub posting.
        
        This is called ONLY when the review has been successfully posted to GitHub.
        Storage failures are logged but do not propagate - they NEVER block the review flow.
        
        Args:
            pr_url: Full GitHub PR URL
            pr_number: PR number
            repo: Repository name (e.g., "owner/repo")
            author: PR author
            changed_files: List of files changed in PR
            review_feedback: Generated review text
            review_status: "NEEDS_REVIEW" or "LOOKS_GOOD"
            inline_comments_count: Number of inline comments posted
        
        Returns:
            True if stored successfully, False otherwise (non-blocking)
        """
        if not self.enabled:
            return False
        
        try:
            now = datetime.now()
            
            # Escape single quotes for SQL
            safe_url = pr_url.replace("'", "''")
            safe_repo = repo.replace("'", "''")
            safe_author = author.replace("'", "''")
            safe_feedback = review_feedback.replace("'", "''")[:5000]  # Limit size
            safe_status = review_status.replace("'", "''")
            safe_files = json.dumps(changed_files).replace("'", "''")
            
            # Ensure table exists
            create_table_sql = """
                CREATE TABLE IF NOT EXISTS sap_dev.pr_reviewer.completed_reviews (
                    pr_url STRING NOT NULL,
                    pr_number INT NOT NULL,
                    repository STRING NOT NULL,
                    author STRING NOT NULL,
                    changed_files STRING,
                    review_feedback STRING,
                    review_status STRING NOT NULL,
                    inline_comments_count INT,
                    posted_at TIMESTAMP NOT NULL,
                    review_date DATE NOT NULL
                )
                USING DELTA
                PARTITIONED BY (review_date)
                COMMENT 'Successfully posted PR reviews for learning and context'
            """
            self._execute_sql(create_table_sql)
            
            # Insert the completed review
            insert_sql = f"""
                INSERT INTO sap_dev.pr_reviewer.completed_reviews 
                (pr_url, pr_number, repository, author, changed_files, review_feedback, review_status, inline_comments_count, posted_at, review_date)
                VALUES (
                    '{safe_url}',
                    {pr_number},
                    '{safe_repo}',
                    '{safe_author}',
                    '{safe_files}',
                    '{safe_feedback}',
                    '{safe_status}',
                    {inline_comments_count},
                    '{now.isoformat()}',
                    '{now.date().isoformat()}'
                )
            """
            self._execute_sql(insert_sql)
            print(f"✅ Stored completed review for PR #{pr_number}")
            return True
            
        except Exception as e:
            print(f"⚠️ Could not store completed review (non-blocking): {str(e)[:300]}")
            return False
    
    def query_rejected_patterns(self, issue_type: str, threshold: int = 2) -> List[Dict]:
        """
        Find patterns that reviewers consistently remove or edit
        If an issue_type has been removed 2+ times, we should stop suggesting it
        Returns list of patterns to skip
        """
        if not self.enabled:
            return []
        
        try:
            safe_type = issue_type.replace("'", "''")
            sql = f"""
                WITH rejection_counts AS (
                    SELECT 
                        issue_type,
                        action,
                        COUNT(*) as count,
                        COLLECT_SET(SUBSTRING(original_comment, 1, 100)) as sample_comments
                    FROM sap_dev.pr_reviewer.review_feedback_loop
                    WHERE issue_type = '{safe_type}'
                      AND action IN ('removed', 'edited')
                      AND feedback_date >= DATE_SUB(CURRENT_DATE(), 90)
                    GROUP BY issue_type, action
                )
                SELECT 
                    issue_type,
                    action,
                    count,
                    sample_comments
                FROM rejection_counts
                WHERE count >= {threshold}
                ORDER BY count DESC
            """
            
            return self._execute_sql(sql)
        except Exception as e:
            print(f"⚠️ Could not query rejected patterns: {str(e)[:200]}")
            return []
