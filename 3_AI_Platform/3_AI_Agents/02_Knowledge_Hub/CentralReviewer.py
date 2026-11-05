"""
Central AI Agent for Code Review
Used by: PR Reviewer, SAP Dashboard, Intelligence Hub

One reviewer logic for all apps - trained on your feedback patterns
"""

import re
from typing import Dict, List, Tuple


class CentralReviewer:
    """Central AI Agent - Common-sense code reviewer"""
    
    def __init__(self, rag_system=None):
        self.rag = rag_system
    
    def review_code(self, file_path: str, code: str, context: Dict = None) -> Dict:
        """Main review entry point"""
        context = context or {}
        
        return self.review(
            file_path=file_path,
            code=code,
            pr_title=context.get("title", ""),
            author=context.get("author", ""),
            project_type=context.get("project_type", "")
        )
    
    def review(self, file_path: str, code: str, pr_title: str = "", author: str = "", project_type: str = "") -> Dict:
        """Review with logic and common sense"""
        
        comments = []
        
        # Detect domain from context
        text = (code + file_path + pr_title).lower()
        is_edufin = any(k in text for k in ["edufin", "cibil", "dpd", "loan"])
        is_sap = "sap" in text or project_type == "sap"
        is_pipeline = any(k in file_path.lower() for kw in ["pipeline", "etl", "bronze", "silver", "gold", "fact", "dim"])
        is_orchestration = "orchestrat" in file_path.lower()
        
        # Run common-sense checks
        if file_path.endswith(".py") or file_path.endswith(".ipynb"):
            comments = self._check_python_logic(code, file_path, is_edufin, is_sap, is_pipeline, is_orchestration)
        elif file_path.endswith(".sql"):
            comments = self._check_sql_logic(code, is_pipeline)
        
        # Limit to 10 per file
        comments = comments[:10]
        
        feedback = self._build_feedback(file_path, comments)
        
        inline_comments = [
            {"path": file_path, "line": c["line"], "body": c["text"]}
            for c in comments
        ]
        
        return {
            "status": "NEEDS_REVIEW" if comments else "LOOKS_GOOD",
            "feedback": feedback,
            "inline_comments": inline_comments,
            "file_path": file_path
        }
    
    def _check_python_logic(self, code: str, file_path: str, is_edufin: bool, is_sap: bool, is_pipeline: bool, is_orchestration: bool) -> List[Dict]:
        """Check Python code logic and common sense"""
        comments = []
        lines = code.split('\n')
        
        # 1. I/O without error handling
        io_operations = []
        for i, line in enumerate(lines, 1):
            if any(op in line for op in ["spark.read", "spark.sql", "requests.get", "requests.post", "open("]):
                in_try = False
                for j in range(max(0, i-10), i):
                    if "try:" in lines[j-1]:
                        in_try = True
                        break
                
                if not in_try:
                    op_name = "this I/O operation"
                    if "spark.read" in line:
                        op_name = "this Spark read"
                    elif "requests." in line:
                        op_name = "this API call"
                    elif "open(" in line:
                        op_name = "this file read"
                    
                    io_operations.append((i, op_name))
        
        if io_operations:
            line, op_name = io_operations[0]
            comments.append({
                "line": line,
                "text": f"Line {line}: What happens when {op_name} fails? Network issues, table doesn't exist, API timeout - you need to handle these or your pipeline will crash at 3am. Wrap in try/except with logging."
            })
        
        # 2. GroupBy without aggregation
        for i, line in enumerate(lines, 1):
            if ".groupBy(" in line:
                has_agg = False
                for j in range(i, min(i+5, len(lines))):
                    if any(agg in lines[j] for agg in [".agg(", ".count()", ".sum(", ".avg(", ".max(", ".min("]):
                        has_agg = True
                        break
                
                if not has_agg:
                    comments.append({
                        "line": i,
                        "text": f"Line {i}: You're grouping but not aggregating. What's the business logic here? Without .agg() or .count(), this will just return the first row per group - is that what you want?"
                    })
        
        # 3. Memory logic: .collect() without limit
        for i, line in enumerate(lines, 1):
            if ".collect()" in line:
                has_limit = False
                for j in range(max(0, i-5), i+1):
                    if ".limit(" in lines[j-1] or "LIMIT" in lines[j-1].upper():
                        has_limit = True
                        break
                
                if not has_limit:
                    comments.append({
                        "line": i,
                        "text": f"Line {i}: You're pulling all data into memory with .collect(). How many rows could this be in production? If it's more than a few thousand, you'll OOM. Do you actually need all rows, or can you use .limit() or .take()?"
                    })
        
        # 4. Observability
        has_logging = any(kw in code for kw in ["logger.", "log.", "print(", "mlflow.log", "statsd"])
        
        if not has_logging and not is_orchestration and len(code) > 200:
            comments.append({
                "line": 1,
                "text": "When this runs in production and fails, how will you debug it? No logging means you're blind. At minimum, log: start/end, row counts, key column values."
            })
        
        # 5. Testing
        has_tests = any(kw in code for kw in ["assert", "test_", "unittest", "pytest", "expect"])
        
        if not has_tests and not is_orchestration and len(code) > 150:
            comments.append({
                "line": 1,
                "text": "Have you tested this with: empty dataset? All nulls? Schema change? Duplicate keys? Add basic assertions so you catch issues before production."
            })
        
        # 6. EduFin: CIBIL NULL handling
        if is_edufin:
            cibil_lines = []
            for i, line in enumerate(lines, 1):
                if "cibil" in line.lower():
                    has_null_check = any(kw in code.lower() for kw in ["fillna", "coalesce", "isnull", "isnotnull", "na.fill", "na.drop"])
                    if not has_null_check:
                        cibil_lines.append(i)
            
            if cibil_lines:
                comments.append({
                    "line": cibil_lines[0],
                    "text": f"Line {cibil_lines[0]}: In EduFin data, about 30% of CIBIL scores are null (thin-file customers, errors). Right now your logic will either fail or give wrong results for those rows. What's your business rule - exclude them? Impute? Flag as high-risk?"
                })
        
        # 7. SAP: Connection pooling
        if is_sap:
            for i, line in enumerate(lines, 1):
                if "pyrfc" in line.lower() or "rfc_connection" in line.lower():
                    has_pool = "pool" in code.lower() or "context" in code.lower()
                    if not has_pool:
                        comments.append({
                            "line": i,
                            "text": f"Line {i}: SAP RFC connections are expensive. Are you opening a new connection for each call? Use connection pooling or context managers to reuse connections."
                        })
                        break
        
        # 8. Pipeline: Idempotency
        if is_pipeline:
            has_write = any(kw in code for kw in ["write.", "saveAsTable", "insertInto", "INSERT"])
            has_overwrite = any(kw in code for kw in ["overwrite", "merge", "MERGE", "CREATE OR REPLACE"])
            
            if has_write and not has_overwrite:
                comments.append({
                    "line": 1,
                    "text": "Pipeline writes should be idempotent. If this job runs twice (retry, manual rerun), what happens? Will you get duplicate rows? Use .mode('overwrite') or MERGE to make reruns safe."
                })
            
            has_dq = any(kw in code for kw in ["expectation", "validate", "assert", "check_", "WHERE", "filter"])
            
            if not has_dq and len(code) > 200:
                comments.append({
                    "line": 1,
                    "text": "Data pipelines need quality checks. What if upstream sends: all nulls? Duplicate keys? Schema changes? Add basic validations - row count thresholds, null %, key uniqueness."
                })
        
        # 9. Security: Credentials
        for i, line in enumerate(lines, 1):
            if any(kw in line.lower() for kw in ["password", "api_key", "secret", "token"]):
                if "=" in line and not any(safe in line for safe in ["os.environ", "dbutils.secrets", "getenv"]):
                    if '"' in line or "'" in line:
                        comments.append({
                            "line": i,
                            "text": f"Line {i}: This looks like a hardcoded credential. Is this going into Git? Anyone with repo access can see it. Use dbutils.secrets.get() or environment variables."
                        })
                        break
        
        return comments
    
    def _check_sql_logic(self, code: str, is_pipeline: bool) -> List[Dict]:
        """Check SQL logic"""
        comments = []
        lines = code.split('\n')
        code_upper = code.upper()
        
        # 1. Destructive ops without WHERE
        for i, line in enumerate(lines, 1):
            line_upper = line.upper()
            if "UPDATE" in line_upper or "DELETE" in line_upper:
                has_where = False
                for j in range(i-1, min(i+5, len(lines))):
                    if "WHERE" in lines[j].upper():
                        has_where = True
                        break
                
                if not has_where:
                    comments.append({
                        "line": i,
                        "text": f"Line {i}: This UPDATE/DELETE has no WHERE clause - it will modify EVERY row in the table. Is that intentional? If not, add WHERE. If yes, add a comment explaining why."
                    })
                    break
        
        # 2. Full table scans
        if is_pipeline:
            has_where = "WHERE" in code_upper
            has_incremental = any(kw in code_upper for kw in ["MERGE", "INCREMENTAL"])
            is_fact = "fact" in code.lower()
            
            if is_fact and not has_where and not has_incremental:
                comments.append({
                    "line": 1,
                    "text": "Fact table load with no WHERE clause means you're reprocessing all history every run. As data grows, this query will get slower and cost more. Add a date filter or use MERGE for incremental loads."
                })
        
        # 3. Join duplication
        join_count = code_upper.count(" JOIN ")
        has_distinct = "DISTINCT" in code_upper or "GROUP BY" in code_upper
        
        if join_count > 1 and not has_distinct:
            comments.append({
                "line": 1,
                "text": f"You have {join_count} joins with no DISTINCT or GROUP BY. Have you checked if this creates duplicate rows? Run a quick row count before/after to verify."
            })
        
        return comments
    
    def _build_feedback(self, file_path: str, comments: List[Dict]) -> str:
        """Build feedback"""
        if not comments:
            return f"**{file_path}**\n\nLooks good."
        
        output = f"**{file_path}**\n\n"
        for i, c in enumerate(comments, 1):
            output += f"{i}. {c['text']}\n\n"
        
        return output


def review_pr_code(file_path: str, code: str, pr_context: Dict = None) -> Dict:
    """Entry point for all apps"""
    reviewer = CentralReviewer()
    return reviewer.review_code(file_path, code, pr_context or {})
