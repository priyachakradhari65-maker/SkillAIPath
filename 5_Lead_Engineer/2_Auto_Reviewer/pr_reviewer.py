"""
Senior-Level PR Reviewer with Architecture Intelligence
Reviews like a 10x engineer - comprehensive, context-aware, no AI slop
"""

import re
from typing import Dict, List, Optional

try:
    from rag_query import get_knowledge_base
    RAG_LOADED = True
except:
    RAG_LOADED = False

try:
    from context_store import get_context_for_review
    CONTEXT_LOADED = True
except:
    CONTEXT_LOADED = False


def review_pr_code(file_path: str, code: str, pr_context: Dict = None) -> Dict:
    """Main entry point for PR review with feedback learning"""
    pr_context = pr_context or {}
    
    reviewer = SeniorReviewer()
    return reviewer.review(
        file_path=file_path,
        code=code,
        pr_title=pr_context.get("title", ""),
        pr_id=pr_context.get("pr_id", ""),
        author=pr_context.get("author", ""),
        all_files=pr_context.get("all_files", []),
        pr_description=pr_context.get("description", ""),
        pr_url=pr_context.get("url", "")
    )


class SeniorReviewer:
    """
    Reviews like a senior engineer:
    - Architecture-level thinking (not just syntax)
    - Context-aware (learns from codebase patterns)
    - Comprehensive (no arbitrary limits)
    - Deduplicated (doesn't repeat itself)
    """
    
    def __init__(self):
        self.kb = get_knowledge_base() if RAG_LOADED else None
    
    def review(self, file_path: str, code: str, pr_title: str, pr_id: str, 
               author: str, all_files: List[str] = None, pr_description: str = "",
               pr_url: str = "") -> Dict:
        """Comprehensive review with architecture intelligence"""
        
        all_comments = []
        
        # Parse PR context to skip intentional decisions
        pr_context = None
        if self.kb and pr_description:
            pr_context = self.kb.parse_pr_context(pr_description, pr_title)
        
        # Detect domain for context
        text = (code + file_path + pr_title).lower()
        domain = self._detect_domain(text)
        
        # 1. Code-level reviews (existing checks)
        if file_path.endswith((".py", ".ipynb")):
            all_comments.extend(self._review_python(code, file_path, domain))
        elif file_path.endswith(".sql"):
            all_comments.extend(self._review_sql(code))
        
        # 2. SECURITY SCANNING (NEW!)
        all_comments.extend(self._review_security(code, file_path))
        
        # 3. SPARK PERFORMANCE (NEW!)
        all_comments.extend(self._review_spark_performance(code))
        
        # 4. ARCHITECTURE-LEVEL REVIEWS (NEW!)
        all_comments.extend(self._review_architecture(code, file_path, all_files or []))
        
        # 5. SYSTEM DESIGN REVIEWS (NEW!)
        all_comments.extend(self._review_system_design(code, file_path, pr_title))
        
        # 6. RAG-POWERED CONTEXT
        if self.kb:
            all_comments = self._enrich_with_rag(all_comments, file_path, author, domain)
        
        # 7. GENERATE AUTO-FIX SUGGESTIONS
        all_comments = self._add_autofixes(all_comments, code)
        
        # 8. FILTER OUT REJECTED PATTERNS (LEARNING!)
        if self.kb:
            all_comments = self._filter_rejected_patterns(all_comments)
        
        # 9. SKIP INTENTIONAL DECISIONS FROM PR CONTEXT
        if pr_context and pr_context.get("skip_patterns"):
            all_comments = self._filter_by_pr_context(all_comments, pr_context)
        
        # 10. INTELLIGENT PRIORITIZATION (NO HARD LIMITS!)
        prioritized_comments = self._prioritize_and_filter(all_comments, file_path)
        
        # 9. BUILD SENIOR-LEVEL FEEDBACK
        feedback = self._build_senior_feedback(file_path, prioritized_comments)
        
        # 9.5 ADD TEAM CONTEXT (Situational Awareness!)
        if CONTEXT_LOADED and author:
            context_note = get_context_for_review(author, all_files or [])
            if context_note:
                feedback += context_note
        
        # 10. LOG TO RAG FOR LEARNING
        if self.kb and pr_id:
            for comment in prioritized_comments[:20]:  # Log top issues
                self.kb.log_review(
                    pr_id=pr_id,
                    pr_title=pr_title,
                    file_path=file_path,
                    issue_type=comment.get("type", "unknown"),
                    issue_text=comment["text"],
                    severity=comment.get("severity", "important"),
                    line_number=comment.get("line", 1),
                    author=author
                )
        
        # Build inline comments for GitHub
        inline_comments = [
            {"path": file_path, "line": c["line"], "body": c["text"]}
            for c in prioritized_comments
        ]
        
        return {
            "status": "NEEDS_REVIEW" if prioritized_comments else "LOOKS_GOOD",
            "feedback": feedback,
            "inline_comments": inline_comments,
            "file_path": file_path
        }
    
    def _detect_domain(self, text: str) -> str:
        """Detect domain/project from code content"""
        if any(k in text for k in ["edufin", "cibil", "dpd", "loan", "bureau"]):
            return "edufin"
        elif any(k in text for k in ["sap", "rfc", "bapi", "s4hana"]):
            return "sap"
        else:
            return "general"
    
    def _review_python(self, code: str, file_path: str, domain: str) -> List[Dict]:
        """Python code review - same as before but with types for prioritization"""
        comments = []
        lines = code.split('\n')
        
        # 1. No error handling (CRITICAL)
        io_lines = []
        for i, line in enumerate(lines, 1):
            if any(op in line for op in ["spark.read", "spark.sql", "requests.get", "open("]):
                in_try = any("try:" in lines[max(0, j-1)] for j in range(max(0, i-10), i))
                if not in_try:
                    io_lines.append(i)
        
        if io_lines:
            comments.append({
                "line": io_lines[0],
                "type": "error_handling",
                "severity": "critical",
                "text": f"Line {io_lines[0]}: This I/O call will crash the entire pipeline if the data source is down. Wrap it in a try/except so you can log the error and either retry or fail gracefully. Have you tested what happens when this table doesn't exist?"
            })
        
        # 2. Hardcoded credentials (CRITICAL)
        creds = []
        for i, line in enumerate(lines, 1):
            if any(kw in line.lower() for kw in ["password", "api_key", "secret", "token"]) and "=" in line:
                if not any(env in line for env in ["os.environ", "dbutils.secrets"]):
                    creds.append(i)
        
        if creds:
            comments.append({
                "line": creds[0],
                "type": "security",
                "severity": "critical",
                "text": f"Line {creds[0]}: This looks like a hardcoded credential. Are you committing this to Git? Use os.environ or dbutils.secrets.get() instead so credentials don't end up in version control."
            })
        
        # 3. .collect() without limit (CRITICAL)
        collect_lines = [i for i, line in enumerate(lines, 1) if ".collect()" in line]
        has_limit = any(".limit(" in line for line in lines)
        
        if collect_lines and not has_limit:
            comments.append({
                "line": collect_lines[0],
                "type": "performance",
                "severity": "critical",
                "text": f"Line {collect_lines[0]}: Calling .collect() here pulls all rows into driver memory. What's the max size of this dataset in production? If it's more than a few thousand rows, this will crash with OOM. Use .limit(1000) or switch to .show() if you're just spot-checking."
            })
        
        # 4. No monitoring (IMPORTANT)
        has_monitoring = any(kw in code for kw in ["log.info", "logger", "print(", "mlflow.log_metric"])
        if not has_monitoring and len(code) > 200:
            comments.append({
                "line": 1,
                "type": "monitoring",
                "severity": "important",
                "text": "I don't see any logging or metrics here. When this runs in production, how will you know if it succeeded? How will you track how many rows were processed or if it took longer than expected? Add some logging so you're not flying blind."
            })
        
        # 5. No tests (IMPORTANT)
        has_tests = any(kw in code for kw in ["assert ", "test_", "unittest"])
        is_orchestration = "orchestrat" in file_path.lower()
        
        if not has_tests and not is_orchestration and len(code) > 100:
            comments.append({
                "line": 1,
                "type": "testing",
                "severity": "important",
                "text": "There are no unit tests for this logic. How do you know it handles null values correctly? Or empty datasets? Or schema changes? At minimum, add a few assertions to validate the output shape and key column values."
            })
        
        # 6. No data quality checks (IMPORTANT)
        is_pipeline = any(kw in file_path.lower() for kw in ["pipeline", "etl", "fact", "dim"])
        has_dq = any(kw in code for kw in ["expectation", "validate", "check_"])
        
        if is_pipeline and not has_dq and len(code) > 200:
            comments.append({
                "line": 1,
                "type": "data_quality",
                "severity": "important",
                "text": "This looks like a data pipeline but I don't see any data quality checks. What happens if the schema changes? Or if someone upstream starts writing nulls? Add some basic checks like row count thresholds or null percentage validation."
            })
        
        # 7. No idempotency (IMPORTANT)
        is_data_write = any(kw in code for kw in ["write.mode", "saveAsTable", "INSERT"])
        has_idempotency = any(kw in code for kw in ["overwrite", "merge", "CREATE OR REPLACE"])
        
        if is_data_write and not has_idempotency:
            comments.append({
                "line": 1,
                "type": "idempotency",
                "severity": "important",
                "text": "You're writing data but I don't see any overwrite or merge logic. What happens if this job runs twice? Will it append duplicate rows? Make it idempotent so reruns are safe."
            })
        
        # 8. Domain-specific: EduFin CIBIL (IMPORTANT)
        if domain == "edufin":
            cibil_lines = [i for i, line in enumerate(lines, 1) if "cibil" in line.lower()]
            has_null_handling = any(kw in code for kw in ["fillna", "coalesce", "isNull"])
            
            if cibil_lines and not has_null_handling:
                comments.append({
                    "line": cibil_lines[0],
                    "type": "domain_specific",
                    "severity": "important",
                    "text": f"Line {cibil_lines[0]}: You're using CIBIL scores here but about 30% of our EduFin data has null CIBIL values. What's your strategy for handling those? Are you filtering them out? Imputing? Right now this will produce incorrect results for a third of your data."
                })
        
        # 9. No partitioning (NICE_TO_HAVE)
        is_large = any(kw in file_path.lower() for kw in ["fact", "transaction"])
        has_partition = "partitionBy" in code or "PARTITIONED BY" in code.upper()
        
        if is_large and not has_partition and len(code) > 200:
            comments.append({
                "line": 1,
                "type": "optimization",
                "severity": "nice_to_have",
                "text": "This looks like a large fact table but there's no partitioning. Queries against this will do full table scans. Consider partitioning by date or another high-cardinality column based on how you'll query it."
            })
        
        # 10. No schema definition (NICE_TO_HAVE)
        is_bronze = "bronze" in file_path.lower()
        has_schema = any(kw in code for kw in ["schema", "StructType"])
        
        if is_bronze and not has_schema and len(code) > 100:
            comments.append({
                "line": 1,
                "type": "schema",
                "severity": "nice_to_have",
                "text": "Bronze layer files should define explicit schemas. If the source adds a column or changes a type, this will break silently. Define the expected schema upfront so you catch changes early."
            })
        
        return comments
    
    def _review_sql(self, code: str) -> List[Dict]:
        """SQL review with severity classification"""
        comments = []
        lines = code.split('\n')
        code_upper = code.upper()
        
        # UPDATE/DELETE without WHERE (CRITICAL)
        for i, line in enumerate(lines, 1):
            if ("UPDATE" in line.upper() or "DELETE" in line.upper()) and "WHERE" not in code_upper:
                comments.append({
                    "line": i,
                    "type": "sql_safety",
                    "severity": "critical",
                    "text": f"Line {i}: This UPDATE/DELETE has no WHERE clause, so it'll modify every row in the table. Is that intentional? If not, add a WHERE clause. If yes, add a comment explaining why you're touching the whole table."
                })
                break
        
        # No incremental logic (IMPORTANT)
        is_fact = "fact" in code.lower()
        has_incremental = any(kw in code_upper for kw in ["WHERE", "MERGE"])
        
        if is_fact and not has_incremental:
            comments.append({
                "line": 1,
                "type": "incremental_load",
                "severity": "important",
                "text": "This looks like a fact table load but there's no incremental logic. Are you reprocessing all historical data every run? That won't scale. Add a WHERE clause to filter by date or implement a MERGE for incremental updates."
            })
        
        return comments
    
    def _review_architecture(self, code: str, file_path: str, all_files: List[str]) -> List[Dict]:
        """
        ARCHITECTURE-LEVEL REVIEW
        This is what makes it senior - looking at system design, not just lines
        """
        comments = []
        
        # 1. Layer violations
        if "bronze" in file_path.lower():
            # Bronze should not reference silver/gold
            if any(layer in code.lower() for layer in ["silver.", "gold."]):
                comments.append({
                    "line": 1,
                    "type": "architecture",
                    "severity": "critical",
                    "text": "This bronze layer is referencing silver/gold tables. That's a layer violation - bronze should only read raw sources. Your data lineage is going backwards. Either move this logic to silver, or remove the cross-layer dependency."
                })
        
        elif "silver" in file_path.lower():
            # Silver should not reference gold
            if "gold." in code.lower():
                comments.append({
                    "line": 1,
                    "type": "architecture",
                    "severity": "important",
                    "text": "Silver layer referencing gold layer - that's unusual. Silver should transform bronze into clean data; gold should aggregate silver for analytics. Are you creating a circular dependency here?"
                })
        
        # 2. Too many dependencies
        table_refs = re.findall(r'FROM\s+([a-z_]+\.[a-z_]+\.[a-z_]+)', code, re.IGNORECASE)
        table_refs += re.findall(r'JOIN\s+([a-z_]+\.[a-z_]+\.[a-z_]+)', code, re.IGNORECASE)
        unique_tables = set(table_refs)
        
        if len(unique_tables) > 8:
            comments.append({
                "line": 1,
                "type": "architecture",
                "severity": "important",
                "text": f"This file joins {len(unique_tables)} different tables. That's a lot of coupling. When any of those tables change, this breaks. Consider if you can break this into multiple steps, or if some of those joins should happen upstream."
            })
        
        # 3. Missing abstraction layer
        if file_path.endswith(".py") and "spark.sql(" in code:
            sql_count = code.count("spark.sql(")
            if sql_count > 5:
                comments.append({
                    "line": 1,
                    "type": "architecture",
                    "severity": "nice_to_have",
                    "text": f"You have {sql_count} inline spark.sql() calls. That's hard to test and maintain. Consider extracting SQL to separate .sql files or building a query builder class."
                })
        
        return comments
    
    def _review_system_design(self, code: str, file_path: str, pr_title: str) -> List[Dict]:
        """
        SYSTEM DESIGN REVIEW
        Scalability, fault tolerance, observability
        """
        comments = []
        
        # 1. No retry logic for external calls
        has_external_call = any(kw in code for kw in ["requests.", "http", "api", "rfc", "bapi"])
        has_retry = any(kw in code for kw in ["retry", "backoff", "tenacity"])
        
        if has_external_call and not has_retry and len(code) > 100:
            comments.append({
                "line": 1,
                "type": "fault_tolerance",
                "severity": "important",
                "text": "You're calling an external service but there's no retry logic. Network hiccups happen. Add exponential backoff retries (e.g., tenacity library) so transient failures don't fail the whole pipeline."
            })
        
        # 2. No timeout on blocking calls
        if "requests.get(" in code or "requests.post(" in code:
            if "timeout=" not in code:
                comments.append({
                    "line": 1,
                    "type": "fault_tolerance",
                    "severity": "critical",
                    "text": "HTTP requests without timeout will hang forever if the remote service is slow. Add timeout=30 (or whatever makes sense) so your pipeline doesn't get stuck."
                })
        
        # 3. Large broadcast join risk
        if any(kw in code for kw in ["broadcast(", "BROADCAST("]):
            comments.append({
                "line": 1,
                "type": "scalability",
                "severity": "important",
                "text": "You're using broadcast join. That's great for small dimension tables but will crash if the broadcasted table grows beyond driver memory. Add a check: if this table ever exceeds 2GB, this join will fail. Is there a safeguard?"
            })
        
        # 4. No circuit breaker for dependencies
        if "pipeline" in file_path.lower() or "orchestrat" in file_path.lower():
            has_validation = any(kw in code for kw in ["validate", "check", "assert"])
            if not has_validation:
                comments.append({
                    "line": 1,
                    "type": "fault_tolerance",
                    "severity": "important",
                    "text": "This pipeline has no validation of upstream dependencies. What if the input table is empty? Or has bad data? Add circuit breakers: validate row counts, freshness, schema before proceeding. Fail fast if upstream is broken."
                })
        
        return comments
    
    def _enrich_with_rag(self, comments: List[Dict], file_path: str, 
                         author: str, domain: str) -> List[Dict]:
        """
        Use RAG to add context and deduplicate
        """
        enriched = []
        
        # Check developer history
        dev_history = self.kb.get_developer_history(author)
        repeat_issues = {h["issue_type"]: h["times_flagged"] for h in dev_history}
        
        for comment in comments:
            issue_type = comment.get("type", "unknown")
            
            # Check if we've flagged this exact issue before in this file
            history = self.kb.query_pr_history(file_path, issue_type, days_back=90)
            
            if history:
                # Add reference to past PR
                past_pr = history[0]
                comment["text"] += f"\n\n(📎 Similar issue in PR #{past_pr['pr_id']} - "
                if past_pr.get("resolved"):
                    comment["text"] += f"was fixed {past_pr.get('resolution_notes', 'successfully')})"
                else:
                    comment["text"] += "still unresolved)"
            
            # Adjust tone if developer has seen this many times
            times_flagged = repeat_issues.get(issue_type, 0)
            if times_flagged >= 3:
                comment["text"] += f"\n\n(⚠️ This is the {times_flagged}th time we've discussed this pattern. Let's make it stick this time.)"
            
            # Check if violates codebase pattern
            pattern_violations = self.kb.check_pattern_violation(comment["text"], domain)
            if pattern_violations:
                pattern = pattern_violations[0]
                comment["text"] += f"\n\n(📚 This violates our team convention: {pattern['pattern_name']} - seen in {pattern['frequency']} other files)"
            
            enriched.append(comment)
        
        return enriched
    
    def _review_security(self, code: str, file_path: str) -> List[Dict]:
        """
        SECURITY SCANNING
        Detects secrets, SQL injection, unsafe imports, path traversal
        """
        comments = []
        lines = code.split('\n')
        
        # 1. Hardcoded secrets (high-entropy strings, AWS keys, tokens)
        for i, line in enumerate(lines, 1):
            # API keys, tokens, passwords
            if re.search(r'["\']([A-Za-z0-9/+=]{40,})["\']', line):
                # Skip if it's an environment variable or secret manager
                if not any(safe in line for safe in ['os.environ', 'dbutils.secrets', 'getenv', 'SECRET', 'TOKEN']):
                    comments.append({
                        "line": i,
                        "type": "security_secrets",
                        "severity": "critical",
                        "text": f"Line {i}: This looks like a hardcoded secret or API key (high-entropy string). NEVER commit credentials. Use environment variables or secret management."
                    })
            
            # AWS/GCP keys
            if re.search(r'(AKIA[0-9A-Z]{16}|AIza[0-9A-Za-z\\-_]{35})', line):
                comments.append({
                    "line": i,
                    "type": "security_secrets",
                    "severity": "critical",
                    "text": f"Line {i}: AWS/GCP API key detected. This MUST be revoked immediately and moved to secure storage. Credentials in code = security breach."
                })
        
        # 2. SQL injection risk (f-strings or + in SQL)
        for i, line in enumerate(lines, 1):
            if any(sql in line.upper() for sql in ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'WHERE']):
                # Check for string formatting in SQL
                if 'f"' in line or "f'" in line or '.format(' in line or '" + ' in line or "' + " in line:
                    comments.append({
                        "line": i,
                        "type": "security_sql_injection",
                        "severity": "critical",
                        "text": f"Line {i}: SQL query with string interpolation = SQL injection risk. Use parameterized queries or spark.sql with proper escaping. An attacker can inject arbitrary SQL here."
                    })
        
        # 3. Unsafe deserialization
        if 'pickle.load' in code or 'yaml.load(' in code:
            for i, line in enumerate(lines, 1):
                if 'pickle.load' in line and 'pickle.load' not in '# ' + line:  # Not a comment
                    comments.append({
                        "line": i,
                        "type": "security_deserialization",
                        "severity": "critical",
                        "text": f"Line {i}: pickle.load() can execute arbitrary code. Only unpickle data from trusted sources. Consider safer formats like JSON or parquet."
                    })
                if 'yaml.load(' in line and 'yaml.safe_load' not in line:
                    comments.append({
                        "line": i,
                        "type": "security_deserialization",
                        "severity": "critical",
                        "text": f"Line {i}: yaml.load() is unsafe - allows arbitrary code execution. Use yaml.safe_load() instead."
                    })
        
        # 4. Path traversal
        if 'open(' in code or 'read_csv' in code:
            for i, line in enumerate(lines, 1):
                if ('open(' in line or 'read_csv' in line) and ('input(' in code or 'request.' in code):
                    if not any(safe in code for safe in ['os.path.basename', 'Path(', 'validate']):
                        comments.append({
                            "line": i,
                            "type": "security_path_traversal",
                            "severity": "important",
                            "text": f"Line {i}: File path from user input without validation = path traversal vulnerability. User could access ../../../etc/passwd. Validate and sanitize paths."
                        })
        
        # 5. Unsafe shell execution
        if 'os.system' in code or 'subprocess.call' in code or 'eval(' in code or 'exec(' in code:
            for i, line in enumerate(lines, 1):
                if 'os.system' in line or 'subprocess.call' in line:
                    comments.append({
                        "line": i,
                        "type": "security_command_injection",
                        "severity": "critical",
                        "text": f"Line {i}: Shell command execution with user input = command injection risk. Use subprocess.run() with shell=False and a list of args, never shell=True with strings."
                    })
                if 'eval(' in line or 'exec(' in line:
                    comments.append({
                        "line": i,
                        "type": "security_code_execution",
                        "severity": "critical",
                        "text": f"Line {i}: eval()/exec() = arbitrary code execution. This is almost never justified. Refactor to avoid dynamic code execution."
                    })
        
        return comments
    
    def _review_spark_performance(self, code: str) -> List[Dict]:
        """
        SPARK-SPECIFIC PERFORMANCE ANTI-PATTERNS
        Catches common PySpark mistakes that kill performance
        """
        comments = []
        
        # 1. Cartesian join
        if 'crossJoin' in code or re.search(r'join\([^,]+\)(?!.*on)', code, re.IGNORECASE):
            comments.append({
                "line": 1,
                "type": "spark_cartesian_join",
                "severity": "critical",
                "text": "Cartesian join detected - this creates rows = left_count × right_count. If you have 1M rows on each side, that's 1 trillion rows. Are you sure? Add a join condition or use broadcast if intentional."
            })
        
        # 2. Missing broadcast
        join_count = len(re.findall(r'\.(join|leftJoin|rightJoin)\(', code))
        broadcast_count = code.count('broadcast(') + code.count('BROADCAST(')
        
        if join_count > 0 and broadcast_count == 0:
            comments.append({
                "line": 1,
                "type": "spark_broadcast",
                "severity": "important",
                "text": f"You have {join_count} joins but no broadcast hints. If any of these are small dimension tables (<2GB), broadcast them to avoid shuffling."
            })
        
        # 3. Multiple collects
        collect_count = code.count('.collect()')
        if collect_count > 2:
            comments.append({
                "line": 1,
                "type": "spark_multiple_collects",
                "severity": "important",
                "text": f"You're calling .collect() {collect_count} times. Each one pulls data to the driver and blocks parallelism. Can you batch these or use Spark transformations instead?"
            })
        
        # 4. UDF without return type
        if '@udf' in code or 'udf(' in code:
            if not any(ret in code for ret in ['returnType=', 'StringType', 'IntegerType', 'DoubleType']):
                comments.append({
                    "line": 1,
                    "type": "spark_udf_no_type",
                    "severity": "important",
                    "text": "UDF without explicit return type = Spark uses slow reflection. Add returnType=StringType() for 2-10x speedup."
                })
        
        # 5. No caching for reused DataFrames
        df_names = re.findall(r'(\w+)\s*=.*(?:spark\.read|spark\.sql|select|filter|join)', code)
        df_usage = {}
        for df in df_names:
            df_usage[df] = code.count(df)
        
        reused_dfs = [df for df, count in df_usage.items() if count > 3]
        has_cache = '.cache()' in code or '.persist()' in code
        
        if reused_dfs and not has_cache:
            comments.append({
                "line": 1,
                "type": "spark_no_cache",
                "severity": "important",
                "text": f"DataFrame {reused_dfs[0]} used {df_usage[reused_dfs[0]]} times without caching. Spark will recompute from scratch each time. Add .cache() after expensive transformations."
            })
        
        return comments
    
    def _add_autofixes(self, comments: List[Dict], code: str) -> List[Dict]:
        """
        GENERATE AUTO-FIX SUGGESTIONS
        Not just "this is wrong" - here's the corrected code
        """
        for comment in comments:
            issue_type = comment.get('type', '')
            line_num = comment.get('line', 1)
            
            # Get the problematic line
            try:
                lines = code.split('\n')
                if 1 <= line_num <= len(lines):
                    problem_line = lines[line_num - 1].strip()
                    autofix = self._generate_fix(issue_type, problem_line)
                    
                    if autofix:
                        comment['text'] += f"\n\n**Auto-fix:**\n```python\n{autofix}\n```"
            except:
                pass
        
        return comments
    
    def _generate_fix(self, issue_type: str, problem_line: str) -> Optional[str]:
        """Generate fix for common issues"""
        
        # Fix: Add try/except for I/O
        if issue_type == 'error_handling':
            return f"""try:
    {problem_line}
except Exception as e:
    logger.error(f"Failed: {{e}}")
    raise"""
        
        # Fix: Use environment variable for secrets
        if 'security' in issue_type:
            if 'password' in problem_line.lower():
                return "import os\npassword = os.environ.get('DB_PASSWORD')"
        
        # Fix: Add .limit() before .collect()
        if issue_type == 'performance' and '.collect()' in problem_line:
            return problem_line.replace('.collect()', '.limit(1000).collect()')
        
        # Fix: Add broadcast
        if issue_type == 'spark_broadcast' and 'join(' in problem_line:
            match = re.search(r'join\(([^,\)]+)', problem_line)
            if match:
                table = match.group(1).strip()
                return f"from pyspark.sql.functions import broadcast\n{problem_line.replace(f'join({table}', f'join(broadcast({table})')}"
        
        return None
    
    def _filter_rejected_patterns(self, comments: List[Dict]) -> List[Dict]:
        """
        FEEDBACK LEARNING: Skip patterns the team consistently removes
        """
        if not self.kb:
            return comments
            
        filtered = []
        
        for comment in comments:
            issue_type = comment.get('type', '')
            
            # Check if this pattern is consistently rejected
            try:
                rejected = self.kb.query_rejected_patterns(issue_type, threshold=2)
                
                if rejected:
                    # Skip this suggestion - team doesn't find it valuable
                    continue
            except:
                pass  # On error, keep the suggestion
            
            filtered.append(comment)
        
        return filtered
    
    def _filter_by_pr_context(self, comments: List[Dict], pr_context: Dict) -> List[Dict]:
        """
        Skip suggestions for intentional design decisions mentioned in PR description
        """
        filtered = []
        skip_patterns = pr_context.get("skip_patterns", [])
        
        if not skip_patterns:
            return comments
        
        for comment in comments:
            should_skip = False
            comment_text = comment.get('text', '').lower()
            issue_type = comment.get('type', '')
            
            for skip_pattern in skip_patterns:
                # Check if this comment is about something intentionally skipped
                if skip_pattern['category'] == 'team_agreement':
                    if 'test' in issue_type and 'test' in skip_pattern.get('phrase', ''):
                        should_skip = True
                        break
                elif skip_pattern['category'] == 'known_issue':
                    if issue_type in comment_text:
                        should_skip = True
                        break
                elif skip_pattern['category'] == 'design_decision':
                    # Respect explicit design decisions
                    should_skip = True
                    break
            
            if not should_skip:
                filtered.append(comment)
        
        return filtered
    
    def _prioritize_and_filter(self, comments: List[Dict], file_path: str) -> List[Dict]:
        """
        INTELLIGENT PRIORITIZATION - NO HARD LIMITS!
        
        Rules:
        - ALL critical issues returned (no limit)
        - Top 10 important issues
        - Top 5 nice-to-have issues
        
        Sort within each tier by impact/complexity
        """
        critical = []
        important = []
        nice_to_have = []
        
        for c in comments:
            severity = c.get("severity", "important")
            
            if severity == "critical":
                critical.append(c)
            elif severity == "important":
                important.append(c)
            else:
                nice_to_have.append(c)
        
        # Return comprehensive coverage
        result = critical + important[:10] + nice_to_have[:5]
        
        return result
    
    def _build_senior_feedback(self, file_path: str, comments: List[Dict]) -> str:
        """Build senior-level feedback"""
        if not comments:
            return f"**{file_path}**\n\n✅ Solid work. Architecture looks clean, no major concerns."
        
        output = f"**{file_path}**\n\n"
        
        # Group by severity
        critical = [c for c in comments if c.get("severity") == "critical"]
        important = [c for c in comments if c.get("severity") == "important"]
        nice_to_have = [c for c in comments if c.get("severity") == "nice_to_have"]
        
        if critical:
            output += "### 🚨 Critical Issues\n"
            for i, c in enumerate(critical, 1):
                output += f"{i}. {c['text']}\n\n"
        
        if important:
            output += "### ⚠️  Important\n"
            for i, c in enumerate(important, 1):
                output += f"{i}. {c['text']}\n\n"
        
        if nice_to_have:
            output += "### 💡 Nice to Have\n"
            for i, c in enumerate(nice_to_have, 1):
                output += f"{i}. {c['text']}\n\n"
        
        return output
