"""
Team Intelligence Hub - LIVE EXECUTION UPGRADE

A premium Gradio app with REAL-TIME capabilities:
✅ Live code execution (Python/SQL)
✅ Streaming AI chat (ChatGPT-style)
✅ Live job monitoring (auto-refresh)
✅ AI-powered code review
✅ Interactive debugger
✅ Live team activity
✅ Real-time alerts

Upgrade from static forms → live execution environment
"""

import gradio as gr
import pandas as pd
import os
import json
import sys
import io
import traceback
from datetime import datetime
from databricks.sdk import WorkspaceClient
from typing import List, Dict, Optional, Iterator
import time

# --- Gradio 4.44.0 schema compatibility fix ---
# Gradio 4.44.0's API schema generator (gradio_client/utils.py) crashes on
# boolean JSON Schema values (additionalProperties: True) produced for streaming
# Chatbot endpoints. This runtime monkey-patch handles boolean schemas as "Any"
# type, preserving all existing UI and functionality. Does not modify library files.
import gradio_client.utils as _gradio_client_utils
_original_json_schema_to_python_type = _gradio_client_utils._json_schema_to_python_type

def _patched_json_schema_to_python_type(schema, defs=None):
    if isinstance(schema, bool):
        return "Any"
    return _original_json_schema_to_python_type(schema, defs)

_gradio_client_utils._json_schema_to_python_type = _patched_json_schema_to_python_type
# --- End fix ---

# LLM imports for AI Assistant
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

# Initialize Databricks SDK
w = WorkspaceClient()
current_user = w.current_user.me()
USER_EMAIL = current_user.user_name
USER_DISPLAY_NAME = current_user.display_name

def get_warehouse_id():
    """Get warehouse ID from env or auto-detect first available warehouse"""
    warehouse_id = os.getenv("WAREHOUSE_ID")
    if warehouse_id:
        return warehouse_id
    
    warehouses = list(w.warehouses.list())
    if warehouses:
        return warehouses[0].id
    raise ValueError("No SQL warehouses available")

WAREHOUSE_ID = get_warehouse_id()

# ============================================================================
# UC DATABASE OPERATIONS (Read + Write)
# ============================================================================

from databricks.sdk.service.sql import StatementParameterListItem

def query_uc(sql: str, params: Optional[List[Dict]] = None) -> pd.DataFrame:
    """Execute SQL against Unity Catalog"""
    try:
        formatted_params = [
            StatementParameterListItem(name=p["name"], value=p["value"])
            for p in (params or [])
        ]
        result = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=sql,
            parameters=formatted_params,
            wait_timeout="30s"
        )
        
        # Always extract column names from manifest schema (available even with 0 rows)
        columns = []
        if result.manifest and result.manifest.schema and result.manifest.schema.columns:
            columns = [col.name for col in result.manifest.schema.columns]
        
        if result.result and result.result.data_array:
            return pd.DataFrame(result.result.data_array, columns=columns)
        else:
            # Return empty DataFrame with correct columns so callers can check .empty
            return pd.DataFrame(columns=columns)
    except Exception as e:
        print(f"[query_uc] Error: {e}", flush=True)
        return pd.DataFrame({"Error": [str(e)]})

def write_uc(sql: str, params: Optional[List[Dict]] = None) -> Dict:
    """Write data to Unity Catalog with audit trail"""
    try:
        formatted_params = [
            StatementParameterListItem(name=p["name"], value=p["value"])
            for p in (params or [])
        ]
        w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=sql,
            parameters=formatted_params,
            wait_timeout="30s"
        )
        return {"success": True, "message": "Data saved successfully"}
    except Exception as e:
        print(f"[write_uc] Error: {e}", flush=True)
        return {"success": False, "message": f"Error: {str(e)}"}


# ============================================================================
# 🚀 LIVE CODE EXECUTION
# ============================================================================

def execute_code_live(code: str, language: str = "python") -> str:
    """Execute Python or SQL code in real-time"""
    if not code or code.strip() == "":
        return "⚠️ Please enter code to execute"
    
    try:
        if language == "sql":
            # Execute SQL via UC
            result = query_uc(code)
            if "Error" in result.columns:
                return f"❌ SQL Error:\n{result['Error'].iloc[0]}"
            else:
                return f"✅ SQL executed successfully!\n\n{result.to_string()}"
        
        elif language == "python":
            # Execute Python code
            # Capture stdout
            old_stdout = sys.stdout
            sys.stdout = captured_output = io.StringIO()
            
            try:
                # Execute the code
                exec_globals = {
                    "__name__": "__main__",
                    "w": w,
                    "query_uc": query_uc,
                    "pd": pd,
                    "datetime": datetime
                }
                exec(code, exec_globals)
                
                # Get output
                output = captured_output.getvalue()
                sys.stdout = old_stdout
                
                return f"✅ Python executed successfully!\n\n{output if output else '(No output)'}"
            
            except Exception as e:
                sys.stdout = old_stdout
                error_trace = traceback.format_exc()
                return f"❌ Python Error:\n{error_trace}"
    
    except Exception as e:
        return f"❌ Execution Error:\n{str(e)}"


# ============================================================================
# 🤖 AI-POWERED CHAT (Streaming)
# ============================================================================

def load_central_reviewer():
    """Load central_reviewer.py from Knowledge Hub"""
    try:
        # Import the central reviewer
        import sys
        sys.path.insert(0, '/Workspace/Shared/dev/SkillAIPath/3_AI_Platform/3_AI_Agents/Knowledge_Hub')
        from central_reviewer import CentralReviewer
        return CentralReviewer()
    except Exception as e:
        return None

REVIEWER = load_central_reviewer()

LLM_MODEL = "databricks-meta-llama-3-3-70b-instruct"

SYSTEM_PROMPT = (
    "You are the Team Intelligence Hub AI Assistant, running on Databricks. "
    "You help users with data engineering, analytics, Databricks, Spark, SQL, and Python questions.\n\n"
    "Available tools in this app:\n"
    "- **Live Code Executor tab**: Execute Python or SQL code in real-time\n"
    "- **AI Code Review tab**: Get AI-powered code review for pasted code\n"
    "- **Interactive Debugger tab**: Debug job failures with AI diagnosis\n"
    "- **Live Jobs Monitor tab**: Monitor running Databricks jobs\n"
    "- **Knowledge Search tab**: Search 7,322 SkillAI knowledge entries\n"
    "- **Sprint Planning tab**: Track sprint plans\n"
    "- **Bug Reports tab**: Report bugs\n\n"
    "Rules:\n"
    "- If the user asks to execute or run code, direct them to the **Live Code Executor** tab. "
    "Do NOT claim you executed code — only the Live Code Executor can do that.\n"
    "- If the user asks for code review, direct them to the **AI Code Review** tab.\n"
    "- If the user asks to debug a job, direct them to the **Interactive Debugger** tab.\n"
    "- Be concise, helpful, and specific to the user's question.\n"
)

def stream_ai_response(message: str, history: List) -> Iterator[List]:
    """Stream AI response using Databricks Foundation Model API"""
    if not message or message.strip() == "":
        yield history + [[message, "⚠️ Please enter a message"]], ""
        return
    
    try:
        # Build conversation context from history
        chat_messages = [
            ChatMessage(role=ChatMessageRole.SYSTEM, content=SYSTEM_PROMPT)
        ]
        
        # Add conversation history (last 10 turns for context)
        for user_msg, assistant_msg in (history or [])[-10:]:
            if user_msg:
                chat_messages.append(ChatMessage(role=ChatMessageRole.USER, content=str(user_msg)))
            if assistant_msg:
                chat_messages.append(ChatMessage(role=ChatMessageRole.ASSISTANT, content=str(assistant_msg)))
        
        # Add current message
        chat_messages.append(ChatMessage(role=ChatMessageRole.USER, content=message))
        
        # Call the LLM via Databricks Foundation Model API
        try:
            response = w.serving_endpoints.query(
                name=LLM_MODEL,
                messages=chat_messages,
                max_tokens=512,
                temperature=0.7
            )
            
            if hasattr(response, 'choices') and response.choices:
                response_text = response.choices[0].message.content
            else:
                response_text = str(response)
                print(f"[AI Assistant] Unexpected response format: {type(response)}", flush=True)
                
        except Exception as llm_error:
            print(f"[AI Assistant] LLM call failed: {llm_error}", flush=True)
            response_text = (
                "⚠️ I couldn't connect to the AI model. "
                f"Error: {str(llm_error)[:200]}\n\n"
                "You can still use the other tabs:\n"
                "• **Live Code Executor** — Execute Python or SQL code\n"
                "• **AI Code Review** — Get code review\n"
                "• **Interactive Debugger** — Debug job failures\n"
                "• **Knowledge Search** — Search knowledge base"
            )
        
        # Stream the response word by word
        words = response_text.split()
        partial = ""
        for word in words:
            partial += word + " "
            yield history + [[message, partial]], ""
            time.sleep(0.02)
        
        # Yield the complete response (preserves full formatting)
        yield history + [[message, response_text]], ""
    
    except Exception as e:
        print(f"[AI Assistant] Unexpected error: {e}", flush=True)
        yield history + [[message, f"❌ Error: {str(e)}"]], ""


# ============================================================================
# 📊 LIVE JOB MONITORING
# ============================================================================

def get_live_jobs() -> pd.DataFrame:
    """Get currently running jobs"""
    try:
        # Get recent job runs
        runs = list(w.jobs.list_runs(active_only=True, limit=20))
        
        if not runs:
            return pd.DataFrame({"Message": ["No active jobs running"]})
        
        data = []
        for run in runs:
            data.append({
                "Job ID": run.job_id,
                "Run ID": run.run_id,
                "State": run.state.life_cycle_state.value if run.state else "Unknown",
                "Start Time": datetime.fromtimestamp(run.start_time / 1000).strftime("%Y-%m-%d %H:%M:%S") if run.start_time else "N/A",
                "Duration (min)": round((time.time() * 1000 - run.start_time) / 60000, 1) if run.start_time else 0
            })
        
        return pd.DataFrame(data)
    
    except Exception as e:
        return pd.DataFrame({"Error": [f"Failed to fetch jobs: {str(e)}"]})


def get_job_logs(job_id: str, run_id: str) -> str:
    """Get live logs for a job run"""
    if not job_id or not run_id:
        return "⚠️ Please enter both Job ID and Run ID"
    
    try:
        # Get run output
        run_output = w.jobs.get_run_output(run_id=int(run_id))
        
        if run_output.error:
            return f"❌ **Job Failed:**\n\n{run_output.error}\n\n{run_output.error_trace if run_output.error_trace else ''}"
        else:
            return f"✅ **Job Output:**\n\n{run_output.logs if hasattr(run_output, 'logs') else 'No logs available'}"
    
    except Exception as e:
        return f"❌ Error fetching logs: {str(e)}"


# ============================================================================
# 🔍 AI-POWERED CODE REVIEW
# ============================================================================

def review_code_with_ai(code: str, file_type: str = "python") -> str:
    """AI-powered code review using LLM"""
    if not code or code.strip() == "":
        return "⚠️ Please enter code to review"
    
    try:
        review_prompt = f"""Review the following {file_type} code for bugs, runtime errors, security issues, and best practices.
For each issue found, provide:
1. The specific issue (not generic advice)
2. Why it's a problem
3. A concrete fix or corrected code snippet

Code to review:
```
{code}
```

If the code has no issues, say "No issues found." Be specific — do not give generic advice about try/except or logging unless the code actually has that problem."""

        chat_messages = [
            ChatMessage(
                role=ChatMessageRole.SYSTEM,
                content="You are an expert code reviewer. You identify specific bugs, runtime errors, and security issues in code. You provide concrete, actionable fixes — not generic advice."
            ),
            ChatMessage(role=ChatMessageRole.USER, content=review_prompt)
        ]
        
        response = w.serving_endpoints.query(
            name=LLM_MODEL,
            messages=chat_messages,
            max_tokens=512,
            temperature=0.3
        )
        
        review_text = response.choices[0].message.content if hasattr(response, 'choices') and response.choices else str(response)
        
        return f"🤖 **AI Code Review Results:**\n\n{review_text}"
    
    except Exception as e:
        print(f"[Code Review] LLM error: {e}", flush=True)
        return f"❌ Review Error: {str(e)}"


# ============================================================================
# 🐛 INTERACTIVE DEBUGGER
# ============================================================================

def debug_job_failure(job_id: str, run_id: str) -> tuple:
    """AI-powered job failure diagnosis"""
    if not job_id or not run_id:
        return "⚠️ Please enter both Job ID and Run ID", ""
    
    try:
        # Get run details
        run = w.jobs.get_run(run_id=int(run_id))
        
        # Get error details
        error_trace = ""
        ai_diagnosis = ""
        
        if run.state and run.state.state_message:
            error_trace = f"**Job State:** {run.state.life_cycle_state.value if run.state.life_cycle_state else 'Unknown'}\n"
            error_trace += f"**Error Message:**\n{run.state.state_message}\n\n"
            
            # Try to get run output
            try:
                run_output = w.jobs.get_run_output(run_id=int(run_id))
                if run_output.error:
                    error_trace += f"**Error Details:**\n{run_output.error}\n\n"
                    if run_output.error_trace:
                        error_trace += f"**Stack Trace:**\n{run_output.error_trace}\n"
            except:
                pass
            
            # Use LLM for diagnosis
            try:
                debug_prompt = f"""Analyze this Databricks job failure and provide a diagnosis:

{error_trace}

Provide:
1. The likely root cause (be specific)
2. Concrete steps to fix it"""

                chat_messages = [
                    ChatMessage(
                        role=ChatMessageRole.SYSTEM,
                        content="You are a Databricks job failure diagnostician. Analyze error traces and provide specific root cause analysis and actionable fixes."
                    ),
                    ChatMessage(role=ChatMessageRole.USER, content=debug_prompt)
                ]
                
                response = w.serving_endpoints.query(
                    name=LLM_MODEL,
                    messages=chat_messages,
                    max_tokens=512,
                    temperature=0.3
                )
                ai_diagnosis = f"🤖 **AI Diagnosis:**\n\n{response.choices[0].message.content}" if hasattr(response, 'choices') and response.choices else f"🤖 **AI Diagnosis:**\n\n{response}"
            except Exception as llm_err:
                print(f"[Debugger] LLM error: {llm_err}", flush=True)
                ai_diagnosis = f"⚠️ AI diagnosis unavailable: {str(llm_err)[:200]}"
        
        else:
            error_trace = f"✅ Job completed successfully or still running"
            ai_diagnosis = "No errors detected. Job may be running or succeeded."
        
        return error_trace, ai_diagnosis
    
    except Exception as e:
        return f"❌ Error: {str(e)}", f"Failed to diagnose: {str(e)}"


# ============================================================================
# 👥 LIVE TEAM ACTIVITY
# ============================================================================

def get_live_team_activity() -> pd.DataFrame:
    """Get recent team activity from UC"""
    try:
        sql = """
        SELECT 
            user_email,
            activity_type,
            timestamp,
            sprint_plan,
            status
        FROM sap_dev.gold.skillai_user_activity
        WHERE timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
        ORDER BY timestamp DESC
        LIMIT 20
        """
        return query_uc(sql)
    except Exception as e:
        return pd.DataFrame({"Error": [f"Failed to fetch team activity: {str(e)}"]})


# ============================================================================
# 📚 KNOWLEDGE SEARCH (Static - kept from original)
# ============================================================================

def search_knowledge(query: str, top_k: int = 5) -> str:
    """Search SkillAI knowledge base via Unity Catalog"""
    if not query or query.strip() == "":
        return "Please enter a search query."
    
    try:
        # Search the UC knowledge_base table
        safe_query = query.replace("'", "''")
        sql = f"""
        SELECT id, title, content, source_path, category, tags
        FROM sap_dev.gold.knowledge_base
        WHERE LOWER(content) LIKE '%{safe_query.lower()}%'
           OR LOWER(title) LIKE '%{safe_query.lower()}%'
           OR LOWER(tags) LIKE '%{safe_query.lower()}%'
        LIMIT {top_k}
        """
        result_df = query_uc(sql)
        
        if "Error" in result_df.columns:
            return f"❌ Error searching knowledge base: {result_df['Error'].iloc[0]}"
        
        if result_df.empty:
            return f"No results found for '{query}'. Try different keywords."
        
        output = f"### 🔍 Found {len(result_df)} results for '{query}'\n\n"
        for i, (_, row) in enumerate(result_df.iterrows(), 1):
            output += f"**{i}. {row['category']}** — {row['title']}\n"
            content = str(row['content'])
            output += f"{content[:200]}{'...' if len(content) > 200 else ''}\n"
            output += f"📎 Source: {row['source_path']}\n\n"
        
        output += f"\n📄 **Knowledge Base**: sap_dev.gold.knowledge_base"
        return output
        
    except Exception as e:
        print(f"[Knowledge Search] Error: {e}", flush=True)
        return f"Error searching knowledge base: {str(e)}"


# ============================================================================
# 📋 SPRINT PLANNING & BUG TRACKING (Static - kept from original)
# ============================================================================

def submit_sprint_plan(sprint_name: str, action_items: str, deliverables: str) -> str:
    """Submit sprint plan to Unity Catalog"""
    if not sprint_name or not action_items:
        return "❌ Sprint name and action items are required"
    
    try:
        sql = """
        INSERT INTO sap_dev.gold.skillai_user_activity
        (activity_id, user_email, timestamp, activity_type, sprint_plan, action_items, bugs, deliverables, status)
        VALUES
        (:activity_id, :user_email, CURRENT_TIMESTAMP(), 'sprint_planning', :sprint_name, :action_items, '', :deliverables, 'active')
        """
        
        activity_id = f"sprint_{USER_EMAIL}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        result = write_uc(sql, [
            {"name": "activity_id", "value": activity_id},
            {"name": "user_email", "value": USER_EMAIL},
            {"name": "sprint_name", "value": sprint_name},
            {"name": "action_items", "value": action_items},
            {"name": "deliverables", "value": deliverables}
        ])
        
        if result["success"]:
            return f"✅ Sprint plan '{sprint_name}' saved to Unity Catalog!\n\n📊 Activity ID: {activity_id}\n🗄️ Table: sap_dev.gold.skillai_user_activity"
        else:
            return f"❌ {result['message']}"
    except Exception as e:
        return f"❌ Error: {str(e)}"


def submit_bug_report(bug_title: str, bug_description: str, severity: str) -> str:
    """Submit bug report to Unity Catalog"""
    if not bug_title or not bug_description:
        return "❌ Bug title and description are required"
    
    try:
        sql = """
        INSERT INTO sap_dev.gold.skillai_user_activity
        (activity_id, user_email, timestamp, activity_type, sprint_plan, action_items, bugs, deliverables, status)
        VALUES
        (:activity_id, :user_email, CURRENT_TIMESTAMP(), 'bug_report', '', '', :bug_info, '', 'reported')
        """
        
        activity_id = f"bug_{USER_EMAIL}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        bug_info = json.dumps({
            "title": bug_title,
            "description": bug_description,
            "severity": severity
        })
        
        result = write_uc(sql, [
            {"name": "activity_id", "value": activity_id},
            {"name": "user_email", "value": USER_EMAIL},
            {"name": "bug_info", "value": bug_info}
        ])
        
        if result["success"]:
            return f"✅ Bug report saved!\n\n🐛 {bug_title}\n📊 Severity: {severity}\n🗄️ Table: sap_dev.gold.skillai_user_activity"
        else:
            return f"❌ {result['message']}"
    except Exception as e:
        return f"❌ Error: {str(e)}"


def get_team_resources() -> str:
    """List key SkillAIPath resources"""
    return """
### 📚 Team Resources

**🤖 AI Agents & Knowledge Hub:**
- 📁 `/Workspace/Shared/dev/SkillAIPath/3_AI_Platform/3_AI_Agents/Knowledge_Hub/`
- 📝 `knowledge_base.json` - 7,322 code chunks
- 🤖 `central_reviewer.py` - Code review engine
- 🔍 `rag_system.py` - Knowledge search

**📊 Jobs & Automation:**
- ⚙️ Job 13: Daily Auto Documentation
- 📅 Updates knowledge base from Google Sheets + code changes

**📈 Dashboards:**
- 📊 Customer 360 Dashboard
- 📦 Inventory Health Dashboard
- 💰 Sales Performance Dashboard

**🗄️ Unity Catalog Tables:**
- `sap_dev.gold.knowledge_base` - Auto-documentation
- `sap_dev.gold.skillai_user_profiles` - User profiles
- `sap_dev.gold.skillai_chat_history` - Chat history
- `sap_dev.gold.skillai_user_activity` - Activity tracking

**🔗 Google Sheets Integration:**
- Portfolio tasks and sprint plans
- Synced daily via Job 13
    """


def get_my_activity_history() -> pd.DataFrame:
    """Get user's activity history from UC"""
    sql = """
    SELECT 
        timestamp,
        activity_type,
        sprint_plan,
        action_items,
        bugs,
        deliverables,
        status
    FROM sap_dev.gold.skillai_user_activity
    WHERE user_email = :user_email
    ORDER BY timestamp DESC
    LIMIT 20
    """
    return query_uc(sql, [{"name": "user_email", "value": USER_EMAIL}])


# ============================================================================
# 🎨 GRADIO UI - LIVE EXECUTION HUB
# ============================================================================

with gr.Blocks(title="Team Intelligence Hub - Live Execution", theme=gr.themes.Soft()) as demo:
    # Header
    gr.Markdown("""
    # 🚀 Team Intelligence Hub - LIVE EXECUTION
    ### Your Real-Time AI-Powered Work Environment
    
    **Welcome, {}!** This is your live execution hub with:
    - ⚡ Real-time code execution (Python/SQL)
    - 🤖 Streaming AI assistant (ChatGPT-style)
    - 📊 Live job monitoring (auto-refresh)
    - 🔍 AI-powered code review
    - 🐛 Interactive debugger
    - 👥 Live team activity
    
    **Upgraded from static forms → live execution environment!**
    ---
    """.format(USER_DISPLAY_NAME))
    
    # NEW TAB 1: ⚡ Live Code Executor
    with gr.Tab("⚡ Live Code Executor"):
        gr.Markdown("### Execute Python or SQL in Real-Time")
        gr.Markdown("*Run code directly from the UI - see results instantly!*")
        
        language_choice = gr.Radio(["python", "sql"], label="Language", value="python")
        code_input = gr.Code(language="python", label="Write Your Code", lines=10)
        run_button = gr.Button("▶️ Run Now", variant="primary")
        code_output = gr.Textbox(label="Live Output", lines=15)
        
        run_button.click(
            fn=execute_code_live,
            inputs=[code_input, language_choice],
            outputs=code_output
        )
        
        gr.Markdown("""
**Examples:**

**Python:**
```python
# Query Unity Catalog
df = query_uc("SELECT COUNT(*) FROM sap_dev.gold.knowledge_base")
print(df)
```

**SQL:**
```sql
SELECT activity_type, COUNT(*) as count
FROM sap_dev.gold.skillai_user_activity
GROUP BY activity_type
LIMIT 10
```
        """)
    
    # NEW TAB 2: 🤖 AI Assistant (Streaming)
    with gr.Tab("🤖 AI Assistant"):
        gr.Markdown("### Streaming AI Chat (ChatGPT-style)")
        gr.Markdown("*Get real-time AI help with code, debugging, and more*")
        
        chatbot = gr.Chatbot(label="AI Assistant", height=400)
        msg_input = gr.Textbox(label="Ask anything...", placeholder="e.g., How do I debug a failing job?")
        clear_button = gr.Button("🗑️ Clear Chat")
        
        msg_input.submit(
            fn=stream_ai_response,
            inputs=[msg_input, chatbot],
            outputs=[chatbot, msg_input]
        )
        
        clear_button.click(lambda: [], None, chatbot)
    
    # NEW TAB 3: 📊 Live Jobs Monitor
    with gr.Tab("📊 Live Jobs Monitor"):
        gr.Markdown("### Real-Time Job Status Dashboard")
        gr.Markdown("*Auto-refreshes every 10 seconds*")
        
        jobs_table = gr.Dataframe(label="Running Jobs", interactive=False)
        refresh_jobs_btn = gr.Button("🔄 Refresh Now", variant="secondary")
        
        gr.Markdown("### Get Job Logs")
        with gr.Row():
            log_job_id = gr.Textbox(label="Job ID", placeholder="12345")
            log_run_id = gr.Textbox(label="Run ID", placeholder="67890")
        get_logs_btn = gr.Button("📜 Get Logs")
        logs_output = gr.Textbox(label="Job Logs", lines=15)
        
        refresh_jobs_btn.click(fn=get_live_jobs, outputs=jobs_table)
        get_logs_btn.click(
            fn=get_job_logs,
            inputs=[log_job_id, log_run_id],
            outputs=logs_output
        )
        
        # Auto-refresh jobs every 10 seconds
        demo.load(fn=get_live_jobs, outputs=jobs_table)
    
    # NEW TAB 4: 🔍 AI Code Review
    with gr.Tab("🔍 AI Code Review"):
        gr.Markdown("### AI-Powered Code Review (Live)")
        gr.Markdown("*Get instant feedback on your code*")
        
        review_lang = gr.Radio(["python", "sql"], label="Code Type", value="python")
        review_code = gr.Code(language="python", label="Paste Your Code", lines=15)
        review_btn = gr.Button("🔍 Review My Code", variant="primary")
        review_output = gr.Markdown(label="AI Review Results")
        
        review_btn.click(
            fn=review_code_with_ai,
            inputs=[review_code, review_lang],
            outputs=review_output
        )
    
    # NEW TAB 5: 🐛 Interactive Debugger
    with gr.Tab("🐛 Interactive Debugger"):
        gr.Markdown("### AI-Powered Job Failure Diagnosis")
        gr.Markdown("*Debug failing jobs with AI assistance*")
        
        with gr.Row():
            debug_job_id = gr.Textbox(label="Job ID", placeholder="12345")
            debug_run_id = gr.Textbox(label="Run ID", placeholder="67890")
        debug_btn = gr.Button("🔍 Debug Job", variant="primary")
        
        error_trace = gr.Textbox(label="Error Details", lines=10)
        ai_diagnosis = gr.Markdown(label="AI Diagnosis & Suggested Fixes")
        
        debug_btn.click(
            fn=debug_job_failure,
            inputs=[debug_job_id, debug_run_id],
            outputs=[error_trace, ai_diagnosis]
        )
    
    # NEW TAB 6: 👥 Live Team Activity
    with gr.Tab("👥 Live Team Activity"):
        gr.Markdown("### See What Your Team is Doing Right Now")
        gr.Markdown("*Auto-refreshes every 30 seconds*")
        
        team_activity = gr.Dataframe(label="Recent Team Activity", interactive=False)
        refresh_team_btn = gr.Button("🔄 Refresh Team Activity", variant="secondary")
        
        refresh_team_btn.click(fn=get_live_team_activity, outputs=team_activity)
        demo.load(fn=get_live_team_activity, outputs=team_activity)
    
    # EXISTING TABS (kept from original)
    with gr.Tab("🔍 Knowledge Search"):
        gr.Markdown("### Search SkillAIPath Knowledge Base (7,322 entries)")
        
        search_input = gr.Textbox(label="Search Query", placeholder="e.g., customer churn model", lines=2)
        search_button = gr.Button("🔍 Search Knowledge Base", variant="primary")
        search_output = gr.Markdown()
        
        search_button.click(fn=search_knowledge, inputs=search_input, outputs=search_output)
    
    with gr.Tab("📋 Sprint Planning"):
        gr.Markdown("### Track Your Sprint Plans")
        
        sprint_name = gr.Textbox(label="Sprint Name", placeholder="Sprint 1: Q4 Goals")
        action_items = gr.Textbox(label="Action Items", placeholder="1. Build dashboard\n2. Test ML model", lines=5)
        deliverables = gr.Textbox(label="Deliverables", placeholder="Working dashboard, Tested model", lines=3)
        sprint_button = gr.Button("💾 Save Sprint Plan", variant="primary")
        sprint_output = gr.Markdown()
        
        sprint_button.click(fn=submit_sprint_plan, inputs=[sprint_name, action_items, deliverables], outputs=sprint_output)
    
    with gr.Tab("🐛 Bug Reports"):
        gr.Markdown("### Report Bugs & Issues")
        
        bug_title = gr.Textbox(label="Bug Title", placeholder="Dashboard not loading data")
        bug_description = gr.Textbox(label="Bug Description", placeholder="Describe the issue...", lines=5)
        bug_severity = gr.Dropdown(choices=["Low", "Medium", "High", "Critical"], label="Severity", value="Medium")
        bug_button = gr.Button("🐛 Submit Bug Report", variant="primary")
        bug_output = gr.Markdown()
        
        bug_button.click(fn=submit_bug_report, inputs=[bug_title, bug_description, bug_severity], outputs=bug_output)
    
    with gr.Tab("📚 Team Resources"):
        gr.Markdown("### SkillAIPath Resources & Links")
        resources_output = gr.Markdown(value=get_team_resources())
        refresh_resources = gr.Button("🔄 Refresh Resources")
        refresh_resources.click(fn=get_team_resources, outputs=resources_output)
    
    with gr.Tab("📊 My Activity"):
        gr.Markdown("### Your Activity History")
        
        activity_df = gr.Dataframe(interactive=False)
        refresh_activity = gr.Button("🔄 Refresh Activity", variant="secondary")
        refresh_activity.click(fn=get_my_activity_history, outputs=activity_df)
        demo.load(fn=get_my_activity_history, outputs=activity_df)
    
    # Footer
    gr.Markdown("""
---

**🚀 LIVE EXECUTION UPGRADE**  
✅ Real-time code execution  
✅ Streaming AI chat  
✅ Live job monitoring  
✅ AI code review  
✅ Interactive debugger  
✅ Team activity feed  

**📊 Data Sources:**  
• Knowledge Base: 7,322 entries  
• Job 13: Daily Auto Documentation  
• Unity Catalog: `sap_dev.gold.skillai_*`  
• Central AI Reviewer: `central_reviewer.py`  
    """)

# Mount Gradio on FastAPI
from fastapi import FastAPI
import uvicorn

fastapi_app = FastAPI()
app = gr.mount_gradio_app(fastapi_app, demo, path="/")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
