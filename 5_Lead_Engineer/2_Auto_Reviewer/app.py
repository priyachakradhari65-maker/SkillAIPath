"""
PR Reviewer - Readable Format with Line Numbers
"""

import os
import json
import gradio as gr
import requests
from fastapi import FastAPI
from typing import Dict, List
from pr_reviewer import review_pr_code
from context_store import store_context, get_team_summary_display
from rag_query import get_knowledge_base

webhook_app = FastAPI()
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")

import re

PR_URL_HELP = (
    "Please enter a valid Pull Request URL.\n\n"
    "Example:\n"
    "https://github.com/owner/repo/pull/24"
)

def parse_pr_url(pr_url: str):
    """Robustly parse a GitHub PR URL.

    Accepts:
      https://github.com/owner/repo/pull/24
      https://github.com/owner/repo/pull/24/files
      github.com/owner/repo/pull/24

    Raises ValueError with a friendly message for anything else,
    including bare repo URLs (https://github.com/owner/repo) or
    PR-list URLs (https://github.com/owner/repo/pulls).
    """
    if not pr_url or not isinstance(pr_url, str) or not pr_url.strip():
        raise ValueError(PR_URL_HELP)

    url = pr_url.strip()

    if "github.com" not in url:
        raise ValueError(PR_URL_HELP)

    match = re.search(r"github\.com/([^/\s]+)/([^/\s]+)/pull/(\d+)", url)
    if not match:
        raise ValueError(PR_URL_HELP)

    owner, repo_name, pr_number_str = match.group(1), match.group(2), match.group(3)

    try:
        pr_number = int(pr_number_str)
    except (TypeError, ValueError):
        raise ValueError(PR_URL_HELP)

    return owner, repo_name, pr_number

@webhook_app.get("/health")
async def health():
    return {"status": "healthy", "version": "16.0"}

def extract_code_from_notebook(content: str) -> str:
    """Extract code from notebook"""
    try:
        nb = json.loads(content)
        code_lines = []
        for cell in nb.get("cells", []):
            if cell.get("cell_type") == "code":
                source = cell.get("source", [])
                if isinstance(source, list):
                    code_lines.extend(source)
                else:
                    code_lines.append(source)
        return "".join(code_lines)
    except:
        return content

def fetch_file_content(repo: str, file_path: str, ref: str, token: str) -> str:
    """Fetch file (token optional for public repos)"""
    try:
        headers = {"Accept": "application/vnd.github.v3.raw"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        url = f"https://api.github.com/repos/{repo}/contents/{file_path}?ref={ref}"
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code == 200:
            content = r.text[:50000]
            if file_path.endswith(".ipynb"):
                content = extract_code_from_notebook(content)
            return content
        return ""
    except:
        return ""

def format_lines(lines: List[int]) -> str:
    """Format line numbers nicely"""
    if not lines:
        return ""
    
    lines = sorted(set(lines))
    
    if len(lines) == 1:
        return f"line {lines[0]}"
    elif len(lines) <= 3:
        return f"lines {', '.join(map(str, lines))}"
    else:
        return f"lines {lines[0]}, {lines[1]}, {lines[2]}, ...+{len(lines)-3} more"

def format_review(issue_types: dict) -> str:
    """Format as readable conversation with line numbers"""
    
    # Count unique files
    total_files = set()
    for issue_type, data in issue_types.items():
        total_files.update(data.get("files", {}).keys())
    
    if not total_files:
        return "Reviewed the PR - code looks solid. No major concerns."
    
    # Start conversation
    output = f"Reviewed {len(total_files)} files. Here's what needs attention:\n\n"
    output += "─" * 70 + "\n\n"
    
    issue_num = 1
    
    # 1. Critical: Error handling
    if issue_types["no_error_handling"]["files"]:
        files = issue_types["no_error_handling"]["files"]
        output += f"{issue_num}. I/O operations without error handling\n\n"
        
        count = 0
        for file_path, lines in sorted(files.items()):
            if count >= 3:
                output += f"   → ...and {len(files) - 3} more files\n"
                break
            output += f"   → {file_path} ({format_lines(lines)})\n"
            count += 1
        
        output += "\n   These will crash the pipeline when data sources are unavailable.\n"
        output += "   Wrap spark.read and API calls in try/except blocks with logging.\n\n"
        issue_num += 1
    
    # 2. Critical: CIBIL nulls
    if issue_types["cibil_nulls"]["files"]:
        files = issue_types["cibil_nulls"]["files"]
        output += f"{issue_num}. CIBIL scores used without null handling\n\n"
        
        count = 0
        for file_path, lines in sorted(files.items()):
            if count >= 3:
                output += f"   → ...and {len(files) - 3} more files\n"
                break
            output += f"   → {file_path} ({format_lines(lines)})\n"
            count += 1
        
        output += "\n   About 30% of EduFin records have null CIBIL scores. Calculations will be wrong.\n"
        output += "   Add .fillna() or .coalesce() with your business rule (exclude? impute? flag?).\n\n"
        issue_num += 1
    
    # 3. Critical: Memory risk
    if issue_types["collect_risk"]["files"]:
        files = issue_types["collect_risk"]["files"]
        output += f"{issue_num}. Memory risk with .collect()\n\n"
        
        count = 0
        for file_path, lines in sorted(files.items()):
            if count >= 3:
                output += f"   → ...and {len(files) - 3} more files\n"
                break
            output += f"   → {file_path} ({format_lines(lines)})\n"
            count += 1
        
        output += "\n   Pulling all rows into driver memory - OOM crashes as data grows.\n"
        output += "   Add .limit(1000) before .collect(), or use .show() / .take() instead.\n\n"
        issue_num += 1
    
    # 4. Critical: Credentials
    if issue_types["hardcoded_creds"]["files"]:
        files = issue_types["hardcoded_creds"]["files"]
        output += f"{issue_num}. Hardcoded credentials\n\n"
        
        count = 0
        for file_path, lines in sorted(files.items()):
            if count >= 2:
                output += f"   → ...and {len(files) - 2} more files\n"
                break
            output += f"   → {file_path} ({format_lines(lines)})\n"
            count += 1
        
        output += "\n   These will be visible in Git history to anyone with repo access.\n"
        output += "   Move to dbutils.secrets.get() or environment variables.\n\n"
        issue_num += 1
    
    # 5. Observability
    obs = []
    if issue_types["no_monitoring"]["files"]:
        obs.append(f"no logging in {len(issue_types['no_monitoring']['files'])} files")
    if issue_types["no_tests"]["files"]:
        obs.append(f"no tests in {len(issue_types['no_tests']['files'])} files")
    
    if obs:
        output += "─" * 70 + "\n\n"
        output += f"{issue_num}. Observability gaps: {', '.join(obs)}\n\n"
        output += "   When these run in production, you won't be able to debug failures or track performance.\n"
        output += "   Add basic logging (start/end, row counts) and a few assertions for key scenarios.\n\n"
        issue_num += 1
    
    # 6. Data quality
    dq = []
    if issue_types["no_dq"]["files"]:
        dq.append(f"no data quality checks in {len(issue_types['no_dq']['files'])} pipeline{'s' if len(issue_types['no_dq']['files']) > 1 else ''}")
    if issue_types["no_idempotency"]["files"]:
        dq.append(f"writes aren't idempotent in {len(issue_types['no_idempotency']['files'])} file{'s' if len(issue_types['no_idempotency']['files']) > 1 else ''}")
    
    if dq:
        output += "─" * 70 + "\n\n"
        output += f"{issue_num}. Data quality: {', '.join(dq)}\n\n"
        output += "   Schema changes or job reruns will cause issues. Add validation checks and use\n"
        output += "   .mode('overwrite') or MERGE to make pipelines safe to rerun.\n\n"
    
    output += "─" * 70 + "\n\n"
    output += "Let me know if you want details on any specific file."
    
    return output

def classify_issues(all_reviews: List[Dict]) -> dict:
    """Classify raw per-file review results into known issue-type buckets.

    This is the single source of truth for issue detection, used by both
    the plain-text summary (format_review) and the structured UI data
    (build_findings / compute_overall_score / render_* helpers below).
    """

    issue_types = {
        "no_error_handling": {"files": {}},
        "no_monitoring": {"files": {}},
        "no_tests": {"files": {}},
        "cibil_nulls": {"files": {}},
        "collect_risk": {"files": {}},
        "no_dq": {"files": {}},
        "no_idempotency": {"files": {}},
        "hardcoded_creds": {"files": {}},
    }
    
    for review in all_reviews:
        file_path = review["file_path"]
        feedback = review["feedback"]
        inline = review.get("inline_comments", [])
        
        # Extract line numbers from inline comments
        def get_lines(keyword):
            lines = []
            for comment in inline:
                if keyword.lower() in comment.get("body", "").lower():
                    lines.append(comment.get("line", 0))
            return [l for l in lines if l > 0]
        
        if "I/O call will crash" in feedback or "I/O operation" in feedback:
            lines = get_lines("I/O")
            if not lines:  # Fallback to line 1 if not found
                lines = [1]
            issue_types["no_error_handling"]["files"][file_path] = lines
        
        if "logging or metrics" in feedback or "logging" in feedback:
            issue_types["no_monitoring"]["files"][file_path] = [1]
        
        if "no unit tests" in feedback or "Have you tested" in feedback:
            issue_types["no_tests"]["files"][file_path] = [1]
        
        if "CIBIL" in feedback and "null" in feedback:
            lines = get_lines("CIBIL")
            if not lines:
                lines = [1]
            issue_types["cibil_nulls"]["files"][file_path] = lines
        
        if ".collect()" in feedback:
            lines = get_lines("collect")
            if not lines:
                lines = [1]
            issue_types["collect_risk"]["files"][file_path] = lines
        
        if "data quality checks" in feedback or "quality check" in feedback:
            issue_types["no_dq"]["files"][file_path] = [1]
        
        if "idempotent" in feedback or "duplicate" in feedback.lower():
            issue_types["no_idempotency"]["files"][file_path] = [1]
        
        if "credential" in feedback.lower() or "secret" in feedback.lower():
            lines = get_lines("credential")
            if not lines:
                lines = get_lines("secret")
            if not lines:
                lines = [1]
            issue_types["hardcoded_creds"]["files"][file_path] = lines

    return issue_types

def deduplicate_issues(all_reviews: List[Dict]) -> str:
    """Deduplicate and format with line numbers (plain-text summary)."""
    return format_review(classify_issues(all_reviews))


# ----------------------------------------------------------------------------
# Structured UI data — built from the SAME classify_issues() output as the
# text summary above, so the Overall Score / tab badges / issue cards always
# match what's shown in the Editable Review box. No detection logic lives
# here; this only maps each issue-type bucket to display metadata.
# ----------------------------------------------------------------------------

ISSUE_META = {
    "hardcoded_creds": {
        "category": "Security",
        "title": "Hardcoded Credential Detected",
        "severity": "High",
        "description": "A hardcoded credential, token, or secret was found in {file}.",
        "recommendation": "Move this value to environment variables or a secrets manager (e.g. dbutils.secrets.get()) instead of committing it to source control.",
    },
    "collect_risk": {
        "category": "Performance",
        "title": "Memory Risk with .collect()",
        "severity": "Medium",
        "description": "{file} pulls a full DataFrame into driver memory with .collect(), which risks out-of-memory failures as data grows.",
        "recommendation": "Add .limit(1000) before .collect(), or use .show() / .take() for inspection instead.",
    },
    "no_error_handling": {
        "category": "Architecture",
        "title": "Missing Error Handling",
        "severity": "High",
        "description": "{file} performs I/O (reads, API/network calls) without try/except handling.",
        "recommendation": "Wrap I/O calls in try/except blocks with logging so failures don't crash the pipeline.",
    },
    "no_dq": {
        "category": "Architecture",
        "title": "Missing Data Quality Checks",
        "severity": "Medium",
        "description": "{file} does not validate incoming or outgoing data.",
        "recommendation": "Add validation checks (row counts, null/range checks, schema checks) before writing downstream.",
    },
    "no_idempotency": {
        "category": "Architecture",
        "title": "Non-Idempotent Write",
        "severity": "Medium",
        "description": "{file} writes data in a way that may create duplicates if the job is rerun.",
        "recommendation": "Use .mode('overwrite') or a MERGE statement so the write is safe to rerun.",
    },
    "cibil_nulls": {
        "category": "Code Quality",
        "title": "Unhandled Nulls in CIBIL Scores",
        "severity": "High",
        "description": "{file} uses CIBIL scores without handling null values.",
        "recommendation": "Add .fillna() or .coalesce() with an explicit business rule for missing scores (exclude, impute, or flag).",
    },
    "no_monitoring": {
        "category": "Code Quality",
        "title": "Missing Logging / Monitoring",
        "severity": "Low",
        "description": "{file} has no logging or metrics instrumentation.",
        "recommendation": "Add basic logging (start/end, row counts) so failures and performance can be tracked in production.",
    },
    "no_tests": {
        "category": "Code Quality",
        "title": "Missing Unit Tests",
        "severity": "Low",
        "description": "{file} has no accompanying unit tests.",
        "recommendation": "Add tests covering the key transformation logic and edge cases (nulls, empty input).",
    },
}

CATEGORIES = ["Security", "Performance", "Architecture", "Code Quality"]
SEVERITY_WEIGHT = {"High": 1.0, "Medium": 0.5, "Low": 0.2}
SEVERITY_CSS = {"High": "sev-high", "Medium": "sev-med", "Low": "sev-low"}

def build_findings(issue_types: dict) -> Dict[str, List[dict]]:
    """Turn classify_issues() output into per-category lists of issue cards."""
    findings = {cat: [] for cat in CATEGORIES}

    for issue_key, meta in ISSUE_META.items():
        files = issue_types.get(issue_key, {}).get("files", {})
        for file_path, lines in sorted(files.items()):
            line = sorted(lines)[0] if lines else None
            findings[meta["category"]].append({
                "title": meta["title"],
                "severity": meta["severity"],
                "file": file_path,
                "line": line,
                "description": meta["description"].format(file=file_path),
                "recommendation": meta["recommendation"],
            })

    return findings

def compute_overall_score(findings: Dict[str, List[dict]]) -> float:
    """Derive a 0-10 overall score from the actual findings' severities."""
    total_deduction = sum(
        SEVERITY_WEIGHT.get(issue["severity"], 0.3)
        for issues in findings.values()
        for issue in issues
    )
    score = max(0.0, min(10.0, 10.0 - total_deduction))
    return round(score, 1)

def _escape(text: str) -> str:
    if text is None:
        return ""
    return (str(text).replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace('"', "&quot;"))

def render_score_html(score: float, counts: Dict[str, int]) -> str:
    """Render the Overall Score card from a real score + real category counts."""
    ring_deg = round((score / 10.0) * 360)
    return f"""
<p class="card-title">Overall Score</p>
<p class="card-subtitle">Summary view — full write-up is in Editable Review below</p>
<div class="score-grid">
    <div class="score-ring-wrap" style="background: conic-gradient(var(--accent) 0deg {ring_deg}deg, var(--accent-soft) {ring_deg}deg 360deg);"><div class="score-ring-inner"><div class="score-num">{score}</div><div class="score-den">/ 10</div></div></div>
    <div class="metric-card"><div class="metric-label">Security Issues</div><div class="metric-num c-red">{counts['Security']}</div><div class="metric-sub">Critical</div></div>
    <div class="metric-card"><div class="metric-label">Performance</div><div class="metric-num c-orange">{counts['Performance']}</div><div class="metric-sub">Improvements</div></div>
    <div class="metric-card"><div class="metric-label">Architecture</div><div class="metric-num c-accent">{counts['Architecture']}</div><div class="metric-sub">Suggestions</div></div>
    <div class="metric-card"><div class="metric-label">Code Quality</div><div class="metric-num c-ink">{counts['Code Quality']}</div><div class="metric-sub">Issues</div></div>
</div>
"""

def render_cat_nav_html(counts: Dict[str, int]) -> str:
    """Render the category tab bar with real badge counts.

    Purely presentational: each tab carries a data-cat attribute and an
    onclick that calls the client-side showReviewCategory() helper (defined
    once via the page-load JS), which toggles which .cat-section is visible
    in the issue panel below. No backend/event-handler changes involved.
    """
    items = ""
    for i, cat in enumerate(CATEGORIES):
        active = " active" if i == 0 else ""
        safe_cat = _escape(cat)
        items += (
            f'    <div class="cat-item{active}" data-cat="{safe_cat}" '
            f'onclick="showReviewCategory(this, \'{safe_cat}\')">'
            f'{safe_cat} <span class="cat-count">{counts[cat]}</span></div>\n'
        )
    return f'<div class="cat-nav">\n{items}</div>\n'

def _render_issue_card(issue: dict) -> str:
    sev = issue["severity"]
    sev_class = SEVERITY_CSS.get(sev, "sev-med")
    file_label = _escape(issue["file"])
    if issue.get("line"):
        file_label += f":{issue['line']}"
    return f"""
<div class="issue-card">
    <div class="issue-top">
        <div class="issue-left">
            <span class="sev-badge {sev_class}">{_escape(sev)}</span>
            <span class="issue-name">{_escape(issue['title'])}</span>
        </div>
        <span class="issue-file">{file_label}</span>
    </div>
    <p class="issue-desc">{_escape(issue['description'])}</p>
    <div class="issue-suggest"><b>Suggestion:</b>&nbsp;{_escape(issue['recommendation'])}</div>
</div>
"""

def render_issue_panel_html(findings: Dict[str, List[dict]]) -> str:
    """Render the AI Review Output panel from real findings, grouped by category.

    Each category is wrapped in a .cat-section with a data-cat attribute.
    Only the first category is visible by default; the rest start hidden.
    Clicking a tab in the cat-nav (see render_cat_nav_html) toggles which
    single section is shown, purely on the client side — the underlying
    data/markup for every category is still rendered exactly as before.

    Categories with no issues show a friendly empty message instead of
    fake issue cards.
    """
    sections = ""
    for i, cat in enumerate(CATEGORIES):
        issues = findings[cat]
        safe_cat = _escape(cat)
        display = "block" if i == 0 else "none"
        sections += f'<div class="cat-section" data-cat="{safe_cat}" style="display:{display};">\n'
        sections += f'<p class="issue-panel-title">{safe_cat} Issues ({len(issues)})</p>\n\n'
        if issues:
            for issue in issues:
                sections += _render_issue_card(issue)
        else:
            sections += '<p class="card-subtitle" style="margin:0 0 20px 4px;">No issues detected in this category.</p>\n'
        sections += '</div>\n'
    return sections

def render_empty_score_html() -> str:
    return render_score_html(0.0, {"Security": 0, "Performance": 0, "Architecture": 0, "Code Quality": 0}).replace(
        '<div class="score-num">0.0</div>', '<div class="score-num">—</div>'
    ).replace(
        "background: conic-gradient(var(--accent) 0deg 0deg, var(--accent-soft) 0deg 360deg);",
        "background: var(--accent-soft);"
    )

def render_empty_cat_nav_html() -> str:
    return render_cat_nav_html({"Security": 0, "Performance": 0, "Architecture": 0, "Code Quality": 0})

def render_empty_issue_panel_html() -> str:
    return '<p class="card-subtitle" style="margin:0;">Run a review to see AI findings.</p>'

def manual_review(pr_url, token=None):
    """Review PR.

    Returns a 4-tuple: (editable_review_text, score_html, cat_nav_html,
    issue_panel_html). On any failure before a real review is produced,
    the UI panels are reset to their empty state rather than showing
    stale or fake data.
    """
    empty_ui = (render_empty_score_html(), render_empty_cat_nav_html(), render_empty_issue_panel_html())

    try:
        owner, repo_name, pr_number = parse_pr_url(pr_url)
    except ValueError as e:
        return (f"❌ {str(e)}", *empty_ui)

    repo = f"{owner}/{repo_name}"
    num = str(pr_number)

    use_token = token or GITHUB_TOKEN
    # Token is optional for public repos - GitHub allows unauthenticated access
    # (with lower rate limits: 60 req/hour vs 5000 with token)

    try:
        # Build headers - add Authorization only if token is provided
        headers = {"Accept": "application/vnd.github.v3+json"}
        if use_token:
            headers["Authorization"] = f"Bearer {use_token}"
        
        pr_resp = requests.get(f"https://api.github.com/repos/{repo}/pulls/{num}", headers=headers, timeout=10)
        if pr_resp.status_code != 200:
            return (f"❌ Failed to fetch PR: {pr_resp.status_code}", *empty_ui)
        
        pr_data = pr_resp.json()
        pr_title = pr_data.get("title", "")
        pr_description = pr_data.get("body", "")  # Fetch PR description for context learning
        author = pr_data.get("user", {}).get("login", "")
        head_ref = pr_data.get("head", {}).get("ref", "main")
        
        files_resp = requests.get(f"https://api.github.com/repos/{repo}/pulls/{pr_number}/files", headers=headers, timeout=10)
        if files_resp.status_code != 200:
            return (f"❌ Failed to fetch files: {files_resp.status_code}", *empty_ui)
        
        files = files_resp.json()
        
        all_reviews = []
        
        for file in files:
            file_path = file.get("filename", "")
            
            if file.get("status") == "removed":
                continue
            if not any(file_path.endswith(ext) for ext in [".py", ".sql", ".ipynb"]):
                continue
            
            patch = file.get("patch", "")
            if patch:
                code_lines = [line[1:] for line in patch.split("\n") if line.startswith("+") and not line.startswith("+++")]
                code = "\n".join(code_lines) if code_lines else ""
            else:
                code = ""
            
            if not code or len(code.strip()) < 20:
                code = fetch_file_content(repo, file_path, head_ref, use_token)
            
            if not code or len(code.strip()) < 20:
                continue
            
            result = review_pr_code(file_path, code, {
                "title": pr_title,
                "author": author,
                "description": pr_description,
                "url": pr_url,
                "pr_id": str(pr_number)
            })
            
            if result["status"] != "LOOKS_GOOD":
                all_reviews.append(result)
        
        issue_types = classify_issues(all_reviews)
        summary = format_review(issue_types)

        findings = build_findings(issue_types)
        counts = {cat: len(findings[cat]) for cat in CATEGORIES}
        score = compute_overall_score(findings)

        score_html = render_score_html(score, counts)
        cat_nav_html = render_cat_nav_html(counts)
        issue_panel_html = render_issue_panel_html(findings)

        return (summary, score_html, cat_nav_html, issue_panel_html)
        
    except requests.exceptions.RequestException as e:
        print(f"❌ GitHub API RequestException: {type(e).__name__}: {str(e)}")
        return ("❌ Could not reach GitHub. Please check your connection and try again.", *empty_ui)
    except Exception as e:
        import traceback
        print(f"❌ EXCEPTION in manual_review: {type(e).__name__}: {str(e)}")
        print("Traceback:")
        traceback.print_exc()
        return (f"❌ Error during review: {type(e).__name__}: {str(e)}", *empty_ui)

def submit_review(review_body, pr_url, token=None):
    """Post review as a comment on the GitHub PR (accepts optional user token)
    
    After successful posting, stores the review for learning/context (non-blocking).
    """
    if not pr_url or not review_body:
        return "❌ Missing PR URL or review"
    
    if "code looks solid" in review_body.lower():
        return "ℹ️ No issues to post"

    try:
        owner, repo_name, pr_number = parse_pr_url(pr_url)
    except ValueError as e:
        return f"❌ {str(e)}"

    repo = f"{owner}/{repo_name}"
    
    # Use provided token or fallback to environment token
    use_token = token or GITHUB_TOKEN
    if not use_token:
        return "❌ No GitHub token available"

    try:
        headers = {"Authorization": f"Bearer {use_token}", "Accept": "application/vnd.github.v3+json"}
        
        # Post the review comment
        url = f"https://api.github.com/repos/{repo}/issues/{pr_number}/comments"
        payload = {"body": review_body}
        
        r = requests.post(url, headers=headers, json=payload, timeout=15)

        if r.status_code in [200, 201]:
            # SUCCESS! Now store the review for learning (non-blocking)
            try:
                # Fetch PR metadata for storage
                pr_api_url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}"
                pr_resp = requests.get(pr_api_url, headers=headers, timeout=10)
                
                if pr_resp.status_code == 200:
                    pr_data = pr_resp.json()
                    author = pr_data.get("user", {}).get("login", "unknown")
                    
                    # Fetch changed files
                    files_url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}/files"
                    files_resp = requests.get(files_url, headers=headers, timeout=10)
                    changed_files = []
                    if files_resp.status_code == 200:
                        changed_files = [f.get("filename", "") for f in files_resp.json()]
                    
                    # Determine review status
                    review_status = "NEEDS_REVIEW" if "❌" in review_body or "⚠️" in review_body else "LOOKS_GOOD"
                    
                    # Count inline comments (rough estimate from review body)
                    inline_comments_count = review_body.count("Line ")
                    
                    # Store the completed review (non-blocking - failures are logged but don't affect user)
                    kb = get_knowledge_base()
                    if kb and kb.enabled:
                        kb.store_completed_review(
                            pr_url=pr_url,
                            pr_number=int(pr_number),
                            repo=repo,
                            author=author,
                            changed_files=changed_files,
                            review_feedback=review_body,
                            review_status=review_status,
                            inline_comments_count=inline_comments_count
                        )
            except Exception as storage_error:
                # Storage failures never block the success message
                print(f"⚠️ Review posted successfully but storage failed (non-blocking): {str(storage_error)[:200]}")
            
            return "✅ Posted to GitHub!"
        else:
            return f"❌ Failed to post review (GitHub returned status {r.status_code}). Please try again."

    except requests.exceptions.RequestException as e:
        print(f"❌ GitHub API RequestException in submit_review: {type(e).__name__}: {str(e)}")
        return "❌ Could not reach GitHub. Please check your connection and try again."
    except Exception as e:
        import traceback
        print(f"❌ EXCEPTION in submit_review: {type(e).__name__}: {str(e)}")
        print("Traceback:")
        traceback.print_exc()
        return f"❌ Error posting review: {type(e).__name__}: {str(e)}"

# ============================================================================
# IDEMPOTENCY — Duplicate PR detection and tracking
# ============================================================================

def is_pr_already_reviewed(pr_url: str) -> bool:
    """
    Check if a PR has already been successfully reviewed and posted.
    
    Args:
        pr_url: Full GitHub PR URL
        
    Returns:
        True if PR was already reviewed, False otherwise
    """
    try:
        from databricks.sdk import WorkspaceClient
        
        w = WorkspaceClient()
        warehouse_id = os.environ.get('DATABRICKS_WAREHOUSE_ID')
        
        if not warehouse_id:
            print("⚠️ Idempotency check skipped: No warehouse ID configured")
            return False
        
        # Escape single quotes for SQL
        safe_url = pr_url.replace("'", "''")
        
        query = f"""
            SELECT pr_url, reviewed_at
            FROM sap_dev.pr_reviewer.completed_reviews
            WHERE pr_url = '{safe_url}'
            LIMIT 1
        """
        
        result = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=query,
            wait_timeout="30s"
        )
        
        if result.result and result.result.data_array and len(result.result.data_array) > 0:
            reviewed_at = result.result.data_array[0][1] if len(result.result.data_array[0]) > 1 else 'unknown'
            print(f"✅ Duplicate PR detected: {pr_url} (already reviewed at {reviewed_at})")
            return True
        
        return False
        
    except Exception as e:
        error_msg = str(e)[:200]
        print(f"⚠️ Error checking duplicate PR: {error_msg}")
        # On error, default to allowing the review (fail open)
        return False


def record_completed_review(pr_url: str, pr_number: int, repo: str, review_feedback: str, review_status: str = "NEEDS_REVIEW") -> bool:
    """
    Record a successfully completed and posted PR review.
    
    Args:
        pr_url: Full GitHub PR URL
        pr_number: PR number
        repo: Repository (owner/name format)
        review_feedback: Full review text that was posted
        review_status: Review status (NEEDS_REVIEW/LOOKS_GOOD)
        
    Returns:
        True if recorded successfully, False otherwise
    """
    try:
        from databricks.sdk import WorkspaceClient
        
        w = WorkspaceClient()
        warehouse_id = os.environ.get('DATABRICKS_WAREHOUSE_ID')
        
        if not warehouse_id:
            print("⚠️ Review recording skipped: No warehouse ID configured")
            return False
        
        # Escape single quotes for SQL
        safe_url = pr_url.replace("'", "''")
        safe_repo = repo.replace("'", "''")
        safe_feedback = review_feedback.replace("'", "''")[:5000]  # Truncate if too long
        safe_status = review_status.replace("'", "''")
        
        query = f"""
            INSERT INTO sap_dev.pr_reviewer.completed_reviews
            (pr_url, pr_number, repo, review_feedback, review_status, reviewed_at)
            VALUES (
                '{safe_url}',
                {pr_number},
                '{safe_repo}',
                '{safe_feedback}',
                '{safe_status}',
                current_timestamp()
            )
        """
        
        w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=query,
            wait_timeout="30s"
        )
        
        print(f"✅ Recorded completed review for {pr_url}")
        return True
        
    except Exception as e:
        error_msg = str(e)[:200]
        print(f"⚠️ Error recording completed review: {error_msg}")
        return False

# ============================================================================
# ============================================================================
# UI LAYER — SkillAIPath Reviewer
#
# NOTE ON LIVE PANELS:
# The Review Progress stepper stays static (it just shows pipeline
# stages, not review results). The Overall Score ring/metrics, the tab
# badge counts, and the AI Review Output issue cards are now populated
# from the real backend findings (classify_issues / build_findings /
# compute_overall_score / render_*_html) each time a review runs — see
# manual_review()'s return value. Before any review has been run, they
# show an explicit empty state instead of placeholder numbers. The
# "Editable Review" box remains the exact live text that gets posted to
# GitHub via submit_review().
#
# Placeholder features that had no backing implementation (Review
# History, Settings, Workspace card, and the Knowledge Base / Learning &
# Improvement panel — rag_query is not part of this project, so
# KB_ENABLED was always False) have been removed rather than shown as
# non-functional UI.
# ============================================================================

custom_css = """
:root {
    --accent: #5B2EFF;
    --accent-light: #7C5CFF;
    --accent-soft: #EFEAFF;
    --ink-900: #1F2937;
    --ink-700: #3A4356;
    --ink-500: #6B7280;
    --line: #E5E7EF;
    --surface: #FFFFFF;
    --canvas: #F8F9FC;
    --green: #16A34A;
    --green-soft: #E7F8EE;
    --red: #DC2626;
    --red-soft: #FDECEC;
    --orange: #EA8C1D;
    --orange-soft: #FDF1E2;
}

/* ---------- Force light theme regardless of client dark-mode ---------- */
html, body, .dark, gradio-app, .gradio-container, .gradio-container.dark {
    background: var(--canvas) !important;
    color: var(--ink-900) !important;
}
.dark .card, .dark .block, .dark .gr-group, .dark .form,
.card, .block, .gr-group, .form {
    background: var(--surface) !important;
    color: var(--ink-900) !important;
}
.dark input, .dark textarea, .dark select,
input, textarea, select {
    background: var(--surface) !important;
    color: var(--ink-900) !important;
    border-color: var(--line) !important;
}
.dark label span, .dark p, .dark span, .dark div,
label span { color: inherit; }
.dark .card-subtitle, .card-subtitle { color: var(--ink-500) !important; }

/* ---------- Base ---------- */
.gradio-container {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif !important;
}
.contain { width: 100% !important; max-width: 1800px !important; margin: 0 auto !important; padding: 24px 32px !important; box-sizing: border-box !important; }
.block, .form, .gr-group { border: none !important; box-shadow: none !important; }
#main-content { width: 95%; max-width: 1600px; margin: 0 auto !important; }

/* ---------- Page Header ---------- */
.page-header { text-align: center; padding: 32px 8px 8px 8px; }
.page-header-title { font-size: 30px; font-weight: 700; color: #5B2EFF !important; margin: 0; letter-spacing: -0.01em; }
.page-header-subtitle { font-size: 15px; font-weight: 400; color: #6B7280 !important; margin: 6px 0 0 0; }

/* ---------- Cards (shared) ---------- */
.card {
    background: var(--surface) !important;
    border-radius: 12px !important;
    border: 1px solid var(--line) !important;
    padding: 20px !important;
    margin-bottom: 20px !important;
    box-shadow: 0 1px 2px rgba(31,41,55,0.04), 0 4px 12px rgba(31,41,55,0.03) !important;
}
.card-title { font-size: 16px; font-weight: 700; color: var(--ink-900) !important; margin: 0 0 2px 0; }
.card-subtitle { font-size: 13px; color: var(--ink-500) !important; margin: 0 0 18px 0; }
.card-title-row { display: flex; align-items: flex-start; justify-content: space-between; margin-bottom: 4px; }

/* ---------- Inputs ---------- */
label span { font-weight: 600 !important; color: var(--ink-700) !important; font-size: 13px !important; }
input[type="text"], input[type="password"], textarea {
    border-radius: 9px !important;
    border: 1.5px solid var(--line) !important;
    padding: 11px 13px !important;
    font-size: 14px !important;
    transition: border-color 0.15s ease, box-shadow 0.15s ease !important;
}
input[type="text"]:focus, input[type="password"]:focus, textarea:focus {
    border-color: var(--accent) !important;
    box-shadow: 0 0 0 3.5px var(--accent-soft) !important;
    outline: none !important;
}

/* ---------- Buttons ---------- */
button.primary, #start-review-btn button, #post-github-btn button {
    background: linear-gradient(135deg, var(--accent) 0%, var(--accent-light) 100%) !important;
    border: none !important;
    color: #ffffff !important;
    font-weight: 600 !important;
    padding: 13px 26px !important;
    border-radius: 10px !important;
    font-size: 14.5px !important;
    box-shadow: 0 4px 14px rgba(91,46,255,0.28) !important;
    transition: filter 0.15s ease, transform 0.1s ease !important;
}
button.primary:hover, #start-review-btn button:hover, #post-github-btn button:hover {
    filter: brightness(1.06) !important;
    transform: translateY(-1px) !important;
}

/* ---------- Stepper ---------- */
.stepper-row { display: flex; align-items: flex-start; justify-content: space-between; margin-top: 8px; position: relative; }
.stepper-row::before {
    content: ""; position: absolute; top: 15px; left: 6%; right: 6%; height: 2px; background: var(--accent-soft); z-index: 0;
}
.step { position: relative; z-index: 1; display: flex; flex-direction: column; align-items: center; gap: 6px; flex: 1; }
.step-circle { width: 30px; height: 30px; border-radius: 50%; background: var(--accent); color: #fff !important; display: flex; align-items: center; justify-content: center; font-size: 13px; font-weight: 700; }
.step-label { font-size: 12.5px; font-weight: 600; color: var(--ink-900) !important; }
.step-sub { font-size: 11px; color: var(--ink-500) !important; }
.badge-green { display: inline-flex; align-items: center; gap: 5px; background: var(--green-soft); color: var(--green) !important; font-size: 12px; font-weight: 700; padding: 5px 12px; border-radius: 999px; }

/* ---------- Score section ---------- */
.score-grid { display: flex; gap: 16px; align-items: stretch; margin-top: 4px; flex-wrap: wrap; }
.score-ring-wrap { width: 140px; height: 140px; border-radius: 50%; background: conic-gradient(var(--accent) 0deg 300deg, var(--accent-soft) 300deg 360deg); display: flex; align-items: center; justify-content: center; flex-shrink: 0; }
.score-ring-inner { width: 108px; height: 108px; border-radius: 50%; background: var(--surface); display: flex; flex-direction: column; align-items: center; justify-content: center; }
.score-num { font-size: 28px; font-weight: 800; color: var(--ink-900) !important; line-height: 1; }
.score-den { font-size: 12px; color: var(--ink-500) !important; margin-top: 2px; }
.metric-card { flex: 1; min-width: 140px; border: 1px solid var(--line); border-radius: 12px; padding: 16px; background: var(--surface); }
.metric-label { font-size: 12.5px; font-weight: 600; color: var(--ink-700) !important; margin-bottom: 10px; }
.metric-num { font-size: 26px; font-weight: 800; }
.metric-sub { font-size: 11.5px; color: var(--ink-500) !important; margin-top: 4px; }
.c-red { color: var(--red) !important; } .c-orange { color: var(--orange) !important; } .c-accent { color: var(--accent) !important; } .c-ink { color: var(--ink-900) !important; }

/* ---------- AI review category tabs + issue cards ---------- */
.cat-nav { display: flex; flex-direction: row; flex-wrap: wrap; gap: 10px; width: 100%; margin-bottom: 16px; }
.cat-item { display: inline-flex; align-items: center; gap: 8px; padding: 9px 16px; border-radius: 999px; border: 1.5px solid var(--accent); font-size: 13.5px; font-weight: 600; color: var(--ink-700) !important; background: var(--surface); cursor: pointer; user-select: none; transition: background 0.15s ease, color 0.15s ease; }
.cat-item:hover { background: var(--accent-soft); }
.cat-item.active { border-color: var(--accent); background: var(--accent); color: #fff !important; }
.cat-count { font-size: 11.5px; font-weight: 700; background: var(--canvas); color: var(--ink-500) !important; padding: 2px 8px; border-radius: 999px; }
.cat-item.active .cat-count { background: rgba(255,255,255,0.25); color: #fff !important; }
.cat-item.active:hover { background: var(--accent); }

.issue-panel-title { font-size: 14px; font-weight: 700; color: var(--red) !important; margin: 0 0 12px 4px; }
.issue-card { border: 1px solid var(--line); border-radius: 12px; padding: 14px 16px; margin-bottom: 12px; background: var(--surface); }
.issue-top { display: flex; align-items: center; justify-content: space-between; margin-bottom: 6px; }
.issue-left { display: flex; align-items: center; gap: 8px; }
.sev-badge { font-size: 10.5px; font-weight: 700; padding: 3px 9px; border-radius: 999px; text-transform: uppercase; letter-spacing: 0.03em; }
.sev-high { background: var(--red-soft); color: var(--red) !important; }
.sev-med { background: var(--orange-soft); color: var(--orange) !important; }
.sev-low { background: var(--green-soft); color: var(--green) !important; }
.issue-name { font-size: 14px; font-weight: 700; color: var(--ink-900) !important; }
.issue-file { font-size: 12px; color: var(--accent) !important; font-family: 'SFMono-Regular', Consolas, monospace; }
.issue-desc { font-size: 13px; color: var(--ink-700) !important; margin: 4px 0 10px 0; }
.issue-suggest { display: flex; gap: 8px; align-items: flex-start; background: var(--canvas); border-radius: 9px; padding: 10px 12px; font-size: 12.5px; color: var(--ink-700) !important; }
.issue-suggest b { color: var(--ink-900) !important; }

/* ---------- Editor toolbar ---------- */
.editor-toolbar { display: flex; align-items: center; gap: 14px; padding: 10px 14px; border: 1.5px solid var(--line); border-bottom: none; border-radius: 10px 10px 0 0; background: var(--canvas); font-size: 13px; color: var(--ink-500) !important; font-weight: 600; }
#review-textbox textarea { border-radius: 0 0 10px 10px !important; border-top: none !important; }
.md-note { text-align: right; font-size: 11.5px; color: var(--ink-500) !important; margin-top: 6px; }

/* ---------- Right panel ---------- */

/* ---------- Footer ---------- */
#app-footer { text-align: center; padding: 24px 8px; color: var(--ink-500) !important; font-size: 12.5px; }
.copy-button svg { color: var(--ink-500) !important; }
"""

# Small script that strips any client-applied "dark" class so the palette
# above always renders, even on systems set to dark mode.
FORCE_LIGHT_JS = """
() => {
    document.documentElement.classList.remove('dark');
    document.body.classList.remove('dark');
    const container = document.querySelector('.gradio-container');
    if (container) container.classList.remove('dark');

    // AI Review Output tab switching: shows exactly one category's
    // findings at a time. Purely a display-state toggle over the HTML
    // that render_cat_nav_html / render_issue_panel_html already produce —
    // no backend calls, no change to review data or event handlers.
    window.showReviewCategory = (tabEl, cat) => {
        document.querySelectorAll('.cat-item').forEach((el) => {
            el.classList.toggle('active', el.getAttribute('data-cat') === cat);
        });
        document.querySelectorAll('.cat-section').forEach((el) => {
            el.style.display = (el.getAttribute('data-cat') === cat) ? 'block' : 'none';
        });
    };
}
"""

PAGE_HEADER_HTML = """
<div class="page-header">
    <p class="page-header-title">SkillAIPath Reviewer</p>
    <p class="page-header-subtitle">AI-Powered Pull Request Review Dashboard</p>
</div>
"""

PROGRESS_HTML = """
<div class="card-title-row">
    <div>
        <p class="card-title">Review Progress</p>
        <p class="card-subtitle" style="margin-bottom:0;">Pipeline stages for the current run</p>
    </div>
    <span class="badge-green">Completed</span>
</div>
<div class="stepper-row">
    <div class="step"><div class="step-circle">1</div><div class="step-label">Fetching PR</div><div class="step-sub">Completed</div></div>
    <div class="step"><div class="step-circle">2</div><div class="step-label">Analyzing Code</div><div class="step-sub">Completed</div></div>
    <div class="step"><div class="step-circle">3</div><div class="step-label">Generating Review</div><div class="step-sub">Completed</div></div>
    <div class="step"><div class="step-circle">4</div><div class="step-label">Review Complete</div><div class="step-sub">Completed</div></div>
</div>
"""

# NOTE: Overall Score, category tab badges, and issue cards are no longer
# static constants — they're rendered from real backend findings via
# render_score_html / render_cat_nav_html / render_issue_panel_html
# (defined above manual_review()). Before any review has run, the UI
# starts in the explicit empty state produced by render_empty_score_html /
# render_empty_cat_nav_html / render_empty_issue_panel_html.

EDITOR_TOOLBAR_HTML = """
<div class="editor-toolbar">
    <span>Normal</span><span>|</span>
    <b>B</b><i>I</i>
    <span>&#8226;&#8801;</span>
    <span>&lt;/&gt;</span>
    <span>&#128279;</span>
</div>
"""

with gr.Blocks(title="SkillAIPath Reviewer", css=custom_css, theme=gr.themes.Soft(primary_hue="violet"), js=FORCE_LIGHT_JS) as ui:
    # ---------------- Top Center Header ----------------
    gr.HTML(PAGE_HEADER_HTML)

    # ---------------- Main content ----------------
    with gr.Column(elem_id="main-content"):
        # Repository Configuration Card
        with gr.Group(elem_classes=["card"]):
            gr.Markdown('<p class="card-title">Repository Configuration</p><p class="card-subtitle">Connect your GitHub repository to start a comprehensive review.</p>')
            with gr.Row():
                url_in = gr.Textbox(
                    label="GitHub Repository URL",
                    placeholder="https://github.com/owner/repo/pull/123",
                    elem_classes=["textbox"]
                )
                token_in = gr.Textbox(
                    label="GitHub Token (Optional)",
                    type="password",
                    placeholder="ghp_xxxxxxxxxxxxx",
                    elem_classes=["textbox"]
                )
            review_btn = gr.Button(
                "Start Review",
                variant="primary",
                size="lg",
                elem_id="start-review-btn",
                elem_classes=["primary"]
            )

        # Review Progress Card (static — see note at top of file)
        with gr.Group(elem_classes=["card"]):
            gr.HTML(PROGRESS_HTML)

        # Overall Score Card — populated from real review findings
        with gr.Group(elem_classes=["card"]):
            score_display = gr.HTML(render_empty_score_html())

        # AI Review Output Card — populated from real review findings
        with gr.Group(elem_classes=["card"]):
            gr.Markdown('<p class="card-title">AI Review Output</p><p class="card-subtitle">Structured preview, styled like the reference</p>')
            cat_nav_display = gr.HTML(render_empty_cat_nav_html())
            issue_panel_display = gr.HTML(render_empty_issue_panel_html())

        # Editable Review Card — this is the REAL, live output
        with gr.Group(elem_classes=["card"]):
            gr.Markdown('<p class="card-title">Editable Review</p><p class="card-subtitle">Live output from the reviewer — edit before posting to GitHub. This is the exact content that gets posted.</p>')
            gr.HTML(EDITOR_TOOLBAR_HTML)
            editable_review = gr.Textbox(
                label=None,
                show_label=False,
                lines=14,
                interactive=True,
                show_copy_button=True,
                elem_id="review-textbox",
                elem_classes=["textbox"],
                placeholder="Your comprehensive review will appear here..."
            )
            gr.HTML('<p class="md-note">Markdown supported</p>')

            gr.Markdown('<p class="card-title" style="margin-top:18px;">Post Review to GitHub</p><p class="card-subtitle">Add a comment to the pull request with the review.</p>')
            submit_btn = gr.Button(
                "Post Review Comment",
                variant="primary",
                size="lg",
                elem_id="post-github-btn",
                elem_classes=["primary"]
            )
            submit_out = gr.Textbox(
                label="Status",
                lines=2,
                elem_classes=["textbox"]
            )

    # Footer
    gr.HTML("""
    <div id="app-footer">
        <p>&copy; 2024 SkillAIPath Reviewer. Built with Databricks.</p>
    </div>
    """)

    review_btn.click(
        manual_review,
        inputs=[url_in, token_in],
        outputs=[editable_review, score_display, cat_nav_display, issue_panel_display]
    )
    submit_btn.click(
        submit_review,
        inputs=[editable_review, url_in, token_in],
        outputs=[submit_out]
    )

app = gr.mount_gradio_app(webhook_app, ui, path="/")


if __name__ == "__main__":
    import uvicorn
    print("SkillAIPath Reviewer — starting server")
    uvicorn.run(app, host="0.0.0.0", port=8000)
