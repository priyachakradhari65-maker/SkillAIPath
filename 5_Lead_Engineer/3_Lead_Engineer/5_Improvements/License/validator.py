# ============================================================================
# SKILLAIPATH LICENSE VALIDATOR + FEATURE GATES
# ============================================================================
# Copy this ENTIRE file as Cell 1 of every job notebook.
# It validates the license key AND provides feature gate checks.
# ============================================================================

import hashlib
import hmac
import os
from datetime import datetime

# --- CONFIG -----------------------------------------------------------------
# License key file locations (tries in order)
LICENSE_PATHS = [
    "/Volumes/sap_dev/license/config/LICENSE_KEY.txt",   # UC Volume (primary)
    "dbfs:/app-config/LICENSE_KEY.txt",                    # DBFS (legacy fallback)
]
SECRET = "SkillAIPath-Master-Secret-2026-v1"

# --- FEATURE GATES ----------------------------------------------------------
FEATURE_GATES = {
    "FREE": {
        "jobs": ["01_Data_Ingestion", "02_Bronze_Stream_Generator"],
        "max_runs_per_day": 10,
        "label": "Free Edition (30% features)"
    },
    "TRIAL": {
        "jobs": "all",
        "max_runs_per_day": 100,
        "label": "Trial (full features, time-limited)"
    },
    "FAST": {
        "jobs": ["01_Data_Ingestion", "02_DBT_Full_Transform", "03_DLT_Setup_And_Run",
                 "04_Lakebase_Setup", "06_LTAP_Sync", "07_Retail_Ops_DB_Setup"],
        "max_runs_per_day": 50,
        "label": "Fast Track (60% features)"
    },
    "COMP": {
        "jobs": "all",
        "max_runs_per_day": 200,
        "label": "Comprehensive (100% features)"
    },
    "ENT": {
        "jobs": "all",
        "max_runs_per_day": "unlimited",
        "label": "Enterprise (unlimited)"
    }
}

# --- LICENSE READER ---------------------------------------------------------
def _read_license_key():
    """Read license key from UC Volume or DBFS"""
    for path in LICENSE_PATHS:
        if path.startswith("/") and not path.startswith("/Workspace/..."):
            try:
                with open(path, "r") as f:
                    return f.read().strip()
            except:
                continue
    for path in LICENSE_PATHS:
        if path.startswith("dbfs:"):
            try:
                return dbutils.fs.head(path).strip()
            except:
                continue
    raise RuntimeError(
        "❌ LICENSE ERROR: License file not found.\n"
        "Create: /Volumes/sap_dev/license/config/LICENSE_KEY.txt\n"
        "Contact: support@skillaipath.com"
    )

# --- VALIDATOR --------------------------------------------------------------
def validate_license():
    """
    Validates SkillAIPath license key.
    FREE tier: Local validation, no API call, fully portable.
    PAID tier: HMAC signature + expiry validation.
    Returns: tier string (FREE, FAST, COMP, etc.)
    """
    key = _read_license_key()
    parts = key.split('-')
    if len(parts) != 5 or parts[0] != 'SKILLAI':
        raise RuntimeError("❌ Invalid license key format")

    tier = parts[1]
    customer_id = parts[2]
    expiry_str = parts[3]
    signature = parts[4]

    # FREE TIER: Local validation only (portable, offline)
    if tier == "FREE":
        print(f"✅ License: FREE tier (limited features)")
        return "FREE"

    # PAID TIER: Validate HMAC signature
    payload = f"{tier}|{customer_id}|{expiry_str}"
    expected_sig = hmac.new(SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()[:12]

    if not hmac.compare_digest(signature, expected_sig):
        raise RuntimeError("❌ Invalid license signature")

    # Check expiry
    expiry_date = datetime.strptime(expiry_str, "%Y%m%d")
    if datetime.now() > expiry_date:
        raise RuntimeError(f"❌ License expired on {expiry_date.strftime('%Y-%m-%d')}")

    print(f"✅ License: {tier} tier (expires {expiry_date.strftime('%Y-%m-%d')})")
    return tier

# --- FEATURE GATE CHECK -----------------------------------------------------
def check_job_access(job_name, tier):
    """Check if tier has access to this job. Raises RuntimeError if blocked."""
    gates = FEATURE_GATES.get(tier, FEATURE_GATES["FREE"])
    if gates["jobs"] == "all":
        return True
    if job_name not in gates["jobs"]:
        raise RuntimeError(
            f"❌ Job '{job_name}' requires higher tier.\n"
            f"Your tier: {tier} ({gates['label']})\n"
            f"Upgrade: https://skillaipath.com/pricing"
        )
    return True

# --- RUN VALIDATION (auto-executes on import/copy) --------------------------
LICENSE_TIER = validate_license()
