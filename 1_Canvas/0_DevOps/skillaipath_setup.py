# SkillAIPath Setup & Clean Slate — Run FIRST in any new workspace
# 1) Creates license UC Volume + writes key
# 2) Validates license
# 3) Cleans data catalog (fresh slate)
# 4) Verifies ready for master job

try:
    dbutils.widgets.text("license_key", "SKILLAI-FREE-000000000000-20991231-ffffffffffff", "License Key")
    dbutils.widgets.dropdown("environment", "dev", ["dev", "main"], "Environment")
    dbutils.widgets.dropdown("reset_mode", "truncate", ["truncate", "drop_cascade", "skip"], "Reset Mode")
except:
    pass

import hashlib, hmac, os
from datetime import datetime

LICENSE_KEY = dbutils.widgets.get("license_key").strip()
ENVIRONMENT = dbutils.widgets.get("environment")
RESET_MODE = dbutils.widgets.get("reset_mode")
SECRET = "SkillAIPath-Master-Secret-2026-v1"
LICENSE_VOLUME_PATH = "/Volumes/sap_dev/license/config/LICENSE_KEY.txt"
CATALOG = f"sap_{ENVIRONMENT}"

print("=" * 70)
print("SKILLAIPATH SETUP & CLEAN SLATE")
print("=" * 70)
print(f"Environment: {ENVIRONMENT}")
print(f"Reset Mode:  {RESET_MODE}")
print(f"License:     {LICENSE_KEY[:20]}...")
print("=" * 70)

# --- STEP 1: CREATE UC VOLUME + WRITE LICENSE KEY ---
print("\n1. License Setup")
try:
    spark.sql("CREATE CATALOG IF NOT EXISTS sap_dev")
    spark.sql("CREATE SCHEMA IF NOT EXISTS sap_dev.license")
    spark.sql("CREATE VOLUME IF NOT EXISTS sap_dev.license.config")
    os.makedirs(os.path.dirname(LICENSE_VOLUME_PATH), exist_ok=True)
    with open(LICENSE_VOLUME_PATH, "w") as f:
        f.write(LICENSE_KEY)
    with open(LICENSE_VOLUME_PATH, "r") as f:
        assert f.read().strip() == LICENSE_KEY
    print("   OK: UC Volume + key written")
except Exception as e:
    print(f"   FAIL: {e}")
    raise

# --- STEP 2: VALIDATE LICENSE ---
print("\n2. License Validation")
parts = LICENSE_KEY.split('-')
if len(parts) != 5 or parts[0] != 'SKILLAI':
    raise RuntimeError("Invalid license key format")
tier = parts[1]
if tier == "FREE":
    print(f"   OK: FREE tier (portable, offline)")
else:
    payload = f"{tier}|{parts[2]}|{parts[3]}"
    sig = hmac.new(SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()[:12]
    if not hmac.compare_digest(parts[4], sig):
        raise RuntimeError("Invalid license signature")
    exp = datetime.strptime(parts[3], "%Y%m%d")
    if datetime.now() > exp:
        raise RuntimeError(f"License expired on {exp.strftime('%Y-%m-%d')}")
    print(f"   OK: {tier} tier (expires {exp.strftime('%Y-%m-%d')})")

# --- STEP 3: CLEAN DATA CATALOG ---
print(f"\n3. Clean Data Catalog ({CATALOG})")
if RESET_MODE == "skip":
    print("   SKIP: reset_mode=skip")
else:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    schemas = ['bronze', 'silver', 'gold', 'staging', 'reference']
    for s in schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{s}")
    if RESET_MODE == "drop_cascade":
        for s in schemas:
            spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{s} CASCADE")
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{s}")
            print(f"   {s}: dropped + recreated")
    else:
        total = 0
        for s in schemas:
            try:
                tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{s}").collect()
                for t in tables:
                    try:
                        spark.sql(f"TRUNCATE TABLE {CATALOG}.{s}.{t.tableName}")
                        total += 1
                    except:
                        pass
                print(f"   {s}: {len(tables)} tables")
            except:
                pass
        print(f"   Total truncated: {total}")

# --- STEP 4: FINAL VERIFICATION ---
print("\n4. Final Verification")
checks = []
try:
    with open(LICENSE_VOLUME_PATH, "r") as f:
        checks.append(("License UC Volume", f.read().strip() == LICENSE_KEY))
except:
    checks.append(("License UC Volume", False))
checks.append(("License validation", True))
try:
    spark.sql(f"USE CATALOG {CATALOG}")
    checks.append((f"Data catalog ({CATALOG})", True))
except:
    checks.append((f"Data catalog ({CATALOG})", False))
try:
    cnt = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.information_schema.schemata WHERE schema_name IN ('bronze','silver','gold')").collect()[0][0]
    checks.append(("Schemas (bronze/silver/gold)", cnt == 3))
except:
    checks.append(("Schemas (bronze/silver/gold)", False))

all_pass = True
for name, result in checks:
    print(f"   {'OK' if result else 'FAIL'}: {name}")
    if not result: all_pass = False

print("\n" + "=" * 70)
if all_pass:
    print("SETUP COMPLETE — READY FOR MASTER JOB")
    print(f"  License: {tier} | Catalog: {CATALOG} | Mode: {RESET_MODE}")
    print("  Next: Run 01_Master_Start_SkillAI")
else:
    print("SETUP INCOMPLETE — Fix errors above")
print("=" * 70)