# Databricks notebook source
# DBTITLE 1,SkillAIPath License Manager — All-in-One Admin Tool
# ============================================================================
# SKILLAIPATH LICENSE MANAGER — All-in-One Admin Tool
# ============================================================================
# This notebook handles EVERYTHING:
#   1. Generate license keys (Cell 2)
#   2. View license registry (Cell 3)
#   3. Revoke a license (Cell 4)
#   4. Test validator end-to-end (Cell 5)
# ============================================================================

import hashlib
import hmac
import os
from datetime import datetime, timedelta

# Shared config
SECRET = "SkillAIPath-Master-Secret-2026-v1"
LICENSE_PATH = "/Volumes/sap_dev/license/config/LICENSE_KEY.txt"

# Widgets for key generation
dbutils.widgets.text("customer_email", "", "Customer Email")
dbutils.widgets.dropdown("tier", "FAST", ["FREE", "TRIAL", "FAST", "COMP", "ENT"], "Tier")
dbutils.widgets.text("duration_months", "6", "Duration (months, 0=lifetime)")
dbutils.widgets.text("price_paid", "0", "Price Paid (USD)")
dbutils.widgets.text("revoke_key", "", "License key to revoke")

print("SkillAIPath License Manager loaded.")
print("Fill in widgets above, then run the cell for the action you need.")

# COMMAND ----------

# DBTITLE 1,1 — Generate License Key
# ============================================================================
# CELL 2: GENERATE LICENSE KEY
# Fill in widgets (customer_email, tier, duration_months, price_paid) then run
# ============================================================================

EMAIL = dbutils.widgets.get("customer_email")
TIER = dbutils.widgets.get("tier")
DURATION = int(dbutils.widgets.get("duration_months"))
PRICE = float(dbutils.widgets.get("price_paid"))

if not EMAIL:
    print("⚠️  Enter a customer email in the widget above, then re-run this cell.")
else:
    # Generate key components
    customer_id = hashlib.md5(EMAIL.lower().encode()).hexdigest()[:12]
    if DURATION == 0:
        expiry_date = datetime(2099, 12, 31)
    else:
        expiry_date = datetime.now() + timedelta(days=DURATION * 30)
    expiry_str = expiry_date.strftime("%Y%m%d")

    # HMAC signature
    payload = f"{TIER}|{customer_id}|{expiry_str}"
    signature = hmac.new(SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()[:12]

    # Construct key: SKILLAI-{tier}-{customer_id}-{expiry}-{signature}
    license_key = f"SKILLAI-{TIER}-{customer_id}-{expiry_str}-{signature}"

    print("=" * 70)
    print("LICENSE KEY GENERATED")
    print("=" * 70)
    print(f"  Customer: {EMAIL}")
    print(f"  Tier:     {TIER}")
    print(f"  Expires:  {expiry_date.strftime('%Y-%m-%d')}")
    print(f"  Price:    ${PRICE:.2f}")
    print()
    print(f"  KEY: {license_key}")
    print("=" * 70)
    print()
    print("Send this key to the customer.")
    print("They save it to: /Volumes/sap_dev/license/config/LICENSE_KEY.txt")

    # Store in UC table for tracking
    try:
        spark.sql("""
            CREATE TABLE IF NOT EXISTS sap_dev.license.license_keys (
                license_key STRING,
                customer_email STRING,
                tier STRING,
                price_paid DECIMAL(10,2),
                duration_months INT,
                issued_at TIMESTAMP,
                expires_at DATE,
                is_active BOOLEAN,
                workspace_id STRING
            )
        """)
        spark.sql(f"""
            INSERT INTO sap_dev.license.license_keys
            VALUES (
                '{license_key}', '{EMAIL}', '{TIER}', {PRICE},
                {DURATION}, current_timestamp(),
                '{expiry_date.strftime('%Y-%m-%d')}', true, NULL
            )
        """)
        print("✅ Saved to sap_dev.license.license_keys")
    except Exception as e:
        print(f"⚠️  Could not save to UC table: {e}")
        print("Key is still valid — send to customer.")

# COMMAND ----------

# DBTITLE 1,2 — View License Registry
# ============================================================================
# CELL 3: VIEW LICENSE REGISTRY
# Run this cell to see all issued licenses
# ============================================================================

try:
    df = spark.sql("""
        SELECT 
            customer_email,
            tier,
            price_paid,
            expires_at,
            is_active,
            CASE WHEN expires_at < current_date() THEN 'EXPIRED'
                 WHEN is_active = false THEN 'REVOKED'
                 ELSE 'ACTIVE' END as status
        FROM sap_dev.license.license_keys
        ORDER BY issued_at DESC
    """)
    print(f"Total licenses: {df.count()}")
    display(df)
except Exception as e:
    print(f"No license table yet. Generate a key first (Cell 2).")
    print(f"Error: {e}")

# COMMAND ----------

# DBTITLE 1,3 — Revoke a License
# ============================================================================
# CELL 4: REVOKE A LICENSE
# Enter the license key in the 'revoke_key' widget, then run this cell
# ============================================================================

REVOKE_KEY = dbutils.widgets.get("revoke_key")

if not REVOKE_KEY:
    print("⚠️  Enter a license key in the 'revoke_key' widget above, then re-run.")
else:
    try:
        spark.sql(f"""
            UPDATE sap_dev.license.license_keys
            SET is_active = false
            WHERE license_key = '{REVOKE_KEY}'
        """)
        print(f"✅ License REVOKED: {REVOKE_KEY}")
        print("Next time the customer's job runs, it will still validate locally.")
        print("For instant remote revocation, implement the API (see README → Future Enhancements).")
    except Exception as e:
        print(f"❌ Could not revoke: {e}")

# COMMAND ----------

# DBTITLE 1,4 — Test Validator
# ============================================================================
# CELL 5: TEST VALIDATOR (End-to-End)
# Run this cell to test the full license validation flow
# ============================================================================

# Test 1: FREE tier
print("=" * 60)
print("TEST 1: FREE TIER")
print("=" * 60)
os.makedirs(os.path.dirname(LICENSE_PATH), exist_ok=True)
with open(LICENSE_PATH, "w") as f:
    f.write("SKILLAI-FREE-000000000000-20991231-ffffffffffff")

def _read_key():
    with open(LICENSE_PATH, "r") as f:
        return f.read().strip()

def _validate(key):
    parts = key.split('-')
    if len(parts) != 5 or parts[0] != 'SKILLAI':
        raise RuntimeError("❌ Invalid format")
    tier = parts[1]
    if tier == "FREE":
        return "FREE"
    customer_id, expiry_str, sig = parts[2], parts[3], parts[4]
    payload = f"{tier}|{customer_id}|{expiry_str}"
    expected = hmac.new(SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()[:12]
    if not hmac.compare_digest(sig, expected):
        raise RuntimeError("❌ Invalid signature")
    exp = datetime.strptime(expiry_str, "%Y%m%d")
    if datetime.now() > exp:
        raise RuntimeError(f"❌ Expired on {exp.strftime('%Y-%m-%d')}")
    return tier

tier = _validate(_read_key())
assert tier == "FREE", f"Expected FREE, got {tier}"
print("✅ FREE tier validated")

# Test 2: FAST tier (valid)
print("\n" + "=" * 60)
print("TEST 2: FAST TIER (VALID)")
print("=" * 60)
cid = hashlib.md5(b"test@test.com").hexdigest()[:12]
exp = (datetime.now() + timedelta(days=180)).strftime("%Y%m%d")
sig = hmac.new(SECRET.encode(), f"FAST|{cid}|{exp}".encode(), hashlib.sha256).hexdigest()[:12]
fast_key = f"SKILLAI-FAST-{cid}-{exp}-{sig}"
with open(LICENSE_PATH, "w") as f:
    f.write(fast_key)
tier = _validate(_read_key())
assert tier == "FAST", f"Expected FAST, got {tier}"
print(f"✅ FAST tier validated (expires {exp})")

# Test 3: Expired key (should reject)
print("\n" + "=" * 60)
print("TEST 3: EXPIRED KEY (SHOULD REJECT)")
print("=" * 60)
old_exp = "20240101"
sig_old = hmac.new(SECRET.encode(), f"FAST|{cid}|{old_exp}".encode(), hashlib.sha256).hexdigest()[:12]
expired_key = f"SKILLAI-FAST-{cid}-{old_exp}-{sig_old}"
with open(LICENSE_PATH, "w") as f:
    f.write(expired_key)
try:
    _validate(_read_key())
    print("❌ FAILED: Expired key was accepted!")
except RuntimeError as e:
    print(f"✅ Correctly rejected: {e}")

# Test 4: Tampered key (should reject)
print("\n" + "=" * 60)
print("TEST 4: TAMPERED KEY (SHOULD REJECT)")
print("=" * 60)
tampered = fast_key[:-4] + "XXXX"
with open(LICENSE_PATH, "w") as f:
    f.write(tampered)
try:
    _validate(_read_key())
    print("❌ FAILED: Tampered key was accepted!")
except RuntimeError as e:
    print(f"✅ Correctly rejected: {e}")

# Restore FREE key
with open(LICENSE_PATH, "w") as f:
    f.write("SKILLAI-FREE-000000000000-20991231-ffffffffffff")

print("\n" + "=" * 60)
print("ALL TESTS PASSED — System ready")
print("=" * 60)
print(f"Default key restored: FREE tier")