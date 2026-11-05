# Databricks notebook source
# MAGIC %md
# MAGIC # Access Control Gate - License Verification
# MAGIC
# MAGIC Purpose: Verify license key and determine content access level
# MAGIC Queries: sap_dev.license.license_keys + sap_dev.license.activations
# MAGIC
# MAGIC Usage:
# MAGIC ```python
# MAGIC ac = AccessController()
# MAGIC access_info = ac.check_engineer_access("engineer@example.com")
# MAGIC ```

# COMMAND ----------

from datetime import datetime, date
from pyspark.sql.functions import col, current_date, datediff

# COMMAND ----------

class AccessController:
    """Verify license key and determine content access level"""
    
    LICENSE_TABLE = "sap_dev.license.license_keys"
    ACTIVATION_TABLE = "sap_dev.license.activations"
    
    def check_student_access(self, student_email: str) -> dict:
        """
        Verify license for a customer by email
        
        Returns:
            {
                "authorized": bool,
                "tier": str,
                "expiry_date": date,
                "months_unlocked": int,
                "github_username": str
            }
        """
        
        license_df = spark.sql(f"""
            SELECT 
                l.license_key,
                l.customer_email,
                l.tier,
                l.max_engineers,
                l.issued_at,
                l.expires_at,
                l.is_active,
                a.workspace_id,
                a.activated_at,
                a.is_primary
            FROM {self.LICENSE_TABLE} l
            LEFT JOIN {self.ACTIVATION_TABLE} a ON l.license_key = a.license_key
            WHERE l.customer_email = '{student_email}'
            AND l.is_active = TRUE
            ORDER BY l.issued_at DESC
            LIMIT 1
        """)
        
        if license_df.count() == 0:
            return {
                "authorized": False,
                "tier": "FREE",
                "expiry_date": None,
                "months_unlocked": 1,
                "github_username": None
            }
        
        license_row = license_df.first()
        
        # Check expiry
        if license_row.expires_at and license_row.expires_at < datetime.now():
            return {
                "authorized": False,
                "tier": "EXPIRED",
                "expiry_date": license_row.expires_at,
                "months_unlocked": 1,
                "github_username": None
            }
        
        # Check if activated (workspace binding)
        if not license_row.workspace_id:
            return {
                "authorized": False,
                "tier": "NOT_ACTIVATED",
                "expiry_date": license_row.expires_at,
                "months_unlocked": 1,
                "github_username": None
            }
        
        # Calculate months unlocked based on issued_at
        if license_row.duration_months == 0:
            months_unlocked = 999  # Lifetime
        else:
            months_elapsed = self._calculate_months_since_purchase(license_row.issued_at)
            months_unlocked = min(months_elapsed, license_row.duration_months)
        
        return {
            "authorized": True,
            "tier": license_row.tier,
            "expiry_date": license_row.expires_at,
            "months_unlocked": months_unlocked,
            "github_username": None
        }
    
    def _calculate_months_since_purchase(self, purchase_date) -> int:
        """Calculate completed months since license issued"""
        if isinstance(purchase_date, datetime):
            pd = purchase_date.date()
        elif isinstance(purchase_date, date):
            pd = purchase_date
        else:
            return 1
        today = date.today()
        months = (today.year - pd.year) * 12 + (today.month - pd.month)
        return max(1, months + 1)
    
    def get_eligible_students_for_month(self, month_number: int) -> list:
        """Get engineers eligible for specific month's content"""
        
        engineers = spark.sql(f"""
            SELECT 
                l.customer_email,
                l.tier,
                l.max_engineers,
                a.workspace_id,
                a.workspace_url
            FROM {self.LICENSE_TABLE} l
            JOIN {self.ACTIVATION_TABLE} a ON l.license_key = a.license_key
            WHERE l.is_active = TRUE
            AND l.expires_at >= CURRENT_DATE()
            AND a.is_primary = TRUE
        """).collect()
        
        return [
            {
                "email": row.customer_email,
                "github": None,
                "catalog": None,
                "tier": row.tier
            }
            for row in engineers
        ]
    
    def check_workspace_access(self) -> dict:
        """
        Check if THIS workspace has a valid activated license.
        Uses spark conf to get workspace ID.
        """
        workspace_id = spark.conf.get("spark.databricks.clusterUsageTags.workspaceId", "unknown")
        
        license_df = spark.sql(f"""
            SELECT 
                l.license_key,
                l.customer_email,
                l.customer_name,
                l.tier,
                l.max_engineers,
                l.expires_at,
                l.is_active
            FROM {self.LICENSE_TABLE} l
            JOIN {self.ACTIVATION_TABLE} a ON l.license_key = a.license_key
            WHERE a.workspace_id = '{workspace_id}'
            AND a.is_primary = TRUE
            AND l.is_active = TRUE
            ORDER BY l.issued_at DESC
            LIMIT 1
        """)
        
        if license_df.count() == 0:
            return {
                "authorized": False,
                "tier": "NO_LICENSE",
                "workspace_id": workspace_id
            }
        
        row = license_df.first()
        expired = row.expires_at and row.expires_at < datetime.now() if row.expires_at else False
        
        return {
            "authorized": not expired,
            "tier": row.tier if not expired else "EXPIRED",
            "customer": row.customer_name,
            "max_engineers": row.max_engineers,
            "expires_at": row.expires_at,
            "workspace_id": workspace_id
        }

# COMMAND ----------

# Example usage: Check by email
ac = AccessController()
access_info = ac.check_student_access("engineer@example.com")

print("="*70)
print("ACCESS CONTROL CHECK (by email)")
print("="*70)
print(f"Authorized: {access_info['authorized']}")
print(f"Track Tier: {access_info['tier']}")
print(f"Expiry Date: {access_info['expiry_date']}")
print(f"Months Unlocked: {access_info['months_unlocked']}")
print("="*70)

# COMMAND ----------

# Example usage: Check by workspace
workspace_access = ac.check_workspace_access()

print("="*70)
print("ACCESS CONTROL CHECK (by workspace)")
print("="*70)
print(f"Authorized: {workspace_access['authorized']}")
print(f"Tier: {workspace_access['tier']}")
if workspace_access.get('customer'):
    print(f"Customer: {workspace_access['customer']}")
    print(f"Max Engineers: {workspace_access['max_engineers']}")
    print(f"Expires: {workspace_access['expires_at']}")
print("="*70)
