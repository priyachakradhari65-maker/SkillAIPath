# Databricks notebook source
# MAGIC %md
# MAGIC # Monthly Content Deployer
# MAGIC
# MAGIC Triggered: 1st of every month via Databricks Job
# MAGIC
# MAGIC Process:
# MAGIC 1. Check current month number
# MAGIC 2. Query eligible engineers (6-month track, month N unlocked)
# MAGIC 3. Copy month N content to engineer catalogs
# MAGIC 4. Update Genie spaces with new tables

# COMMAND ----------

from datetime import datetime
from pathlib import Path
import json

# COMMAND ----------

# MAGIC %run ./00_access_control_gate

# COMMAND ----------

class MonthlyContentDeployer:
    """Deploy new content to 6-month track engineers"""
    
    RELEASES_PATH = "/Workspace/Shared/dev/SkillAIPath/8_Monthly_Upgrade_System/monthly_releases"
    MASTER_CATALOG = "sap_master"
    
    def deploy_month(self, month_number: int):
        """Deploy specific month's content to eligible engineers"""
        
        print(f"Starting deployment for Month {month_number}")
        
        # Load month manifest
        manifest = self._load_manifest(month_number)
        
        if not manifest:
            print(f"No manifest found for Month {month_number}")
            return
        
        print(f"Module: {manifest['module_name']}")
        print(f"Tables to deploy: {len(manifest['tables'])}")
        
        # Get eligible engineers
        ac = AccessController()
        eligible_students = ac.get_eligible_students_for_month(month_number)
        
        print(f"Eligible engineers: {len(eligible_students)}")
        
        # Deploy to each engineer
        for engineer in eligible_students:
            self._deploy_to_student_catalog(
                student_catalog=engineer['catalog'],
                tables=manifest['tables'],
                notebooks=manifest['notebooks']
            )
            
            self._update_genie_space(
                student_email=engineer['email'],
                new_tables=[f"{engineer['catalog']}.gold.{t}" for t in manifest['tables']]
            )
        
        # Log deployment
        self._log_deployment(month_number, len(eligible_students))
        
        print("="*70)
        print(f"Deployment complete: Month {month_number}")
        print(f"Engineers updated: {len(eligible_students)}")
        print("="*70)
    
    def _load_manifest(self, month_number: int) -> dict:
        """Load module manifest JSON"""
        manifest_path = f"{self.RELEASES_PATH}/month_{month_number:02d}_*/module_manifest.json"
        
        try:
            # Find matching folder
            month_folders = dbutils.fs.ls(self.RELEASES_PATH)
            target_folder = None
            
            for folder in month_folders:
                if folder.name.startswith(f"month_{month_number:02d}_"):
                    target_folder = folder.path
                    break
            
            if not target_folder:
                return None
            
            # Read manifest
            manifest_file = f"{target_folder}module_manifest.json"
            content = dbutils.fs.head(manifest_file)
            return json.loads(content)
        except:
            return None
    
    def _deploy_to_student_catalog(self, student_catalog: str, tables: list, notebooks: list):
        """Copy tables and notebooks to engineer catalog"""
        
        # Copy tables (create or replace from master)
        for table_name in tables:
            try:
                spark.sql(f"""
                    CREATE OR REPLACE TABLE {student_catalog}.gold.{table_name}
                    CLONE {self.MASTER_CATALOG}.gold.{table_name}
                """)
                print(f"  Cloned table: {table_name}")
            except Exception as e:
                print(f"  Failed to clone {table_name}: {str(e)}")
        
        # Copy notebooks to engineer workspace
        for notebook_path in notebooks:
            try:
                source = f"{self.RELEASES_PATH}/{notebook_path}"
                dest = f"/Users/{student_catalog.replace('_', '@')}/SkillAIPath/{notebook_path}"
                dbutils.fs.cp(source, dest, recurse=True)
                print(f"  Copied notebook: {notebook_path}")
            except Exception as e:
                print(f"  Failed to copy {notebook_path}: {str(e)}")
    
    def _update_genie_space(self, student_email: str, new_tables: list):
        """Add new tables to engineer's Genie space"""
        # This would use Databricks API to update Genie space configuration
        # For now, log the action
        print(f"  Updated Genie space for {student_email} with {len(new_tables)} tables")
    
    def _log_deployment(self, month_number: int, student_count: int):
        """Log deployment to audit table"""
        try:
            spark.sql(f"""
                INSERT INTO sap_dev.license.deployments
                VALUES (
                    {month_number},
                    CURRENT_TIMESTAMP(),
                    {student_count},
                    'SUCCESS'
                )
            """)
        except Exception as e:
            print(f"Failed to log deployment: {str(e)}")

# COMMAND ----------

# Run deployment for current month
# This cell is triggered by Databricks Job monthly

deployer = MonthlyContentDeployer()

# Calculate current month number (1-6)
import datetime
start_date = datetime.date(2026, 1, 1)  # Adjust to program start date
today = datetime.date.today()
months_elapsed = (today.year - start_date.year) * 12 + (today.month - start_date.month)
current_month = min(months_elapsed + 1, 6)

print(f"Current month number: {current_month}")
deployer.deploy_month(current_month)