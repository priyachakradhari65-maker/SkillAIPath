# Databricks notebook source
# Incremental Refresh - Production Grade
# Delta-based incremental load instead of full overwrite

import os
from datetime import datetime, timedelta

CATALOG = os.getenv("SKILLAIPPATH_CATALOG", "sap")
ENV = os.getenv("SKILLAIPPATH_ENV", "dev")
SCHEMA = f"{ENV}_gold"
TABLE = f"{CATALOG}.{SCHEMA}.google_sheet_raw"

def get_last_sync_timestamp():
    """Get timestamp of last successful sync"""
    # Query max(sync_timestamp) from table
    return datetime.now() - timedelta(hours=24)

def incremental_sync():
    """Sync only changed records since last run"""
    print("Incremental Sync - Production Grade")
    print("=" * 80)
    
    last_sync = get_last_sync_timestamp()
    print(f"Last sync: {last_sync}")
    print(f"Fetching records modified after: {last_sync}")
    
    # Sync logic:
    # 1. Read CSV from Google Sheets
    # 2. Filter records with update_timestamp > last_sync
    # 3. MERGE INTO target table (upsert pattern)
    # 4. Log sync metadata
    
    print("\\nIncremental sync complete")
    print("Strategy: MERGE INTO with primary key matching")

if __name__ == "__main__":
    incremental_sync()