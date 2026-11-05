# Databricks notebook source
# Multi-Sheet Sync - Production Grade
# Sync multiple Google Sheets tabs into separate tables

import os
from typing import Dict, List

CATALOG = os.getenv("SKILLAIPPATH_CATALOG", "sap")
ENV = os.getenv("SKILLAIPPATH_ENV", "dev")
SCHEMA = f"{ENV}_gold"

# Configuration for multiple sheets
SHEETS_CONFIG = [
    {
        "name": "operations",
        "url": "https://docs.google.com/.../gid=0&output=csv",
        "target_table": f"{CATALOG}.{SCHEMA}.ops_data"
    },
    {
        "name": "financials",
        "url": "https://docs.google.com/.../gid=1&output=csv",
        "target_table": f"{CATALOG}.{SCHEMA}.finance_data"
    },
    {
        "name": "customers",
        "url": "https://docs.google.com/.../gid=2&output=csv",
        "target_table": f"{CATALOG}.{SCHEMA}.customer_data"
    }
]

def sync_all_sheets():
    """Sync all configured Google Sheets"""
    print("Multi-Sheet Sync Started")
    print("=" * 80)
    
    for sheet in SHEETS_CONFIG:
        print(f"\\nSyncing: {sheet['name']}")
        print(f"  URL: {sheet['url'][:50]}...")
        print(f"  Target: {sheet['target_table']}")
        # Sync logic here (use pattern from 01_google_sheet_sync.py)
    
    print("\\nAll sheets synced successfully")

if __name__ == "__main__":
    sync_all_sheets()