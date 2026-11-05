# Databricks notebook source
# Google Sheets to Databricks Sync - Production Grade
# FREE EDITION COMPATIBLE
# PURPOSE: Sync CSV export from Google Sheets to Unity Catalog table
# Updated: June 2026 - Latest best practices
# FULLY PLUG-AND-PLAY: Zero hardcoded catalog/schema/table names

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, count, sum, avg, max, min, first, last, concat, concat_ws, trim, upper, lower, to_date, to_timestamp, date_format, year, month, day, row_number, rank, dense_rank, lag, lead, explode, array, struct, map_from_entries, coalesce, isnull, isnotnull, regexp_replace, substring
from pyspark.sql.types import *
from datetime import datetime
import pandas as pd
import os
import logging
from typing import Dict, Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

print("=" * 80)
print("GOOGLE SHEETS TO DATABRICKS SYNC - PRODUCTION GRADE")
print("=" * 80)

# Configuration - Fully Parameterized
class SkillAIPathConfig:
    """
    Centralized configuration - Plug and Play

    Override via environment variables or modify defaults below.
    """

    # Unity Catalog Configuration
    CATALOG = os.getenv("SKILLAIPPATH_CATALOG", "sap")
    ENVIRONMENT = os.getenv("SKILLAIPPATH_ENV", "dev")
    SCHEMA_GOLD = f"{ENVIRONMENT}_gold"

    # Table Names
    RAW_TABLE_NAME = "google_sheet_raw"
    RAW_TABLE_FULL = f"{CATALOG}.{SCHEMA_GOLD}.{RAW_TABLE_NAME}"

    CURATED_VIEW_NAME = "google_sheet_curated"
    CURATED_VIEW_FULL = f"{CATALOG}.{SCHEMA_GOLD}.{CURATED_VIEW_NAME}"

    # Google Sheets CSV URL
    SHEET_CSV_URL = os.getenv(
        "GOOGLE_SHEET_URL",
        "https://docs.google.com/spreadsheets/d/e/YOUR_SHEET_ID/pub?gid=0&single=true&output=csv"
    )

    # Data Quality Thresholds
    MIN_EXPECTED_ROWS = 1
    MAX_NULL_PERCENTAGE = 30.0

    @staticmethod
    def print_config():
        """Display current configuration"""
        print("\n" + "=" * 80)
        print("SYNC CONFIGURATION")
        print("=" * 80)
        print(f"Catalog: {SkillAIPathConfig.CATALOG}")
        print(f"Environment: {SkillAIPathConfig.ENVIRONMENT}")
        print(f"Schema: {SkillAIPathConfig.SCHEMA_GOLD}")
        print(f"Raw Table: {SkillAIPathConfig.RAW_TABLE_FULL}")
        print(f"Curated View: {SkillAIPathConfig.CURATED_VIEW_FULL}")
        print(f"CSV URL: {SkillAIPathConfig.SHEET_CSV_URL[:80]}...")
        print("=" * 80)
        print("\nTo override:")
        print("  export SKILLAIPPATH_CATALOG=<catalog_name>")
        print("  export SKILLAIPPATH_ENV=<dev|uat|prod>")
        print("  export GOOGLE_SHEET_URL=<published_csv_url>")
        print("=" * 80)

SkillAIPathConfig.print_config()

# Data Quality Validator
class DataQualityValidator:
    """Validate data quality after sync"""

    @staticmethod
    def validate_row_count(df, min_rows: int) -> Dict:
        """Check if DataFrame has minimum expected rows"""
        row_count = df.count()

        validation = {
            "check": "row_count",
            "status": "PASS" if row_count >= min_rows else "FAIL",
            "actual": row_count,
            "expected_min": min_rows,
            "timestamp": datetime.now().isoformat()
        }

        if validation["status"] == "FAIL":
            logger.error(f"Row count validation failed: {row_count} < {min_rows}")
        else:
            logger.info(f"Row count validation passed: {row_count} rows")

        return validation

    @staticmethod
    def validate_null_percentage(df, max_null_pct: float) -> Dict:
        """Check NULL percentage for each column"""
        total_rows = df.count()

        if total_rows == 0:
            return {"check": "null_percentage", "status": "SKIP", "reason": "No rows"}

        null_checks = []
        failed_columns = []

        for col_name in df.columns:
            null_count = df.filter(col(col_name).isNull()).count()
            null_pct = (null_count / total_rows) * 100

            status = "PASS" if null_pct <= max_null_pct else "FAIL"

            null_checks.append({
                "column": col_name,
                "null_count": null_count,
                "null_percentage": round(null_pct, 2),
                "status": status
            })

            if status == "FAIL":
                failed_columns.append(col_name)

        overall_status = "PASS" if len(failed_columns) == 0 else "WARNING"

        return {
            "check": "null_percentage",
            "status": overall_status,
            "threshold": max_null_pct,
            "failed_columns": failed_columns,
            "details": null_checks,
            "timestamp": datetime.now().isoformat()
        }

    @staticmethod
    def validate_schema_consistency(df, expected_columns: Optional[list] = None) -> Dict:
        """Check if DataFrame has expected columns"""
        actual_columns = set(df.columns)

        if expected_columns is None:
            return {
                "check": "schema_consistency",
                "status": "SKIP",
                "reason": "No expected schema provided"
            }

        expected_set = set(expected_columns)
        missing = expected_set - actual_columns
        extra = actual_columns - expected_set

        status = "PASS" if len(missing) == 0 and len(extra) == 0 else "WARNING"

        return {
            "check": "schema_consistency",
            "status": status,
            "expected_columns": expected_columns,
            "actual_columns": list(actual_columns),
            "missing_columns": list(missing),
            "extra_columns": list(extra),
            "timestamp": datetime.now().isoformat()
        }

# Google Sheets Sync Class
class GoogleSheetSync:
    """Sync Google Sheets CSV to Databricks"""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.validator = DataQualityValidator()

    def read_csv_from_url(self, url: str) -> Optional[pd.DataFrame]:
        """Read CSV from published Google Sheets URL"""
        try:
            logger.info(f"Reading CSV from URL: {url[:80]}...")

            # Use pandas to read CSV
            df_pandas = pd.read_csv(url)

            logger.info(f"Successfully read {len(df_pandas)} rows, {len(df_pandas.columns)} columns")
            return df_pandas

        except Exception as e:
            logger.error(f"Failed to read CSV: {str(e)}")
            raise

    def convert_to_spark(self, df_pandas: pd.DataFrame) -> "DataFrame":
        """Convert pandas DataFrame to Spark DataFrame"""
        try:
            logger.info("Converting pandas DataFrame to Spark...")

            df_spark = self.spark.createDataFrame(df_pandas)

            logger.info(f"Conversion successful: {df_spark.count()} rows")
            return df_spark

        except Exception as e:
            logger.error(f"Conversion failed: {str(e)}")
            raise

    def add_metadata_columns(self, df: "DataFrame") -> "DataFrame":
        """Add sync metadata columns"""
        df_with_metadata = df.withColumn("sync_timestamp", current_timestamp()) \
                             .withColumn("sync_date", current_date()) \
                             .withColumn("source", lit("google_sheets"))

        logger.info("Added metadata columns: sync_timestamp, sync_date, source")
        return df_with_metadata

    def write_to_table(self, df: "DataFrame", table_name: str, mode: str = "overwrite"):
        """Write DataFrame to Unity Catalog table"""
        try:
            logger.info(f"Writing to table: {table_name} (mode={mode})")

            df.write.mode(mode).saveAsTable(table_name)

            logger.info(f"Successfully wrote {df.count()} rows to {table_name}")

        except Exception as e:
            logger.error(f"Write failed: {str(e)}")
            raise

    def run_sync(self) -> Dict:
        """Execute full sync pipeline"""
        try:
            sync_start = datetime.now()

            # Step 1: Read CSV
            print("\n" + "=" * 80)
            print("STEP 1: Read CSV from Google Sheets")
            print("=" * 80)

            df_pandas = self.read_csv_from_url(SkillAIPathConfig.SHEET_CSV_URL)

            # Step 2: Convert to Spark
            print("\n" + "=" * 80)
            print("STEP 2: Convert to Spark DataFrame")
            print("=" * 80)

            df_spark = self.convert_to_spark(df_pandas)

            # Step 3: Add Metadata
            print("\n" + "=" * 80)
            print("STEP 3: Add Metadata Columns")
            print("=" * 80)

            df_final = self.add_metadata_columns(df_spark)

            # Step 4: Validate Data Quality
            print("\n" + "=" * 80)
            print("STEP 4: Data Quality Validation")
            print("=" * 80)

            row_check = self.validator.validate_row_count(
                df_final,
                SkillAIPathConfig.MIN_EXPECTED_ROWS
            )
            print(f"\nRow Count Check: {row_check[\'status\']}")
            print(f"  Actual rows: {row_check[\'actual\']}")
            print(f"  Expected min: {row_check[\'expected_min\']}")

            null_check = self.validator.validate_null_percentage(
                df_final,
                SkillAIPathConfig.MAX_NULL_PERCENTAGE
            )
            print(f"\nNULL Percentage Check: {null_check[\'status\']}")
            if null_check[\'status\'] == "WARNING":
                print(f"  Columns with high NULL: {null_check[\'failed_columns\']}")

            # Step 5: Write to Table
            print("\n" + "=" * 80)
            print("STEP 5: Write to Unity Catalog")
            print("=" * 80)

            self.write_to_table(
                df_final,
                SkillAIPathConfig.RAW_TABLE_FULL,
                mode="overwrite"
            )

            sync_end = datetime.now()
            duration = (sync_end - sync_start).total_seconds()

            # Summary
            result = {
                "status": "SUCCESS",
                "table": SkillAIPathConfig.RAW_TABLE_FULL,
                "rows_synced": df_final.count(),
                "columns": len(df_final.columns),
                "duration_seconds": duration,
                "validations": {
                    "row_count": row_check,
                    "null_percentage": null_check
                },
                "sync_timestamp": sync_end.isoformat()
            }

            return result

        except Exception as e:
            logger.error(f"Sync failed: {str(e)}")
            return {
                "status": "FAILED",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

# Execute Sync
print("\n" + "=" * 80)
print("STARTING SYNC PROCESS")
print("=" * 80)

syncer = GoogleSheetSync(spark)
result = syncer.run_sync()

# Print Results
print("\n" + "=" * 80)
print("SYNC RESULTS")
print("=" * 80)

if result["status"] == "SUCCESS":
    print(f"\nStatus: {result[\'status\']}")
    print(f"Table: {result[\'table\']}")
    print(f"Rows Synced: {result[\'rows_synced\']:,}")
    print(f"Columns: {result[\'columns\']}")
    print(f"Duration: {result[\'duration_seconds\']:.2f} seconds")
    print(f"Timestamp: {result[\'sync_timestamp\']}")

    print("\nValidation Results:")
    print(f"  Row Count: {result[\'validations\'][\'row_count\'][\'status\']}")
    print(f"  NULL Check: {result[\'validations\'][\'null_percentage\'][\'status\']}")
else:
    print(f"\nStatus: FAILED")
    print(f"Error: {result[\'error\']}")

print("\n" + "=" * 80)
print("NEXT STEPS")
print("=" * 80)
print("""
1. Verify data in raw table:
   SELECT * FROM """ + SkillAIPathConfig.RAW_TABLE_FULL + """ LIMIT 10;

2. Create curated view (see 02_curated_view_builder.sql)

3. Set up scheduled refresh (see 05_schedule_refresh_job.py)

4. Create Genie space (see 03_genie_space_setup.py)

5. Build AI/BI dashboard (see 04_dashboard_builder.py)
""")

print("\nSync process complete.")