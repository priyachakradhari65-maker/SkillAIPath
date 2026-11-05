"""SkillAIPath Monitoring & Observability Configuration

Production-grade configuration manager with:
- Multi-environment support (dev/uat/prod)
- Unity Catalog table mapping
- Dynamic environment detection
- Comprehensive monitoring settings
- Alert configuration
- Table and schema validation

Author: SkillAIPath Platform Team
Last Updated: 2026-07-11
"""

import os
import logging
from typing import Dict, List, Optional
from datetime import datetime

# Setup logging
logger = logging.getLogger(__name__)


class SkillAIPathConfig:
    """Enterprise-grade configuration for SkillAIPath Monitoring & Observability"""
    
    # ========================================
    # ENVIRONMENT DETECTION
    # ========================================
    
    @staticmethod
    def detect_environment() -> str:
        """Auto-detect environment from workspace or env var"""
        env = os.getenv('SKILLAIPPATH_ENV', 'dev').lower()
        
        # Validate environment
        valid_envs = ['dev', 'uat', 'prod']
        if env not in valid_envs:
            logger.warning(f"Invalid environment '{env}', defaulting to 'dev'")
            env = 'dev'
        
        return env
    
    # ========================================
    # UNITY CATALOG CONFIGURATION
    # ========================================
    
    # Current environment
    ENVIRONMENT = detect_environment.__func__()
    
    # Catalog mapping by environment
    CATALOGS = {
        'dev': 'sap_dev',
        'uat': 'sap_uat',
        'prod': 'sap_main'
    }
    
    # Current catalog
    CATALOG = CATALOGS[ENVIRONMENT]
    
    # Schema names (consistent across environments)
    SCHEMA_BRONZE = 'bronze'
    SCHEMA_SILVER = 'silver'
    SCHEMA_GOLD = 'gold'
    SCHEMA_MONITORING = 'monitoring'  # For monitoring results
    
    # ========================================
    # SKILLAIPPATH TABLE REGISTRY
    # ========================================
    
    # Bronze Layer Tables
    BRONZE_TABLES = {
        'sales': 'stg_sales',
        'purchase': 'stg_purchase',
        'stock': 'stg_stock_status'
    }
    
    # Silver Layer Tables - Dimensions
    SILVER_DIMENSIONS = {
        'agent': 'dim_agent',
        'customer': 'dim_customer',
        'date': 'dim_date',
        'payment_mode': 'dim_payment_mode',
        'product': 'dim_product',
        'product_category': 'dim_product_category',
        'retailer': 'dim_retailer',
        'warehouse': 'dim_warehouse',
        'wholesaler': 'dim_wholesaler'
    }
    
    # Silver Layer Tables - Facts
    SILVER_FACTS = {
        'inventory': 'fact_inventory',
        'purchase': 'fact_purchase',
        'sales': 'fact_sales'
    }
    
    # Gold Layer Tables - Marts
    GOLD_MARTS = {
        'customer_360': 'mart_customer_360',
        'product_performance': 'mart_product_performance',
        'rfm_analysis': 'mart_rfm_analysis'
    }
    
    # ========================================
    # TABLE PATH BUILDERS
    # ========================================
    
    @classmethod
    def get_table(cls, layer: str, table_key: str) -> str:
        """Get fully qualified table name
        
        Args:
            layer: 'bronze', 'silver', 'gold', 'monitoring'
            table_key: Key from table registry
            
        Returns:
            Full table path: catalog.schema.table
            
        Example:
            get_table('bronze', 'sales') -> 'sap_dev.bronze.stg_sales'
            get_table('silver', 'sales') -> 'sap_dev.silver.fact_sales'
        """
        schema_map = {
            'bronze': cls.SCHEMA_BRONZE,
            'silver': cls.SCHEMA_SILVER,
            'gold': cls.SCHEMA_GOLD,
            'monitoring': cls.SCHEMA_MONITORING
        }
        
        schema = schema_map.get(layer)
        if not schema:
            raise ValueError(f"Invalid layer: {layer}. Must be one of {list(schema_map.keys())}")
        
        # Get table name from appropriate registry
        table_name = None
        if layer == 'bronze':
            table_name = cls.BRONZE_TABLES.get(table_key)
        elif layer == 'silver':
            table_name = cls.SILVER_FACTS.get(table_key) or cls.SILVER_DIMENSIONS.get(table_key)
        elif layer == 'gold':
            table_name = cls.GOLD_MARTS.get(table_key)
        elif layer == 'monitoring':
            table_name = table_key  # Direct table name for monitoring schema
        
        if not table_name:
            raise ValueError(f"Unknown table key '{table_key}' for layer '{layer}'")
        
        return f"{cls.CATALOG}.{schema}.{table_name}"
    
    @classmethod
    def get_schema(cls, layer: str) -> str:
        """Get fully qualified schema name
        
        Example:
            get_schema('bronze') -> 'sap_dev.bronze'
        """
        schema_map = {
            'bronze': cls.SCHEMA_BRONZE,
            'silver': cls.SCHEMA_SILVER,
            'gold': cls.SCHEMA_GOLD,
            'monitoring': cls.SCHEMA_MONITORING
        }
        schema = schema_map.get(layer)
        if not schema:
            raise ValueError(f"Invalid layer: {layer}")
        return f"{cls.CATALOG}.{schema}"
    
    @classmethod
    def get_all_tables(cls, layer: Optional[str] = None) -> Dict[str, str]:
        """Get all tables for a layer or all layers
        
        Args:
            layer: Optional layer filter ('bronze', 'silver', 'gold')
            
        Returns:
            Dict mapping table keys to full paths
        """
        all_tables = {}
        
        if layer == 'bronze' or layer is None:
            all_tables.update({f"bronze.{k}": cls.get_table('bronze', k) 
                             for k in cls.BRONZE_TABLES.keys()})
        
        if layer == 'silver' or layer is None:
            all_tables.update({f"silver.fact.{k}": cls.get_table('silver', k) 
                             for k in cls.SILVER_FACTS.keys()})
            all_tables.update({f"silver.dim.{k}": cls.get_table('silver', k) 
                             for k in cls.SILVER_DIMENSIONS.keys()})
        
        if layer == 'gold' or layer is None:
            all_tables.update({f"gold.{k}": cls.get_table('gold', k) 
                             for k in cls.GOLD_MARTS.keys()})
        
        return all_tables
    
    # ========================================
    # MONITORING CONFIGURATION
    # ========================================
    
    # Monitoring Results Tables (will be created in monitoring schema)
    MONITORING_TABLES = {
        'data_quality': 'data_quality_results',
        'performance': 'performance_metrics',
        'cost': 'cost_tracking_results',
        'ml_drift': 'ml_drift_metrics',
        'lakehouse': 'lakehouse_monitoring_results',
        'custom': 'custom_metrics_results',
        'orchestration': 'orchestration_runs'
    }
    
    # Thresholds
    DATA_QUALITY_THRESHOLD = float(os.getenv('DATA_QUALITY_THRESHOLD', '0.95'))  # 95% pass rate
    DRIFT_THRESHOLD = float(os.getenv('DRIFT_THRESHOLD', '0.15'))  # 15% max drift
    MODEL_PERFORMANCE_THRESHOLD = float(os.getenv('MODEL_PERFORMANCE_THRESHOLD', '0.85'))  # 85% accuracy
    COST_ALERT_THRESHOLD_USD = float(os.getenv('COST_ALERT_THRESHOLD_USD', '10000'))  # $10K
    SMALL_FILE_THRESHOLD_MB = int(os.getenv('SMALL_FILE_THRESHOLD_MB', '128'))  # 128 MB
    SMALL_FILE_COUNT_THRESHOLD = int(os.getenv('SMALL_FILE_COUNT_THRESHOLD', '100'))  # 100 files
    QUERY_DURATION_THRESHOLD_SEC = int(os.getenv('QUERY_DURATION_THRESHOLD_SEC', '300'))  # 5 minutes
    
    # Monitoring Intervals
    DATA_QUALITY_CHECK_INTERVAL = os.getenv('DATA_QUALITY_CHECK_INTERVAL', 'daily')
    PERFORMANCE_CHECK_INTERVAL = os.getenv('PERFORMANCE_CHECK_INTERVAL', 'daily')
    COST_CHECK_INTERVAL = os.getenv('COST_CHECK_INTERVAL', 'phase-based')
    ML_DRIFT_CHECK_INTERVAL = os.getenv('ML_DRIFT_CHECK_INTERVAL', 'daily')
    
    # ========================================
    # DATABRICKS WORKSPACE CONFIGURATION
    # ========================================
    
    DATABRICKS_HOST = os.getenv('DATABRICKS_HOST', 'https://your-workspace.cloud.databricks.com')
    DATABRICKS_TOKEN = os.getenv('DATABRICKS_TOKEN')  # From secrets or env
    SQL_WAREHOUSE_ID = os.getenv('SQL_WAREHOUSE_ID')
    CLUSTER_ID = os.getenv('CLUSTER_ID')
    
    # ========================================
    # ALERT CONFIGURATION
    # ========================================
    
    ALERT_EMAIL = os.getenv('ALERT_EMAIL', 'sap-alerts@example.com')
    ALERT_EMAIL_CC = os.getenv('ALERT_EMAIL_CC', '').split(',') if os.getenv('ALERT_EMAIL_CC') else []
    SLACK_WEBHOOK_URL = os.getenv('SLACK_WEBHOOK_URL')
    PAGERDUTY_KEY = os.getenv('PAGERDUTY_KEY')
    
    # Alert Severity Levels
    ALERT_SEVERITY = {
        'CRITICAL': {'email': True, 'slack': True, 'pagerduty': True},
        'HIGH': {'email': True, 'slack': True, 'pagerduty': False},
        'MEDIUM': {'email': True, 'slack': True, 'pagerduty': False},
        'LOW': {'email': False, 'slack': True, 'pagerduty': False}
    }
    
    # ========================================
    # SYSTEM TABLES (Databricks)
    # ========================================
    
    SYSTEM_TABLES = {
        'audit_logs': 'system.access.audit',
        'billing': 'system.billing.usage',
        'query_history': 'system.query.history',
        'query_metrics': 'system.query.task_metrics'
    }
    
    # ========================================
    # VALIDATION & HEALTH CHECKS
    # ========================================
    
    @classmethod
    def validate_config(cls) -> Dict[str, any]:
        """Validate configuration and return status
        
        Returns:
            Dict with validation results
        """
        results = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'info': {}
        }
        
        # Check environment
        results['info']['environment'] = cls.ENVIRONMENT
        results['info']['catalog'] = cls.CATALOG
        results['info']['timestamp'] = datetime.now().isoformat()
        
        # Check required configs (optional for monitoring)
        if not cls.DATABRICKS_HOST:
            results['warnings'].append('DATABRICKS_HOST not configured')
        
        if not cls.ALERT_EMAIL:
            results['warnings'].append('ALERT_EMAIL not configured - alerts will not be sent')
        
        if not cls.SLACK_WEBHOOK_URL:
            results['warnings'].append('SLACK_WEBHOOK_URL not configured - Slack alerts disabled')
        
        # Validate thresholds
        if not 0 <= cls.DATA_QUALITY_THRESHOLD <= 1:
            results['errors'].append(f'DATA_QUALITY_THRESHOLD must be between 0 and 1, got {cls.DATA_QUALITY_THRESHOLD}')
            results['valid'] = False
        
        if not 0 <= cls.DRIFT_THRESHOLD <= 1:
            results['errors'].append(f'DRIFT_THRESHOLD must be between 0 and 1, got {cls.DRIFT_THRESHOLD}')
            results['valid'] = False
        
        # Info about table counts
        results['info']['bronze_tables'] = len(cls.BRONZE_TABLES)
        results['info']['silver_tables'] = len(cls.SILVER_FACTS) + len(cls.SILVER_DIMENSIONS)
        results['info']['gold_tables'] = len(cls.GOLD_MARTS)
        results['info']['monitoring_tables'] = len(cls.MONITORING_TABLES)
        
        return results
    
    @classmethod
    def print_config(cls, verbose: bool = False):
        """Print current configuration (for debugging)"""
        print("=" * 80)
        print("SkillAIPath Monitoring Configuration")
        print("=" * 80)
        print(f"Environment: {cls.ENVIRONMENT.upper()}")
        print(f"Catalog: {cls.CATALOG}")
        print(f"Schemas: {cls.SCHEMA_BRONZE}, {cls.SCHEMA_SILVER}, {cls.SCHEMA_GOLD}, {cls.SCHEMA_MONITORING}")
        print(f"")
        print(f"Tables:")
        print(f"  Bronze: {len(cls.BRONZE_TABLES)} tables")
        print(f"  Silver: {len(cls.SILVER_FACTS)} facts + {len(cls.SILVER_DIMENSIONS)} dimensions")
        print(f"  Gold: {len(cls.GOLD_MARTS)} marts")
        print(f"")
        print(f"Thresholds:")
        print(f"  Data Quality: {cls.DATA_QUALITY_THRESHOLD * 100}%")
        print(f"  ML Drift: {cls.DRIFT_THRESHOLD * 100}%")
        print(f"  Model Performance: {cls.MODEL_PERFORMANCE_THRESHOLD * 100}%")
        print(f"  Cost Alert: ${cls.COST_ALERT_THRESHOLD_USD:,}")
        print(f"")
        print(f"Alerts:")
        print(f"  Email: {cls.ALERT_EMAIL or 'Not configured'}")
        print(f"  Slack: {'Configured' if cls.SLACK_WEBHOOK_URL else 'Not configured'}")
        print(f"  PagerDuty: {'Configured' if cls.PAGERDUTY_KEY else 'Not configured'}")
        print("=" * 80)
        
        if verbose:
            print(f"\nAll Bronze Tables:")
            for key, table in cls.BRONZE_TABLES.items():
                print(f"  {key}: {cls.get_table('bronze', key)}")
            
            print(f"\nAll Silver Fact Tables:")
            for key, table in cls.SILVER_FACTS.items():
                print(f"  {key}: {cls.get_table('silver', key)}")
            
            print(f"\nAll Gold Mart Tables:")
            for key, table in cls.GOLD_MARTS.items():
                print(f"  {key}: {cls.get_table('gold', key)}")
            print("=" * 80)


# Singleton instance for convenience
config = SkillAIPathConfig()


# ========================================
# QUICK REFERENCE GUIDE
# ========================================
"""
USAGE EXAMPLES:

1. Get a table path:
   from config import config
   sales_table = config.get_table('bronze', 'sales')
   # Returns: 'sap_dev.bronze.stg_sales'

2. Get a schema:
   silver_schema = config.get_schema('silver')
   # Returns: 'sap_dev.silver'

3. Get all tables:
   all_tables = config.get_all_tables()
   bronze_tables = config.get_all_tables('bronze')

4. Access thresholds:
   if quality_score < config.DATA_QUALITY_THRESHOLD:
       send_alert()

5. Validate configuration:
   validation = config.validate_config()
   if not validation['valid']:
       print(validation['errors'])

6. Print configuration:
   config.print_config(verbose=True)

7. Get monitoring table:
   results_table = config.get_table('monitoring', 'data_quality')
   # Returns: 'sap_dev.monitoring.data_quality_results'

8. Check environment:
   if config.ENVIRONMENT == 'prod':
       enable_strict_mode()
"""
