"""Supply Chain Intelligence Agent - Configuration Module

Central configuration for all agent components.

Author: SkillAIPath Team
Last Updated: August 2026
"""

from dataclasses import dataclass
import math

# ============================================================================
# DATABASE CONFIGURATION
# ============================================================================

CATALOG = "sap_dev"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

SILVER_PATH = f"{CATALOG}.{SILVER_SCHEMA}"
GOLD_PATH = f"{CATALOG}.{GOLD_SCHEMA}"

# ============================================================================
# TABLE REFERENCES
# ============================================================================

@dataclass
class SilverTables:
    """Input tables from silver layer"""
    FACT_SALES = f"{SILVER_PATH}.fact_sales"
    FACT_INVENTORY = f"{SILVER_PATH}.fact_inventory"
    FACT_PURCHASE = f"{SILVER_PATH}.fact_purchase"
    DIM_PRODUCT = f"{SILVER_PATH}.dim_product"
    DIM_WAREHOUSE = f"{SILVER_PATH}.dim_warehouse"
    DIM_DATE = f"{SILVER_PATH}.dim_date"
    DIM_SUPPLIER = f"{SILVER_PATH}.dim_supplier"

@dataclass
class GoldTables:
    """Output tables in gold layer"""
    PREDICTIONS = f"{GOLD_PATH}.supply_chain_predictions"
    ALERTS = f"{GOLD_PATH}.supply_chain_alerts"
    METRICS = f"{GOLD_PATH}.supply_chain_metrics"
    AUDIT_LOG = f"{GOLD_PATH}.supply_chain_audit_log"

# ============================================================================
# BUSINESS CONSTANTS
# ============================================================================

TOTAL_PRODUCTS = 78
TOTAL_WAREHOUSES = 20
TOTAL_COMBINATIONS = 1560
HISTORY_DAYS = 30
SALES_VELOCITY_WINDOWS = [7, 14, 30]

# ============================================================================
# ALERT THRESHOLDS
# ============================================================================

@dataclass
class AlertThresholds:
    """Alert generation thresholds"""
    # Stockout risk (days until stockout)
    CRITICAL_STOCKOUT_DAYS = 1
    HIGH_STOCKOUT_DAYS = 3
    MEDIUM_STOCKOUT_DAYS = 7
    LOW_STOCKOUT_DAYS = 14
    
    # Excess inventory (days of inventory)
    EXCESS_INVENTORY_DAYS = 60
    DEAD_STOCK_DAYS = 90
    NO_SALES_PERIOD_DAYS = 14
    
    # Healthy range
    HEALTHY_DOI_MIN = 7
    HEALTHY_DOI_MAX = 45

# ============================================================================
# SAFETY STOCK CONFIGURATION
# ============================================================================

@dataclass
class SafetyStockConfig:
    """Safety stock calculation parameters"""
    SERVICE_LEVEL = 0.90          # 90% service level
    Z_SCORE = 1.28                # Z-score for 90% confidence
    DEFAULT_LEAD_TIME_DAYS = 4    # Default supplier lead time
    MIN_LEAD_TIME_DAYS = 2
    MAX_LEAD_TIME_DAYS = 15

# ============================================================================
# REORDER CONFIGURATION
# ============================================================================

@dataclass
class ReorderConfig:
    """Reorder point and quantity parameters"""
    MIN_ORDER_QUANTITY = 50
    MAX_ORDER_QUANTITY = 1000
    ORDERING_COST = 500
    HOLDING_COST_PCT = 0.25
    REORDER_SUPPLY_DAYS = 15      # Days of supply per order
    ROUND_TO_NEAREST = 10
    BUFFER_MULTIPLIER = 1.2

# ============================================================================
# TRANSFER CONFIGURATION
# ============================================================================

@dataclass
class TransferConfig:
    """Warehouse transfer parameters"""
    TRANSFER_COST_PER_UNIT = 50
    FIXED_TRANSFER_COST = 200
    MIN_TRANSFER_QUANTITY = 5
    MAX_TRANSFER_QUANTITY = 200
    SOURCE_MIN_DOI = 45
    TARGET_MAX_DAYS_TO_STOCKOUT = 7
    MIN_NET_BENEFIT = 500

# ============================================================================
# STOCK STATUS LABELS
# ============================================================================

class StockStatus:
    OUT_OF_STOCK = "Out of Stock"
    CRITICAL = "Critical"
    WARNING = "Warning"
    HEALTHY = "Healthy"
    EXCESS = "Excess"
    DEAD_STOCK = "Dead Stock"

class AlertType:
    STOCKOUT = "Stockout"
    EXCESS_INVENTORY = "Excess_Inventory"
    TRANSFER_OPPORTUNITY = "Transfer_Opportunity"
    REORDER_NEEDED = "Reorder_Needed"

class Priority:
    CRITICAL = "Critical"
    HIGH = "High"
    MEDIUM = "Medium"
    LOW = "Low"

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def calculate_safety_stock(avg_daily_sales: float, sales_volatility: float,
                          lead_time_days: int = None) -> int:
    """Calculate safety stock: Z × σ × √(LT)"""
    if lead_time_days is None:
        lead_time_days = SafetyStockConfig.DEFAULT_LEAD_TIME_DAYS
    
    safety_stock = (
        SafetyStockConfig.Z_SCORE * 
        sales_volatility * 
        math.sqrt(lead_time_days)
    )
    return max(0, int(round(safety_stock)))

def calculate_reorder_point(avg_daily_sales: float, safety_stock: int,
                           lead_time_days: int = None) -> int:
    """Calculate reorder point: (Avg Sales × LT) + Safety Stock"""
    if lead_time_days is None:
        lead_time_days = SafetyStockConfig.DEFAULT_LEAD_TIME_DAYS
    
    reorder_point = (avg_daily_sales * lead_time_days) + safety_stock
    return max(0, int(round(reorder_point)))

def calculate_order_quantity(avg_daily_sales: float, supply_days: int = None) -> int:
    """Calculate order quantity with buffer and rounding"""
    if supply_days is None:
        supply_days = ReorderConfig.REORDER_SUPPLY_DAYS
    
    order_qty = avg_daily_sales * supply_days * ReorderConfig.BUFFER_MULTIPLIER
    order_qty = round(order_qty / ReorderConfig.ROUND_TO_NEAREST) * ReorderConfig.ROUND_TO_NEAREST
    order_qty = max(ReorderConfig.MIN_ORDER_QUANTITY, min(order_qty, ReorderConfig.MAX_ORDER_QUANTITY))
    return int(order_qty)

def get_stock_status(days_of_inventory: float, current_stock: int, 
                     days_until_stockout: float) -> str:
    """Classify stock status based on metrics"""
    if current_stock <= 0:
        return StockStatus.OUT_OF_STOCK
    elif days_until_stockout <= AlertThresholds.CRITICAL_STOCKOUT_DAYS:
        return StockStatus.CRITICAL
    elif days_until_stockout <= AlertThresholds.MEDIUM_STOCKOUT_DAYS:
        return StockStatus.WARNING
    elif days_of_inventory > AlertThresholds.DEAD_STOCK_DAYS:
        return StockStatus.DEAD_STOCK
    elif days_of_inventory > AlertThresholds.EXCESS_INVENTORY_DAYS:
        return StockStatus.EXCESS
    else:
        return StockStatus.HEALTHY

def get_priority_level(days_until_stockout: float) -> str:
    """Determine alert priority"""
    if days_until_stockout <= AlertThresholds.CRITICAL_STOCKOUT_DAYS:
        return Priority.CRITICAL
    elif days_until_stockout <= AlertThresholds.HIGH_STOCKOUT_DAYS:
        return Priority.HIGH
    elif days_until_stockout <= AlertThresholds.MEDIUM_STOCKOUT_DAYS:
        return Priority.MEDIUM
    else:
        return Priority.LOW

if __name__ == "__main__":
    print("🔧 Supply Chain Intelligence Agent - Configuration")
    print("=" * 60)
    print(f"Catalog: {CATALOG}")
    print(f"Total Combinations: {TOTAL_COMBINATIONS}")
    print(f"Service Level: {SafetyStockConfig.SERVICE_LEVEL * 100}%")
    print(f"Critical Stockout: < {AlertThresholds.CRITICAL_STOCKOUT_DAYS} day")
    print(f"Reorder Supply Days: {ReorderConfig.REORDER_SUPPLY_DAYS}")
    print(f"Transfer Cost/Unit: ₹{TransferConfig.TRANSFER_COST_PER_UNIT}")
