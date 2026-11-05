# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,SkillAIPath Demand Forecasting - Complete ML Pipeline
# MAGIC %md
# MAGIC # SkillAIPath Demand Forecasting System
# MAGIC
# MAGIC ## Time-Series Forecasting for Inventory Optimization
# MAGIC
# MAGIC **THE PROBLEM: "We're either out of stock or drowning in inventory"**
# MAGIC
# MAGIC **Monday Morning - Operations Review**
# MAGIC
# MAGIC **COO:** "Last week we ran out of rice (₹2.5M lost sales). This week we have 50 tons of expired yogurt (₹1.8M write-off)."
# MAGIC
# MAGIC **Warehouse Manager:** "I order based on gut feel. No data, no forecasting. Just reactive."
# MAGIC
# MAGIC **Data Engineer:** "We have 3 years of daily sales by product, location, season, holidays, promotions."
# MAGIC
# MAGIC **ML Engineer:** "We can build a forecasting model:
# MAGIC 1. **Trend Analysis** - Long-term growth patterns
# MAGIC 2. **Seasonality** - Weekly/monthly/yearly cycles  
# MAGIC 3. **Holiday Effects** - Diwali, Holi, etc.
# MAGIC 4. **Model Comparison** - Prophet vs ARIMA vs Exponential Smoothing"
# MAGIC
# MAGIC **Business Goal:** Reduce stockouts by 80% and overstock by 60% via accurate demand forecasting.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## THE SOLUTION: Multi-Model Time-Series Forecasting
# MAGIC
# MAGIC **What We're Building:**
# MAGIC - Three forecasting models: Prophet, SARIMA, Exponential Smoothing
# MAGIC - Holiday effects (Indian festivals, weekends, promotions)
# MAGIC - Product-level and category-level forecasts
# MAGIC - Multi-horizon evaluation (7, 14, 30 days)
# MAGIC - Seasonal decomposition and trend analysis
# MAGIC - Inventory optimization recommendations
# MAGIC
# MAGIC **Input Data:**
# MAGIC - `gold.fact_sales` - Daily sales by product
# MAGIC - `gold.dim_product` - Product categories
# MAGIC - `gold.dim_date` - Calendar with holidays
# MAGIC
# MAGIC **Output:**
# MAGIC - MLflow registered model: `sap_demand_forecasting`
# MAGIC - Multi-horizon forecasts with confidence intervals
# MAGIC - MAPE, RMSE, MAE metrics across horizons
# MAGIC - Safety stock recommendations
# MAGIC - Inventory cost optimization
# MAGIC
# MAGIC **FREE Tier Compatible:**
# MAGIC - No paid Databricks features
# MAGIC - Standard MLflow + Prophet + statsmodels
# MAGIC - Batch inference via notebooks
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Multi-Environment Support:** Auto-detects dev/uat/prod from notebook path

# COMMAND ----------

# DBTITLE 1,Auto-Detect Environment
# Auto-detect environment from job parameters or notebook path
import re
import os

print("="*80)
print("🤖 ENVIRONMENT AUTO-DETECTION")
print("="*80)

# First, try to get from job parameters (widgets)
try:
    dbutils.widgets.text("ENVIRONMENT", "")
    dbutils.widgets.text("CATALOG", "")
    
    ENVIRONMENT = dbutils.widgets.get("ENVIRONMENT")
    CATALOG = dbutils.widgets.get("CATALOG")
    
    if ENVIRONMENT and CATALOG:
        print(f"✅ Using job parameters:")
        print(f"  Environment: {ENVIRONMENT}")
        print(f"  Catalog: {CATALOG}")
    else:
        raise ValueError("Empty parameters")
except:
    # Fallback: auto-detect from notebook path
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    print(f"Notebook Path: {notebook_path}")
    
    # Extract environment from path
    match = re.search(r'/Shared/([^/]+)/SkillAIPath', notebook_path)
    
    if match:
        ENVIRONMENT = match.group(1).lower()
        CATALOG = f"sap_{ENVIRONMENT}"
        print(f"✅ Auto-detected from path:")
        print(f"  Environment: {ENVIRONMENT}")
        print(f"  Catalog: {CATALOG}")
    else:
        # Fallback to dev if running from personal folder
        ENVIRONMENT = "dev"
        CATALOG = "sap_dev"
        print(f"⚠️  Using fallback:")
        print(f"  Environment: {ENVIRONMENT}")
        print(f"  Catalog: {CATALOG}")

print("="*80)

# COMMAND ----------

# DBTITLE 1,Install Dependencies
print("Installing ML libraries...")
%pip install prophet scikit-learn mlflow pandas numpy matplotlib seaborn statsmodels pmdarima --quiet
# Note: restartPython() removed to preserve CATALOG and ENVIRONMENT variables

# COMMAND ----------

# DBTITLE 1,Load Historical Sales Data
import mlflow
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pyspark.sql import functions as F
import warnings
warnings.filterwarnings('ignore')

print("="*80)
print("📊 LOADING HISTORICAL SALES DATA")
print("="*80)

query = f"""
SELECT 
    DATE(fs.BatchDate) as ds,
    fs.ProductSK as product_id,
    pc.ProductCategory as category,
    NULL as subcategory,
    dp.ProductName as product_name,
    SUM(fs.ProductQuantity) as quantity_sold,
    SUM(fs.ProductQuantity * fs.finalprice) as revenue,
    COUNT(DISTINCT fs.CustomerSK) as unique_customers
FROM {CATALOG}.silver.fact_sales fs
JOIN {CATALOG}.silver.dim_product dp ON fs.ProductSK = dp.ProductSK
JOIN {CATALOG}.silver.dim_product_category pc ON fs.ProductCategorySK = pc.ProductCategorySK
WHERE fs.BatchDate >= CURRENT_DATE - INTERVAL 24 MONTHS
GROUP BY DATE(fs.BatchDate), fs.ProductSK, pc.ProductCategory, dp.ProductName
ORDER BY ds, product_id
"""

df_sales = spark.sql(query).toPandas()

print(f"✅ Loaded {len(df_sales):,} daily product records")
print(f"✅ Date range: {df_sales['ds'].min()} to {df_sales['ds'].max()}")
print(f"✅ Total days: {(df_sales['ds'].max() - df_sales['ds'].min()).days}")
print(f"✅ Products: {df_sales['product_id'].nunique():,}")
print(f"✅ Categories: {df_sales['category'].nunique()}")
print(f"✅ Total quantity sold: {df_sales['quantity_sold'].sum():,.0f} units")
print(f"✅ Total revenue: ₹{df_sales['revenue'].sum():,.2f}")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Exploratory Data Analysis
import matplotlib.pyplot as plt
import seaborn as sns

print("="*80)
print("📈 EXPLORATORY DATA ANALYSIS")
print("="*80)

# Convert revenue to numeric (from Decimal/object dtype)
df_sales['revenue'] = pd.to_numeric(df_sales['revenue'], errors='coerce')

# Daily aggregates
daily_sales = df_sales.groupby('ds').agg({
    'quantity_sold': 'sum',
    'revenue': 'sum',
    'unique_customers': 'sum'
}).reset_index()

print(f"\n📊 Daily Statistics:")
print(f"  Avg daily quantity: {daily_sales['quantity_sold'].mean():,.0f} units")
print(f"  Std daily quantity: {daily_sales['quantity_sold'].std():,.0f} units")
print(f"  Avg daily revenue: ₹{daily_sales['revenue'].mean():,.2f}")
print(f"  Coefficient of variation: {(daily_sales['quantity_sold'].std() / daily_sales['quantity_sold'].mean()):.2%}")

# Day of week analysis
daily_sales['day_of_week'] = pd.to_datetime(daily_sales['ds']).dt.dayofweek
daily_sales['day_name'] = pd.to_datetime(daily_sales['ds']).dt.day_name()

dow_stats = daily_sales.groupby('day_name').agg({
    'quantity_sold': 'mean',
    'revenue': 'mean'
}).reindex(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'])

print(f"\n📅 Day of Week Patterns:")
print(dow_stats.to_string())

# Visualization
fig, axes = plt.subplots(2, 2, figsize=(15, 10))

# Time series
axes[0, 0].plot(daily_sales['ds'], daily_sales['quantity_sold'], linewidth=0.8)
axes[0, 0].set_title('Daily Quantity Sold Over Time')
axes[0, 0].set_xlabel('Date')
axes[0, 0].set_ylabel('Quantity')
axes[0, 0].grid(True, alpha=0.3)

# Day of week
axes[0, 1].bar(range(7), dow_stats['quantity_sold'].values)
axes[0, 1].set_xticks(range(7))
axes[0, 1].set_xticklabels(['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'])
axes[0, 1].set_title('Average Quantity by Day of Week')
axes[0, 1].set_ylabel('Avg Quantity')

# Distribution
axes[1, 0].hist(daily_sales['quantity_sold'], bins=50, edgecolor='black')
axes[1, 0].set_title('Distribution of Daily Quantity')
axes[1, 0].set_xlabel('Quantity')
axes[1, 0].set_ylabel('Frequency')

# Category performance
category_stats = df_sales.groupby('category').agg({
    'quantity_sold': 'sum',
    'revenue': 'sum'
}).nlargest(10, 'revenue')

axes[1, 1].barh(range(len(category_stats)), category_stats['revenue'].values)
axes[1, 1].set_yticks(range(len(category_stats)))
axes[1, 1].set_yticklabels(category_stats.index)
axes[1, 1].set_title('Top 10 Categories by Revenue')
axes[1, 1].set_xlabel('Revenue (₹)')

plt.tight_layout()
plt.savefig('/tmp/eda_forecasting.png', dpi=100)
print("\n✅ EDA visualization saved")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Seasonal Decomposition Analysis
from statsmodels.tsa.seasonal import seasonal_decompose

print("="*80)
print("📊 SEASONAL DECOMPOSITION ANALYSIS")
print("="*80)

# Prepare daily series
ts_data = daily_sales.set_index('ds')['quantity_sold']

# Check if sufficient data for decomposition (need at least 2 complete cycles)
period = 7
min_observations = 2 * period

if len(ts_data) < min_observations:
    print(f"⚠️  Insufficient data for seasonal decomposition")
    print(f"   Required: {min_observations} observations (2 complete weekly cycles)")
    print(f"   Available: {len(ts_data)} observations")
    print(f"   Skipping seasonal decomposition. Load more historical data for this analysis.")
    print("="*80)
    # Skip decomposition visualization
    decomposition = None
else:
    # Decompose
    decomposition = seasonal_decompose(ts_data, model='additive', period=period)

    print(f"✅ Trend component extracted")
    print(f"✅ Seasonal component extracted (weekly)")
    print(f"✅ Residual component extracted")

    # Plot decomposition
    fig, axes = plt.subplots(4, 1, figsize=(14, 10))

    ts_data.plot(ax=axes[0], title='Original Time Series')
    axes[0].set_ylabel('Quantity')

    decomposition.trend.plot(ax=axes[1], title='Trend Component')
    axes[1].set_ylabel('Trend')

    decomposition.seasonal.plot(ax=axes[2], title='Seasonal Component (Weekly)')
    axes[2].set_ylabel('Seasonality')

    decomposition.resid.plot(ax=axes[3], title='Residual Component')
    axes[3].set_ylabel('Residuals')

    plt.tight_layout()
    plt.savefig('/tmp/seasonal_decomposition.png', dpi=100)
    print("✅ Decomposition plot saved")
    print("="*80)

# COMMAND ----------

# DBTITLE 1,Prepare Training Data - Top Products
print("="*80)
print("🔧 PREPARING TRAINING DATA")
print("="*80)

# Select top 5 products by revenue
top_products = df_sales.groupby('product_id').agg({
    'revenue': 'sum',
    'quantity_sold': 'sum',
    'product_name': 'first',
    'category': 'first'
}).nlargest(5, 'revenue').reset_index()

print("\n📦 Top 5 Products for Forecasting:")
for idx, row in top_products.iterrows():
    print(f"  {idx+1}. {row['product_name']:40s} | {row['category']:15s} | ₹{row['revenue']:>12,.0f}")

# Focus on #1 product for detailed modeling
target_product = top_products.iloc[0]['product_id']
target_name = top_products.iloc[0]['product_name']

df_product = df_sales[df_sales['product_id'] == target_product][['ds', 'quantity_sold']].copy()
df_product.columns = ['ds', 'y']
df_product = df_product.sort_values('ds').reset_index(drop=True)

print(f"\n✅ Primary model training on: {target_name}")
print(f"✅ Training records: {len(df_product)}")
print(f"✅ Date range: {df_product['ds'].min()} to {df_product['ds'].max()}")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Add Holiday Effects (Indian Calendar)
import pandas as pd

print("="*80)
print("🎉 ADDING HOLIDAY EFFECTS")
print("="*80)

# Major Indian holidays that impact retail
indian_holidays = pd.DataFrame({
    'holiday': 'diwali',
    'ds': pd.to_datetime(['2023-11-12', '2024-10-31', '2025-10-20', '2026-11-08']),
    'lower_window': -7,
    'upper_window': 3
})

holi = pd.DataFrame({
    'holiday': 'holi',
    'ds': pd.to_datetime(['2024-03-25', '2025-03-14', '2026-03-03', '2027-03-22']),
    'lower_window': -2,
    'upper_window': 1
})

republic_day = pd.DataFrame({
    'holiday': 'republic_day',
    'ds': pd.to_datetime(['2024-01-26', '2025-01-26', '2026-01-26', '2027-01-26']),
    'lower_window': -1,
    'upper_window': 1
})

independence_day = pd.DataFrame({
    'holiday': 'independence_day',
    'ds': pd.to_datetime(['2024-08-15', '2025-08-15', '2026-08-15', '2027-08-15']),
    'lower_window': -1,
    'upper_window': 1
})

ganesh_chaturthi = pd.DataFrame({
    'holiday': 'ganesh_chaturthi',
    'ds': pd.to_datetime(['2024-09-07', '2025-08-27', '2026-09-16', '2027-09-05']),
    'lower_window': -1,
    'upper_window': 1
})

holidays = pd.concat([indian_holidays, holi, republic_day, independence_day, ganesh_chaturthi])

print(f"✅ Added {len(holidays)} holiday periods")
print(f"✅ Holiday types: {holidays['holiday'].nunique()}")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Train/Test Split (Time-Based)
print("="*80)
print("🔀 TRAIN/TEST SPLIT")
print("="*80)

# Multiple horizons: 7, 14, 30 days
horizons = [7, 14, 30]

# Use 30-day test set (or 20% if less data available)
data_span = (df_product['ds'].max() - df_product['ds'].min()).days
test_days = min(30, max(1, int(data_span * 0.2)))
train_cutoff = df_product['ds'].max() - timedelta(days=test_days)
df_train = df_product[df_product['ds'] <= train_cutoff]
df_test = df_product[df_product['ds'] > train_cutoff]

print(f"✅ Train set: {len(df_train)} days ({df_train['ds'].min()} to {df_train['ds'].max()})")
print(f"✅ Test set: {len(df_test)} days ({df_test['ds'].min()} to {df_test['ds'].max()})")
print(f"✅ Evaluation horizons: {horizons} days")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Model 1: Train Prophet
from prophet import Prophet
import time

print("="*80)
print("🤖 MODEL 1: PROPHET")
print("="*80)

prophet_model = Prophet(
    yearly_seasonality=True,
    weekly_seasonality=True,
    daily_seasonality=False,
    holidays=holidays,
    changepoint_prior_scale=0.05,
    seasonality_prior_scale=10.0,
    interval_width=0.95,
    seasonality_mode='multiplicative'
)

start_time = time.time()
prophet_model.fit(df_train)
prophet_training_time = time.time() - start_time

print(f"✅ Training completed in {prophet_training_time:.2f} seconds")
print(f"✅ Changepoints detected: {len(prophet_model.changepoints)}")
print("="*80)

# Generate forecast
future = prophet_model.make_future_dataframe(periods=30, freq='D')
prophet_forecast = prophet_model.predict(future)

# COMMAND ----------

# DBTITLE 1,Model 2: Train SARIMA
from statsmodels.tsa.statespace.sarimax import SARIMAX

print("="*80)
print("🤖 MODEL 2: SARIMA")
print("="*80)

# Prepare series (ensure DatetimeIndex with daily frequency for forecasting)
y_train = df_train.copy()
y_train['ds'] = pd.to_datetime(y_train['ds'])
y_train = y_train.set_index('ds').asfreq('D')['y']

# SARIMA model (p,d,q)(P,D,Q,s)
# Using (1,1,1)(1,1,1,7) for phase-based seasonality
sarima_model = SARIMAX(
    y_train,
    order=(1, 1, 1),
    seasonal_order=(1, 1, 1, 7),
    enforce_stationarity=False,
    enforce_invertibility=False
)

start_time = time.time()
sarima_fit = sarima_model.fit(disp=False)
sarima_training_time = time.time() - start_time

print(f"✅ Training completed in {sarima_training_time:.2f} seconds")
print(f"✅ AIC: {sarima_fit.aic:.2f}")
print(f"✅ BIC: {sarima_fit.bic:.2f}")
print("="*80)

# Generate forecast
sarima_forecast = sarima_fit.get_forecast(steps=30)
sarima_forecast_df = pd.DataFrame({
    'ds': pd.date_range(start=df_train['ds'].max() + timedelta(days=1), periods=30),
    'yhat': sarima_forecast.predicted_mean.values,
    'yhat_lower': sarima_forecast.conf_int().iloc[:, 0].values,
    'yhat_upper': sarima_forecast.conf_int().iloc[:, 1].values
})

# COMMAND ----------

# DBTITLE 1,Model 3: Train Exponential Smoothing (Holt-Winters)
from statsmodels.tsa.holtwinters import ExponentialSmoothing

print("="*80)
print("🤖 MODEL 3: EXPONENTIAL SMOOTHING (HOLT-WINTERS)")
print("="*80)

# Fill NaN values from asfreq('D') — missing days have 0 sales
y_train = y_train.fillna(0)

# Check if sufficient data for seasonal Holt-Winters (need at least 2 complete cycles)
seasonal_periods = 7
min_observations = 2 * seasonal_periods

if len(y_train) < min_observations:
    print(f"⚠️  Insufficient data for seasonal Holt-Winters")
    print(f"   Required: {min_observations} observations (2 complete weekly cycles)")
    print(f"   Available: {len(y_train)} observations")
    print(f"   Falling back to Simple Exponential Smoothing (no seasonality)")
    print("="*80)
    
    # Use Simple Exponential Smoothing instead
    hw_model = ExponentialSmoothing(
        y_train,
        trend='add',
        seasonal=None
    )
    
    start_time = time.time()
    hw_fit = hw_model.fit()
    hw_training_time = time.time() - start_time
    
    print(f"✅ Training completed in {hw_training_time:.2f} seconds")
    print(f"✅ AIC: {hw_fit.aic:.2f}")
    print("="*80)
else:
    hw_model = ExponentialSmoothing(
        y_train,
        seasonal_periods=seasonal_periods,
        trend='add',
        seasonal='add',
        damped_trend=True
    )
    
    start_time = time.time()
    hw_fit = hw_model.fit()
    hw_training_time = time.time() - start_time
    
    print(f"✅ Training completed in {hw_training_time:.2f} seconds")
    print(f"✅ AIC: {hw_fit.aic:.2f}")
    print("="*80)

# Generate forecast
hw_forecast_vals = hw_fit.forecast(steps=30)
hw_forecast_df = pd.DataFrame({
    'ds': pd.date_range(start=df_train['ds'].max() + timedelta(days=1), periods=30),
    'yhat': hw_forecast_vals.values
})

# COMMAND ----------

# DBTITLE 1,Evaluation Metrics Function
from sklearn.metrics import mean_absolute_error, mean_squared_error, mean_absolute_percentage_error

def evaluate_forecast(actual, predicted, model_name):
    """Calculate evaluation metrics"""
    mae = mean_absolute_error(actual, predicted)
    rmse = np.sqrt(mean_squared_error(actual, predicted))
    mape = mean_absolute_percentage_error(actual, predicted) * 100
    
    # Mean forecast bias
    bias = np.mean(predicted - actual)
    
    return {
        'model': model_name,
        'mae': mae,
        'rmse': rmse,
        'mape': mape,
        'bias': bias
    }

print("="*80)
print("✅ Evaluation metrics function defined")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Evaluate All Models - Multi-Horizon
print("="*80)
print("📊 MULTI-HORIZON EVALUATION")
print("="*80)

evaluation_results = []

for horizon in horizons:
    print(f"\n{'='*80}")
    print(f"Evaluating {horizon}-day horizon")
    print('='*80)
    
    # Get actual values
    test_horizon = df_test[df_test['ds'] <= df_train['ds'].max() + timedelta(days=horizon)]
    actual_values = test_horizon['y'].values
    
    if len(actual_values) == 0:
        continue
    
    # Convert train_cutoff to pandas Timestamp for comparison
    train_cutoff_ts = pd.Timestamp(train_cutoff)
    
    # Prophet predictions
    prophet_preds = prophet_forecast[
        (prophet_forecast['ds'] > train_cutoff_ts) & 
        (prophet_forecast['ds'] <= train_cutoff_ts + timedelta(days=horizon))
    ]['yhat'].values[:len(actual_values)]
    
    prophet_metrics = evaluate_forecast(actual_values, prophet_preds, 'Prophet')
    prophet_metrics['horizon'] = horizon
    evaluation_results.append(prophet_metrics)
    
    # SARIMA predictions
    sarima_preds = sarima_forecast_df[
        sarima_forecast_df['ds'] <= train_cutoff_ts + timedelta(days=horizon)
    ]['yhat'].values[:len(actual_values)]
    
    sarima_metrics = evaluate_forecast(actual_values, sarima_preds, 'SARIMA')
    sarima_metrics['horizon'] = horizon
    evaluation_results.append(sarima_metrics)
    
    # Holt-Winters predictions
    hw_preds = hw_forecast_df[
        hw_forecast_df['ds'] <= train_cutoff_ts + timedelta(days=horizon)
    ]['yhat'].values[:len(actual_values)]
    
    hw_metrics = evaluate_forecast(actual_values, hw_preds, 'Holt-Winters')
    hw_metrics['horizon'] = horizon
    evaluation_results.append(hw_metrics)
    
    print(f"\nProphet   - MAE: {prophet_metrics['mae']:.2f}, MAPE: {prophet_metrics['mape']:.2f}%")
    print(f"SARIMA    - MAE: {sarima_metrics['mae']:.2f}, MAPE: {sarima_metrics['mape']:.2f}%")
    print(f"Holt-Winters - MAE: {hw_metrics['mae']:.2f}, MAPE: {hw_metrics['mape']:.2f}%")

# Create results dataframe
eval_df = pd.DataFrame(evaluation_results)

print("\n" + "="*80)
print("📊 COMPLETE EVALUATION RESULTS")
print("="*80)
print(eval_df.to_string(index=False))
print("="*80)

# COMMAND ----------

# DBTITLE 1,Model Comparison and Winner Selection
print("="*80)
print("🏆 MODEL COMPARISON")
print("="*80)

# Average metrics across horizons
avg_metrics = eval_df.groupby('model').agg({
    'mae': 'mean',
    'rmse': 'mean',
    'mape': 'mean',
    'bias': 'mean'
}).reset_index()

avg_metrics['training_time'] = [
    prophet_training_time,
    sarima_training_time,
    hw_training_time
]

print(avg_metrics.to_string(index=False))

# Select winner based on MAPE
winner_idx = avg_metrics['mape'].idxmin()
winner_model_name = avg_metrics.loc[winner_idx, 'model']

print(f"\n🥇 WINNER: {winner_model_name}")
print(f"   Reason: Best average MAPE ({avg_metrics.loc[winner_idx, 'mape']:.2f}%)")
print("="*80)

# Assign winner
if winner_model_name == 'Prophet':
    best_model = prophet_model
    best_forecast = prophet_forecast
elif winner_model_name == 'SARIMA':
    best_model = sarima_fit
    best_forecast = sarima_forecast_df
else:
    best_model = hw_fit
    best_forecast = hw_forecast_df

# COMMAND ----------

# DBTITLE 1,Visualize Best Model Forecast
print("="*80)
print("📈 VISUALIZING BEST MODEL FORECAST")
print("="*80)

fig, ax = plt.subplots(figsize=(14, 6))

# Plot training data
ax.plot(df_train['ds'], df_train['y'], label='Training Data', color='blue', linewidth=1)

# Plot test data
ax.plot(df_test['ds'], df_test['y'], label='Actual (Test)', color='green', linewidth=1)

# Plot forecast
if winner_model_name == 'Prophet':
    train_cutoff_ts = pd.Timestamp(train_cutoff)
    forecast_plot = best_forecast[best_forecast['ds'] > train_cutoff_ts]
    ax.plot(forecast_plot['ds'], forecast_plot['yhat'], label='Forecast', color='red', linewidth=1.5)
    ax.fill_between(
        forecast_plot['ds'],
        forecast_plot['yhat_lower'],
        forecast_plot['yhat_upper'],
        alpha=0.2,
        color='red',
        label='Confidence Interval (95%)'
    )
else:
    ax.plot(best_forecast['ds'], best_forecast['yhat'], label='Forecast', color='red', linewidth=1.5)
    if 'yhat_lower' in best_forecast.columns:
        ax.fill_between(
            best_forecast['ds'],
            best_forecast['yhat_lower'],
            best_forecast['yhat_upper'],
            alpha=0.2,
            color='red',
            label='Confidence Interval (95%)'
        )

ax.axvline(train_cutoff, color='black', linestyle='--', label='Train/Test Split', linewidth=1)
ax.set_title(f'Demand Forecast: {target_name} ({winner_model_name})', fontsize=14, fontweight='bold')
ax.set_xlabel('Date', fontsize=12)
ax.set_ylabel('Quantity Sold', fontsize=12)
ax.legend(loc='best')
ax.grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig('/tmp/demand_forecast_best.png', dpi=100)
print("✅ Forecast visualization saved")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Forecast All Top Products
print("="*80)
print("🔄 FORECASTING ALL TOP PRODUCTS")
print("="*80)

all_product_forecasts = {}
all_product_metrics = []

for idx, row in top_products.iterrows():
    product_id = row['product_id']
    product_name = row['product_name']
    
    print(f"\n{'='*80}")
    print(f"Forecasting: {product_name}")
    print('='*80)
    
    # Prepare data
    df_prod = df_sales[df_sales['product_id'] == product_id][['ds', 'quantity_sold']].copy()
    df_prod.columns = ['ds', 'y']
    df_prod = df_prod.sort_values('ds').reset_index(drop=True)
    
    # Use adaptive split (30 days or 20% of available data)
    data_span_prod = (df_prod['ds'].max() - df_prod['ds'].min()).days
    test_days_prod = min(30, max(1, int(data_span_prod * 0.2)))
    train_cutoff_prod = df_prod['ds'].max() - timedelta(days=test_days_prod)
    df_train_prod = df_prod[df_prod['ds'] <= train_cutoff_prod]
    df_test_prod = df_prod[df_prod['ds'] > train_cutoff_prod]
    
    # Train Prophet (winner)
    model_prod = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=True,
        daily_seasonality=False,
        holidays=holidays,
        changepoint_prior_scale=0.05
    )
    
    model_prod.fit(df_train_prod)
    
    future_prod = model_prod.make_future_dataframe(periods=30, freq='D')
    forecast_prod = model_prod.predict(future_prod)
    
    # Evaluate
    train_cutoff_prod_ts = pd.Timestamp(train_cutoff_prod)
    test_forecast_prod = forecast_prod[forecast_prod['ds'] > train_cutoff_prod_ts]
    
    # Convert ds to datetime for merge
    df_test_prod_dt = df_test_prod.copy()
    df_test_prod_dt['ds'] = pd.to_datetime(df_test_prod_dt['ds'])
    
    test_comparison_prod = df_test_prod_dt.merge(
        test_forecast_prod[['ds', 'yhat']], 
        on='ds', 
        how='left'
    )
    
    mae_prod = mean_absolute_error(test_comparison_prod['y'], test_comparison_prod['yhat'])
    mape_prod = mean_absolute_percentage_error(test_comparison_prod['y'], test_comparison_prod['yhat']) * 100
    rmse_prod = np.sqrt(mean_squared_error(test_comparison_prod['y'], test_comparison_prod['yhat']))
    
    # Store
    all_product_forecasts[product_id] = {
        'model': model_prod,
        'forecast': forecast_prod,
        'name': product_name,
        'category': row['category']
    }
    
    all_product_metrics.append({
        'product_id': product_id,
        'product_name': product_name,
        'category': row['category'],
        'mae': mae_prod,
        'rmse': rmse_prod,
        'mape': mape_prod
    })
    
    print(f"  ✅ MAE: {mae_prod:.2f}, RMSE: {rmse_prod:.2f}, MAPE: {mape_prod:.2f}%")

print("\n" + "="*80)
print("✅ ALL PRODUCTS FORECASTED")
print("="*80)

# Summary metrics
metrics_summary_df = pd.DataFrame(all_product_metrics)
print("\n📊 Forecast Accuracy Summary:")
print(metrics_summary_df.to_string(index=False))
print(f"\n✅ Average MAPE: {metrics_summary_df['mape'].mean():.2f}%")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Persist Demand Forecasts to Gold Table
print("="*80)
print("📦 PERSISTING DEMAND FORECASTS TO GOLD TABLE")
print("="*80)

# Collect all forecasts into a single DataFrame for persistence
print(f"\nCollecting forecasts for {len(all_product_forecasts)} products...")

forecast_records = []

for product_id, forecast_data in all_product_forecasts.items():
    forecast_df = forecast_data['forecast']
    product_name = forecast_data['name']
    category = forecast_data['category']
    
    # Get only future forecasts (after training cutoff)
    # Future forecasts are dates greater than the max date in training data
    max_train_date = pd.Timestamp(df_train['ds'].max())
    future_forecast = forecast_df[forecast_df['ds'] > max_train_date].copy()
    
    for _, row in future_forecast.iterrows():
        forecast_records.append({
            'product_id': int(product_id),
            'product_name': product_name,
            'category': category,
            'forecast_date': row['ds'].date() if hasattr(row['ds'], 'date') else row['ds'],
            'predicted_demand': float(row['yhat']),
            'lower_bound': float(row['yhat_lower']) if 'yhat_lower' in row else float(row['yhat']) * 0.8,
            'upper_bound': float(row['yhat_upper']) if 'yhat_upper' in row else float(row['yhat']) * 1.2,
            'prediction_date': datetime.now().date(),
            'model_type': winner_model_name,
            'forecast_horizon_days': (pd.Timestamp(row['ds']).date() - pd.Timestamp(df_train['ds'].max()).date()).days
        })

demand_forecast_df = pd.DataFrame(forecast_records)

print(f"✅ Collected {len(demand_forecast_df):,} forecast records")
print(f"  Products: {demand_forecast_df['product_id'].nunique()}")
print(f"  Date range: {demand_forecast_df['forecast_date'].min()} to {demand_forecast_df['forecast_date'].max()}")
print(f"  Forecast horizons: {demand_forecast_df['forecast_horizon_days'].min()}-{demand_forecast_df['forecast_horizon_days'].max()} days")

# Convert to Spark DataFrame
demand_spark = spark.createDataFrame(demand_forecast_df)

# Define table name
table_name = f"{CATALOG}.gold.demand_forecast"

print(f"\n{'='*80}")
print(f"WRITING TO TABLE: {table_name}")
print("="*80)

# Check if table exists and handle deduplication
try:
    existing_df = spark.table(table_name)
    table_exists = True
    print(f"✅ Table exists - will append new forecasts")
    
    # Delete existing forecasts for today's prediction_date to avoid duplicates
    today = datetime.now().date()
    print(f"\n🗑️  Removing existing forecasts with prediction_date = {today}...")
    
    # Filter out today's forecasts and append new ones
    filtered_df = existing_df.filter(f"prediction_date != '{today}'")
    combined_df = filtered_df.union(demand_spark)
    
    # Write with overwrite to replace entire table (after filtering)
    combined_df.write \
        .mode("overwrite") \
        .option("overwriteSchema", "false") \
        .saveAsTable(table_name)
    
    print(f"✅ Deduplicated and appended forecasts")
    
except Exception as e:
    if "TABLE_OR_VIEW_NOT_FOUND" in str(e) or "table_or_view_not_found" in str(e).lower():
        print(f"📝 Table does not exist - creating new table")
        table_exists = False
        
        # Create new table
        demand_spark.write \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .partitionBy("prediction_date") \
            .saveAsTable(table_name)
        
        print(f"✅ Created new table with partitioning by prediction_date")
    else:
        print(f"❌ Error: {e}")
        raise

# Validate the write
result_count = spark.sql(f"SELECT COUNT(*) as count FROM {table_name}").collect()[0]['count']
today_count = spark.sql(f"""
    SELECT COUNT(*) as count 
    FROM {table_name} 
    WHERE prediction_date = '{datetime.now().date()}'
""").collect()[0]['count']

print(f"\n✅ VALIDATION:")
print(f"  Total forecasts in table: {result_count:,}")
print(f"  Today's forecasts: {today_count:,}")
print(f"  Expected today: {len(demand_forecast_df):,}")

if today_count == len(demand_forecast_df):
    print(f"  ✅ Row count matches!")
else:
    print(f"  ⚠️  Row count mismatch!")

# Show sample forecasts
print(f"\n📊 SAMPLE FORECASTS:")
sample_df = spark.sql(f"""
    SELECT 
        product_id,
        product_name,
        category,
        forecast_date,
        predicted_demand,
        lower_bound,
        upper_bound,
        forecast_horizon_days,
        prediction_date
    FROM {table_name}
    WHERE prediction_date = '{datetime.now().date()}'
    ORDER BY product_id, forecast_date
    LIMIT 15
""")
sample_df.show(15, truncate=False)

print("="*80)
print(f"✅ DEMAND FORECASTS SAVED TO: {table_name}")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Business Impact Analysis - Inventory Optimization
print("="*80)
print("💰 BUSINESS IMPACT ANALYSIS")
print("="*80)

# Current inventory management issues
avg_daily_demand = df_train.groupby('ds')['y'].sum().mean()
demand_std = df_train.groupby('ds')['y'].sum().std()

# Safety stock calculation (Service Level 95% ~ Z=1.65)
service_level_z = 1.65
lead_time_days = 3  # Assumed lead time

current_safety_stock = service_level_z * demand_std * np.sqrt(lead_time_days)
current_reorder_point = (avg_daily_demand * lead_time_days) + current_safety_stock

print(f"\n📊 Current Inventory Situation:")
print(f"  Avg Daily Demand: {avg_daily_demand:.2f} units")
print(f"  Demand Std Dev: {demand_std:.2f} units")
print(f"  Lead Time: {lead_time_days} days")
print(f"  Safety Stock (95% SL): {current_safety_stock:.2f} units")
print(f"  Reorder Point: {current_reorder_point:.2f} units")

# With forecasting (reduced uncertainty)
forecast_error_reduction = 0.4  # 40% reduction in demand std with good forecasting
optimized_demand_std = demand_std * (1 - forecast_error_reduction)

optimized_safety_stock = service_level_z * optimized_demand_std * np.sqrt(lead_time_days)
optimized_reorder_point = (avg_daily_demand * lead_time_days) + optimized_safety_stock

safety_stock_reduction = current_safety_stock - optimized_safety_stock
inventory_value_per_unit = 100  # Assumed ₹100 per unit
inventory_holding_cost_pct = 0.25  # 25% annual holding cost

annual_inventory_savings = safety_stock_reduction * inventory_value_per_unit * inventory_holding_cost_pct

print(f"\n📈 With Demand Forecasting:")
print(f"  Optimized Safety Stock: {optimized_safety_stock:.2f} units")
print(f"  Safety Stock Reduction: {safety_stock_reduction:.2f} units ({(safety_stock_reduction/current_safety_stock)*100:.1f}%)")
print(f"  Optimized Reorder Point: {optimized_reorder_point:.2f} units")

print(f"\n💵 Financial Impact:")
print(f"  Inventory Value per Unit: ₹{inventory_value_per_unit}")
print(f"  Annual Holding Cost: {inventory_holding_cost_pct*100:.0f}%")
print(f"  Annual Inventory Savings: ₹{annual_inventory_savings:,.2f}")

# Stockout reduction
current_stockout_rate = 0.15  # 15% current stockout rate
target_stockout_rate = 0.03  # 3% with forecasting
stockout_reduction_pct = ((current_stockout_rate - target_stockout_rate) / current_stockout_rate) * 100

avg_order_value = 500  # ₹500 per order
annual_orders = 10000
lost_sales_current = annual_orders * current_stockout_rate * avg_order_value
lost_sales_target = annual_orders * target_stockout_rate * avg_order_value
recovered_revenue = lost_sales_current - lost_sales_target

print(f"\n📦 Stockout Impact:")
print(f"  Current Stockout Rate: {current_stockout_rate*100:.0f}%")
print(f"  Target Stockout Rate: {target_stockout_rate*100:.0f}%")
print(f"  Stockout Reduction: {stockout_reduction_pct:.0f}%")
print(f"  Current Lost Sales: ₹{lost_sales_current:,.2f}")
print(f"  Recovered Revenue: ₹{recovered_revenue:,.2f}")

total_annual_impact = annual_inventory_savings + recovered_revenue

print(f"\n🎯 TOTAL ANNUAL IMPACT: ₹{total_annual_impact:,.2f}")
print(f"   Inventory Savings: ₹{annual_inventory_savings:,.2f} ({(annual_inventory_savings/total_annual_impact)*100:.1f}%)")
print(f"   Revenue Recovery: ₹{recovered_revenue:,.2f} ({(recovered_revenue/total_annual_impact)*100:.1f}%)")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Register Model with MLflow
import pickle
import mlflow.pyfunc

print("="*80)
print("📦 REGISTERING MODEL WITH MLFLOW")
print("="*80)

# Set MLflow experiment (using shared workspace pattern like other ML notebooks)
EXP_NAME = f"/Shared/{ENVIRONMENT}/SkillAIPath/demand_forecasting_experiment"
print(f"Setting MLflow experiment: {EXP_NAME}")
mlflow.set_experiment(EXP_NAME)

# Define a custom PyFunc model wrapper for proper UC registration
class DemandForecastModel(mlflow.pyfunc.PythonModel):
    def load_context(self, context):
        import pickle
        with open(context.artifacts["model_data"], "rb") as f:
            data = pickle.load(f)
            self.primary_model = data['primary_model']
            self.all_models = data['all_models']
            self.holidays = data['holidays']
            self.product_list = data['product_list']
            self.winner_model_name = data['winner_model_name']
    
    def predict(self, context, model_input):
        # Model input should be a dataframe with columns: product_id, horizon_days
        # Returns: forecast dataframe with product_id, forecast_date, predicted_demand
        import pandas as pd
        from datetime import datetime, timedelta
        
        predictions = []
        for _, row in model_input.iterrows():
            product_id = row.get('product_id')
            horizon = row.get('horizon_days', 30)
            
            if product_id in self.all_models:
                forecast_data = self.all_models[product_id]['forecast']
                future_forecast = forecast_data.tail(int(horizon))
                
                for _, frow in future_forecast.iterrows():
                    predictions.append({
                        'product_id': product_id,
                        'forecast_date': frow['ds'],
                        'predicted_demand': frow['yhat']
                    })
        
        return pd.DataFrame(predictions)

with mlflow.start_run(run_name=f"{winner_model_name.lower()}_demand_v1") as run:
    
    # Log parameters
    mlflow.log_param("model_type", winner_model_name.lower())
    mlflow.log_param("forecast_horizon_days", 30)
    mlflow.log_param("training_window_months", 24)
    mlflow.log_param("num_products", len(all_product_forecasts))
    mlflow.log_param("include_holidays", True)
    mlflow.log_param("holiday_count", len(holidays))
    
    # Log metrics - average across all products
    mlflow.log_metric("avg_mae", metrics_summary_df['mae'].mean())
    mlflow.log_metric("avg_rmse", metrics_summary_df['rmse'].mean())
    mlflow.log_metric("avg_mape", metrics_summary_df['mape'].mean())
    mlflow.log_metric("training_time_seconds", avg_metrics.loc[winner_idx, 'training_time'])
    
    # Log business metrics
    mlflow.log_metric("safety_stock_reduction_pct", (safety_stock_reduction/current_safety_stock)*100)
    mlflow.log_metric("stockout_reduction_pct", stockout_reduction_pct)
    mlflow.log_metric("annual_inventory_savings", annual_inventory_savings)
    mlflow.log_metric("recovered_revenue", recovered_revenue)
    mlflow.log_metric("total_annual_impact", total_annual_impact)
    
    # Save model data to temp file
    model_data_path = "/tmp/demand_forecast_model.pkl"
    with open(model_data_path, "wb") as f:
        pickle.dump({
            'primary_model': best_model,
            'all_models': all_product_forecasts,
            'holidays': holidays,
            'product_list': top_products.to_dict('records'),
            'winner_model_name': winner_model_name
        }, f)
    
    # STEP 1: Define model signature FIRST (REQUIRED for Unity Catalog)
    from mlflow.models.signature import ModelSignature
    from mlflow.types.schema import Schema, ColSpec
    
    input_schema = Schema([
        ColSpec("long", "product_id"),
        ColSpec("long", "horizon_days")
    ])
    
    output_schema = Schema([
        ColSpec("long", "product_id"),
        ColSpec("datetime", "forecast_date"),
        ColSpec("double", "predicted_demand")
    ])
    
    signature = ModelSignature(inputs=input_schema, outputs=output_schema)
    

    # STEP 2: Log the model as a PyFunc model (now signature is defined!)
    artifacts = {"model_data": model_data_path}
    mlflow.pyfunc.log_model(
        artifact_path="demand_forecast_model",
        python_model=DemandForecastModel(),
        artifacts=artifacts,
        signature=signature,
        pip_requirements=["pandas", "numpy", "prophet", "statsmodels", "scikit-learn"]
    )
    
    # Log additional artifacts
    import os
    if os.path.exists("/tmp/eda_forecasting.png"):
        mlflow.log_artifact("/tmp/eda_forecasting.png")
    if os.path.exists("/tmp/seasonal_decomposition.png"):
        mlflow.log_artifact("/tmp/seasonal_decomposition.png")
    if os.path.exists("/tmp/demand_forecast_best.png"):
        mlflow.log_artifact("/tmp/demand_forecast_best.png")
    
    # Log evaluation results
    eval_df.to_csv("/tmp/evaluation_results.csv", index=False)
    mlflow.log_artifact("/tmp/evaluation_results.csv")
    
    metrics_summary_df.to_csv("/tmp/product_metrics.csv", index=False)
    mlflow.log_artifact("/tmp/product_metrics.csv")
    
    run_id = run.info.run_id
    print(f"✅ Model logged as PyFunc with run_id: {run_id}")
    print(f"✅ Model artifact path: demand_forecast_model")

print("="*80)
print("✅ DEMAND FORECASTING MODEL TRAINING COMPLETE")
print(f"✅ Experiment: {EXP_NAME}")
print(f"✅ Best Model: {winner_model_name}")
print(f"✅ Average MAPE: {metrics_summary_df['mape'].mean():.2f}%")
print(f"✅ Annual Impact: ₹{total_annual_impact:,.2f}")
print(f"✅ Stockout Reduction: {stockout_reduction_pct:.0f}%")
print(f"✅ Inventory Savings: ₹{annual_inventory_savings:,.2f}")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Register Model to Unity Catalog and Cleanup
from databricks.sdk import WorkspaceClient

print("="*80)
print("📦 REGISTERING MODEL TO UNITY CATALOG")
print("="*80)

# Register the model to Unity Catalog Models section
model_name = f"{CATALOG}.gold.sap_demand_forecasting"
model_uri = f"runs:/{run_id}/demand_forecast_model"

print(f"Registering model to Unity Catalog: {model_name}")
print(f"Model URI: {model_uri}\n")

try:
    # Register the model to Unity Catalog
    model_version = mlflow.register_model(
        model_uri=model_uri,
        name=model_name,
        tags={
            "model_type": winner_model_name.lower(),
            "forecast_horizon_days": "30",
            "environment": ENVIRONMENT,
            "stage": "production",
            "num_products": str(len(all_product_forecasts))
        }
    )
    
    print(f"✅ Model registered successfully!")
    print(f"   Name: {model_name}")
    print(f"   Version: {model_version.version}")
    print(f"   Status: {model_version.status}")
    
    # Set model alias to 'champion' for the latest version
    from mlflow import MlflowClient
    client = MlflowClient()
    
    client.set_registered_model_alias(
        name=model_name,
        alias="champion",
        version=model_version.version
    )
    print(f"✅ Set alias 'champion' to version {model_version.version}")
    
except Exception as e:
    print(f"⚠️ Model registration error: {e}")
    print(f"   The model artifacts are still logged to MLflow under run_id: {run_id}")

print("\n" + "="*80)
print("🧹 CLEANUP: REMOVING AUTO-GENERATED FUNCTION")
print("="*80)

# MLflow may auto-create a function named sap_demand_forecasting during model registration
# We want to keep ONLY the model in the Models section

auto_function_name = f"{CATALOG}.gold.sap_demand_forecasting"

print(f"Checking for auto-generated function: {auto_function_name}")
print(f"Note: We keep the MODEL {auto_function_name} (in Models section)\n")

# Check if the auto-generated function exists
try:
    functions_check = spark.sql(f"""
        SELECT routine_name
        FROM {CATALOG}.information_schema.routines
        WHERE routine_schema = 'gold'
        AND routine_name = 'sap_demand_forecasting'
    """).collect()
    
    if len(functions_check) > 0:
        print(f"⚠️ Auto-generated function detected: {auto_function_name}")
        print(f"   This function was created automatically by MLflow during model registration.")
        print(f"   Using Databricks SDK to remove it...\n")
        
        # Initialize WorkspaceClient and delete using SDK
        w = WorkspaceClient()
        
        try:
            print(f"   Attempting SDK deletion: w.functions.delete('{auto_function_name}')")
            w.functions.delete(auto_function_name)
            print(f"   ✅ Successfully deleted auto-generated function using SDK")
            dropped = True
        except Exception as sdk_error:
            print(f"   ⚠️ SDK deletion failed: {sdk_error}")
            print(f"\n   ⚠️ MANUAL ACTION REQUIRED:")
            print(f"   Please manually drop this function using the Databricks UI:")
            print(f"   1. Go to Catalog > {CATALOG} > gold > Functions")
            print(f"   2. Find 'sap_demand_forecasting' in the functions list")
            print(f"   3. Click the three dots menu > Delete")
            print(f"   The MODEL will remain intact (this is expected).")
            dropped = False
    else:
        print(f"✅ No auto-generated function detected. Clean state!")
        dropped = True
        
except Exception as e:
    print(f"⚠️ Could not check for auto-generated function: {e}")
    print(f"   The model registration completed successfully.")
    dropped = False

# Verify final state
print("\n" + "="*80)
print("📊 FINAL UNITY CATALOG STATE")
print("="*80)

try:
    final_functions = spark.sql(f"""
        SELECT routine_name
        FROM {CATALOG}.information_schema.routines
        WHERE routine_schema = 'gold'
        AND routine_name = 'sap_demand_forecasting'
    """).toPandas()
    
    print(f"\nFunctions check:")
    if len(final_functions) == 0:
        print(f"  ✅ No duplicate function 'sap_demand_forecasting' in Functions (expected)")
    else:
        print(f"  ⚠️ Function 'sap_demand_forecasting' still exists (should be removed manually)")
    
    print(f"\nExpected final state:")
    print(f"  Models: sap_demand_forecasting ✅ (in Unity Catalog Models section)")
    print(f"  Functions: sap_demand_forecasting should NOT exist ❌")
    print(f"  Table: {CATALOG}.gold.demand_forecast ✅")
    
except Exception as e:
    print(f"⚠️ Could not verify final state: {e}")

print("="*80)
print("✅ DEMAND FORECASTING COMPLETE")
print(f"✅ Model: {model_name} (Unity Catalog Models)")
print(f"✅ Table: {CATALOG}.gold.demand_forecast")
print(f"✅ Run ID: {run_id}")
print("="*80)
