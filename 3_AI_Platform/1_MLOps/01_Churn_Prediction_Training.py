# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,SkillAIPath Churn Prediction - ML Pipeline
# MAGIC %md
# MAGIC # SkillAIPath Customer Churn Prediction
# MAGIC
# MAGIC ## ML Pipeline - End-to-End
# MAGIC
# MAGIC **Purpose:** Train and register churn prediction model using RFM features
# MAGIC
# MAGIC **Input:** `sap_{env}.gold.mart_rfm_analysis`
# MAGIC
# MAGIC **Output:** MLflow registered model `sap_churn_predictor`
# MAGIC
# MAGIC **Features:**
# MAGIC - Recency (days since last purchase)
# MAGIC - Frequency (number of purchases)
# MAGIC - Monetary (total spend)
# MAGIC - RFM Score (composite risk score)
# MAGIC
# MAGIC **Model:** XGBoost Classifier
# MAGIC
# MAGIC **Target:** Predict customer churn (inactive >90 days)
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
# Install ML dependencies
print("Installing ML libraries...")
%pip install xgboost scikit-learn mlflow databricks-sdk --quiet

# Re-declare after pip install (pip clears variables)
import re

try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    match = re.search(r'/Shared/([^/]+)/SkillAIPath', notebook_path)
    
    if match:
        ENVIRONMENT = match.group(1)
        CATALOG = f"sap_{ENVIRONMENT}"
    else:
        ENVIRONMENT = "dev"
        CATALOG = "sap_dev"
except:
    ENVIRONMENT = "dev"
    CATALOG = "sap_dev"

print(f"✅ Catalog: {CATALOG}, Environment: {ENVIRONMENT}")


# COMMAND ----------

# DBTITLE 1,Load & Prepare Data
import mlflow
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from datetime import datetime, timedelta

print("\n" + "="*80)
print("STEP 1: LOAD DATA FROM GOLD LAYER")
print("="*80)

# Load RFM analysis data
df_rfm = spark.table(f"{CATALOG}.gold.mart_rfm_analysis").toPandas()
print(f"✅ Loaded {len(df_rfm):,} customers from mart_rfm_analysis")
print(f"\nColumns: {df_rfm.columns.tolist()}")

# Load customer 360 for additional features
df_customer = spark.table(f"{CATALOG}.gold.mart_customer_360").toPandas()
print(f"✅ Loaded {len(df_customer):,} customers from mart_customer_360")

# Calculate days since first purchase
from datetime import datetime
today = pd.Timestamp.now()
df_customer['days_since_first_purchase'] = (today - pd.to_datetime(df_customer['FirstPurchaseDate'])).dt.days

# Merge datasets - CustomerSK from RFM joins to CustomerSK in Customer360
df = df_rfm.merge(
    df_customer[['CustomerKey', 'days_since_first_purchase', 'TotalPurchases']], 
    left_on='CustomerSK',
    right_on='CustomerKey',
    how='left'
)

print(f"\n✅ Merged dataset: {len(df):,} rows, {len(df.columns)} features")
print(f"\nSample data:")
print(df.head())

# COMMAND ----------

# DBTITLE 1,Feature Engineering & Target Creation
print("\n" + "="*80)
print("STEP 2: FEATURE ENGINEERING")
print("="*80)

# Calculate recency distribution first
df['recency_days'] = df['Recency'].abs()

print(f"Recency distribution (days since last purchase):")
print(f"  Min: {df['recency_days'].min()} days")
print(f"  Max: {df['recency_days'].max()} days")
print(f"  Mean: {df['recency_days'].mean():.1f} days")
print(f"  Median: {df['recency_days'].median():.1f} days")

# Set threshold at median for balanced class distribution (demo data is 2 years old)
CHURN_THRESHOLD_DAYS = int(df['recency_days'].median())
print(f"\nUsing threshold: {CHURN_THRESHOLD_DAYS} days (median for balanced classes)")

# Create target variable: Churn if inactive for > threshold days
df['is_churned'] = (df['recency_days'] > CHURN_THRESHOLD_DAYS).astype(int)

print(f"\nChurn threshold: {CHURN_THRESHOLD_DAYS} days")
print(f"Churned customers: {df['is_churned'].sum():,} ({df['is_churned'].mean()*100:.1f}%)")
print(f"Active customers: {(1-df['is_churned']).sum():,} ({(1-df['is_churned'].mean())*100:.1f}%)")

# Convert RFM_Score string to numeric (e.g. "555" -> 15)
df['rfm_score_numeric'] = df['R_Score'] + df['F_Score'] + df['M_Score']

# Feature engineering
df['rfm_score_normalized'] = df['rfm_score_numeric'] / 15.0  # Normalize to 0-1
df['monetary_per_frequency'] = df['Monetary_Lakhs'] / (df['Frequency'] + 1)  # Average order value
df['customer_tenure_months'] = df['days_since_first_purchase'] / 30.0
df['purchase_frequency_per_month'] = df['Frequency'] / (df['customer_tenure_months'] + 1)

# Select features for model (use correct column names + recency_days)
feature_cols = [
    'recency_days',
    'Frequency', 
    'Monetary_Lakhs',
    'rfm_score_numeric',
    'rfm_score_normalized',
    'monetary_per_frequency',
    'customer_tenure_months',
    'purchase_frequency_per_month'
]

target_col = 'is_churned'

print(f"\n✅ Features: {feature_cols}")
print(f"✅ Target: {target_col}")

# Handle missing values
df[feature_cols] = df[feature_cols].fillna(0)

print(f"\n✅ Dataset ready: {len(df):,} rows, {len(feature_cols)} features")

# COMMAND ----------

# DBTITLE 1,Train/Test Split
print("\n" + "="*80)
print("STEP 3: TRAIN/TEST SPLIT")
print("="*80)

X = df[feature_cols]
y = df[target_col]

# 80/20 split with stratification
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

print(f"Training set: {len(X_train):,} samples")
print(f"  Churned: {y_train.sum():,} ({y_train.mean()*100:.1f}%)")
print(f"  Active: {(~y_train.astype(bool)).sum():,} ({(1-y_train.mean())*100:.1f}%)")

print(f"\nTest set: {len(X_test):,} samples")
print(f"  Churned: {y_test.sum():,} ({y_test.mean()*100:.1f}%)")
print(f"  Active: {(~y_test.astype(bool)).sum():,} ({(1-y_test.mean())*100:.1f}%)")

# Scale features
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

print(f"\n✅ Features scaled using StandardScaler")

# COMMAND ----------

# DBTITLE 1,Train XGBoost Model with MLflow
import xgboost as xgb
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score, confusion_matrix, classification_report

print("\n" + "="*80)
print("STEP 4: TRAIN XGBOOST MODEL")
print("="*80)

# Set MLflow experiment (MLflow will create if doesn't exist)
EXP_NAME = f"/Shared/{ENVIRONMENT}/SkillAIPath/churn_prediction_experiment"
print(f"Setting MLflow experiment: {EXP_NAME}")
mlflow.set_experiment(EXP_NAME)

# Start MLflow run
with mlflow.start_run(run_name=f"churn_xgboost_{ENVIRONMENT}_{datetime.now().strftime('%Y%m%d_%H%M%S')}") as run:
    
    # Log parameters
    params = {
        'max_depth': 6,
        'learning_rate': 0.1,
        'n_estimators': 100,
        'objective': 'binary:logistic',
        'eval_metric': 'logloss',
        'random_state': 42
    }
    
    mlflow.log_params(params)
    mlflow.log_param('environment', ENVIRONMENT)
    mlflow.log_param('catalog', CATALOG)
    mlflow.log_param('churn_threshold_days', CHURN_THRESHOLD_DAYS)
    mlflow.log_param('train_size', len(X_train))
    mlflow.log_param('test_size', len(X_test))
    
    # Train model
    print("\nTraining XGBoost model...")
    model = xgb.XGBClassifier(**params)
    model.fit(X_train_scaled, y_train, verbose=False)
    
    # Predictions
    y_pred = model.predict(X_test_scaled)
    y_pred_proba = model.predict_proba(X_test_scaled)[:, 1]
    
    # Calculate metrics
    accuracy = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred)
    recall = recall_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred)
    roc_auc = roc_auc_score(y_test, y_pred_proba)
    
    # Log metrics
    mlflow.log_metric('accuracy', accuracy)
    mlflow.log_metric('precision', precision)
    mlflow.log_metric('recall', recall)
    mlflow.log_metric('f1_score', f1)
    mlflow.log_metric('roc_auc', roc_auc)
    
    print(f"\n✅ Model trained successfully")
    print(f"\nPerformance Metrics:")
    print(f"  Accuracy:  {accuracy:.4f}")
    print(f"  Precision: {precision:.4f}")
    print(f"  Recall:    {recall:.4f}")
    print(f"  F1 Score:  {f1:.4f}")
    print(f"  ROC-AUC:   {roc_auc:.4f}")
    
    # Confusion matrix
    cm = confusion_matrix(y_test, y_pred)
    print(f"\nConfusion Matrix:")
    print(f"  TN: {cm[0,0]:,}  |  FP: {cm[0,1]:,}")
    print(f"  FN: {cm[1,0]:,}  |  TP: {cm[1,1]:,}")
    
    # Log model with signature
    from mlflow.models.signature import infer_signature
    signature = infer_signature(X_train_scaled, model.predict(X_train_scaled))
    
    mlflow.xgboost.log_model(
        model, 
        "model",
        signature=signature,
        input_example=X_train_scaled[:5]
    )
    
    # Log feature importance
    feature_importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    print(f"\nTop 5 Important Features:")
    for idx, row in feature_importance.head().iterrows():
        print(f"  {row['feature']:30s} {row['importance']:.4f}")
    
    mlflow.log_dict(feature_importance.to_dict(), "feature_importance.json")
    
    run_id = run.info.run_id
    print(f"\n✅ MLflow Run ID: {run_id}")

# COMMAND ----------

# DBTITLE 1,Register Model to Unity Catalog
print("\n" + "="*80)
print("STEP 5: REGISTER MODEL TO UNITY CATALOG")
print("="*80)

# Model name includes environment for isolation
model_name = f"{CATALOG}.gold.sap_churn_predictor"

print(f"\nRegistering model: {model_name}")

# Register model to Unity Catalog
model_uri = f"runs:/{run_id}/model"
registered_model = mlflow.register_model(model_uri, model_name)

print(f"✅ Model registered successfully")
print(f"  Name: {registered_model.name}")
print(f"  Version: {registered_model.version}")

# Add model description and tags
from mlflow.tracking import MlflowClient
client = MlflowClient()

client.update_model_version(
    name=model_name,
    version=registered_model.version,
    description=f"""Customer Churn Prediction Model - {ENVIRONMENT.upper()}
    
    **Model Type:** XGBoost Binary Classifier
    **Training Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    **Environment:** {ENVIRONMENT}
    
    **Performance:**
    - Accuracy: {accuracy:.4f}
    - Precision: {precision:.4f}
    - Recall: {recall:.4f}
    - F1 Score: {f1:.4f}
    - ROC-AUC: {roc_auc:.4f}
    
    **Features:**
    {', '.join(feature_cols)}
    
    **Business Use Case:**
    Predicts customer churn risk based on RFM analysis. Identifies customers 
    inactive for >{CHURN_THRESHOLD_DAYS} days who may need retention campaigns.
    """
)

# Set alias for easy reference
client.set_registered_model_alias(model_name, "champion", registered_model.version)

print(f"✅ Model alias set: 'champion' → version {registered_model.version}")

print("\n" + "="*80)
print("✅ MODEL TRAINING COMPLETE")
print("="*80)
print(f"Model: {model_name}")
print(f"Version: {registered_model.version}")
print(f"Alias: champion")
print(f"Performance: {f1:.4f} F1, {roc_auc:.4f} ROC-AUC")
print(f"Next: Deploy to serving endpoint")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Generate & Persist Churn Predictions
print("\n" + "="*80)
print("STEP 6: GENERATE CHURN PREDICTIONS FOR ALL CUSTOMERS")
print("="*80)

# Generate predictions for ALL customers using the trained model
print(f"\nGenerating churn predictions for {len(df):,} customers...")

# Prepare full dataset
X_all = df[feature_cols]
X_all_scaled = scaler.transform(X_all)

# Generate predictions
df_predictions = df[['CustomerSK', 'Recency', 'Frequency', 'Monetary_Lakhs']].copy()
df_predictions['churn_probability'] = model.predict_proba(X_all_scaled)[:, 1]
df_predictions['is_churned_predicted'] = model.predict(X_all_scaled)
df_predictions['prediction_date'] = datetime.now().date()
df_predictions['model_version'] = str(registered_model.version)
df_predictions['churn_threshold_days'] = CHURN_THRESHOLD_DAYS

# Rename columns for clarity
df_predictions = df_predictions.rename(columns={
    'CustomerSK': 'customer_id',
    'Recency': 'recency_days',
    'Frequency': 'total_purchases',
    'Monetary_Lakhs': 'lifetime_value_lakhs'
})

# Ensure recency is positive days
df_predictions['recency_days'] = df_predictions['recency_days'].abs()

# Add risk segment based on probability
df_predictions['risk_segment'] = pd.cut(
    df_predictions['churn_probability'],
    bins=[0, 0.3, 0.7, 1.0],
    labels=['Low Risk', 'Medium Risk', 'High Risk']
)

print(f"✅ Predictions generated")
print(f"\nRisk Distribution:")
print(df_predictions['risk_segment'].value_counts().to_string())
print(f"\nChurn Prediction Distribution:")
print(f"  Predicted Active: {(df_predictions['is_churned_predicted'] == 0).sum():,}")
print(f"  Predicted Churned: {(df_predictions['is_churned_predicted'] == 1).sum():,}")

# Convert to Spark DataFrame
predictions_spark = spark.createDataFrame(df_predictions)

# Define table name
table_name = f"{CATALOG}.gold.churn_predictions"

print(f"\n" + "="*80)
print(f"WRITING TO TABLE: {table_name}")
print("="*80)

# Check if table exists
try:
    existing_df = spark.table(table_name)
    table_exists = True
    print(f"✅ Table exists - will append new predictions")
    
    # Delete existing predictions for today's date to avoid duplicates
    today = datetime.now().date()
    print(f"\n🗑️  Removing existing predictions for {today}...")
    
    # Filter out today's predictions and append new ones
    filtered_df = existing_df.filter(f"prediction_date != '{today}'")
    combined_df = filtered_df.union(predictions_spark)
    
    # Write with overwrite to replace entire table (after filtering)
    combined_df.write \
        .mode("overwrite") \
        .option("overwriteSchema", "false") \
        .saveAsTable(table_name)
    
    print(f"✅ Deduplicated and appended predictions")
    
except Exception as e:
    if "TABLE_OR_VIEW_NOT_FOUND" in str(e) or "table_or_view_not_found" in str(e).lower():
        print(f"📝 Table does not exist - creating new table")
        table_exists = False
        
        # Create new table
        predictions_spark.write \
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
print(f"  Total predictions in table: {result_count:,}")
print(f"  Today's predictions: {today_count:,}")
print(f"  Expected today: {len(df_predictions):,}")

if today_count == len(df_predictions):
    print(f"  ✅ Row count matches!")
else:
    print(f"  ⚠️  Row count mismatch!")

# Show sample predictions
print(f"\n📊 SAMPLE PREDICTIONS:")
sample_df = spark.sql(f"""
    SELECT 
        customer_id,
        churn_probability,
        is_churned_predicted,
        risk_segment,
        recency_days,
        total_purchases,
        lifetime_value_lakhs,
        prediction_date,
        model_version
    FROM {table_name}
    WHERE prediction_date = '{datetime.now().date()}'
    ORDER BY churn_probability DESC
    LIMIT 10
""")
sample_df.show(10, truncate=False)

print("="*80)
print(f"✅ CHURN PREDICTIONS SAVED TO: {table_name}")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Deploy to Serving Endpoint
"""
# Endpoint deployment skipped - model registered in MLflow
print("\n" + "="*80)
print("⏭️  STEP 6: ENDPOINT DEPLOYMENT SKIPPED")
print("="*80)
print(f"\n✅ Model successfully registered at: {model_name}")
print(f"✅ Model version: {registered_model.version}")
print(f"✅ Alias: champion")
print("\n💡 To deploy endpoint later, uncomment the deployment code in this cell")
print("="*80 + "\n")
"""

# COMMAND ----------

# DBTITLE 1,Test Endpoint with Sample Predictions
"""
# Endpoint testing skipped
print("\n" + "="*80)
print("⏭️  STEP 7: ENDPOINT TESTING SKIPPED")
print("="*80)
print("\n🚧 No endpoint deployed - skipping test predictions")
print("="*80 + "\n")
"""

# COMMAND ----------

# DBTITLE 1,Deploy Model to Serving Endpoint & Test
"""
# Training complete!
print("\n" + "="*80)
print("🎉 CHURN PREDICTION MODEL TRAINING COMPLETE")
print("="*80)
print(f"\n✅ Model: {model_name}")
print(f"✅ Version: {registered_model.version}")
print(f"✅ Catalog: {CATALOG}")
print(f"✅ Environment: {ENVIRONMENT}")
print(f"✅ Model Performance: F1={f1:.4f}, ROC-AUC={roc_auc:.4f}")
print(f"\n📊 Data Processed: {len(df):,} customers")
print("\n🚀 Ready for production inference!")
print("="*80 + "\n")
"""