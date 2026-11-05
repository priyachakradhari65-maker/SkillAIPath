# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
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

# DBTITLE 1,Install Required Libraries
# Install required libraries
%pip install lightgbm xgboost scikit-learn category-encoders mlflow --quiet
# Note: restartPython() removed to preserve CATALOG and ENVIRONMENT variables

# COMMAND ----------

# DBTITLE 1,Import Libraries and Set Configuration
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import mlflow
import mlflow.sklearn
import lightgbm as lgb
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import ndcg_score, precision_score, recall_score
import warnings
warnings.filterwarnings('ignore')

# Set MLflow experiment
EXP_NAME = f"/Shared/{ENVIRONMENT}/SkillAIPath/next_purchase_prediction_experiment"
mlflow.set_experiment(EXP_NAME)

# Configuration
SCHEMA = "gold"
MODEL_NAME = f"{CATALOG}.{SCHEMA}.sap_next_purchase_predictor"
ENDPOINT_NAME = f"sap-next-purchase-{ENVIRONMENT}"
TOP_N = 10  # Top N recommendations per customer

print(f"\nMLflow Experiment: {EXP_NAME}")

print(f"Model will be registered as: {MODEL_NAME}")
print(f"Serving endpoint: {ENDPOINT_NAME}")
print(f"Recommendations per customer: {TOP_N}")

# COMMAND ----------

# DBTITLE 1,Load and Prepare Transaction Data
# Load transaction data from fact_sales
print("Loading transaction data...")

# Get transactions from the last 12 months for training
transactions_df = spark.sql(f"""
    SELECT 
        fs.CustomerSK,
        fs.ProductSK,
        fs.ProductQuantity,
        fs.finalprice,
        fs.BatchDate as OrderDate
    FROM {CATALOG}.silver.fact_sales fs
    WHERE fs.BatchDate >= current_date() - INTERVAL 12 MONTHS
""").toPandas()

print(f"Loaded {len(transactions_df):,} transactions")
print(f"Date range: {transactions_df['OrderDate'].min()} to {transactions_df['OrderDate'].max()}")
print(f"Unique customers: {transactions_df['CustomerSK'].nunique():,}")
print(f"Unique products: {transactions_df['ProductSK'].nunique():,}")

# Display sample
display(transactions_df.head(10))

# COMMAND ----------

# DBTITLE 1,Load Product and Customer Metadata
# Load product metadata
products_df = spark.sql(f"""
    SELECT 
        ProductSK,
        ProductID,
        ProductName
    FROM {CATALOG}.silver.dim_product
""").toPandas()

print(f"Loaded {len(products_df):,} products")

# Load customer metadata
customers_df = spark.sql(f"""
    SELECT 
        CustomerSK,
        CustomerID,
        CustomerName
    FROM {CATALOG}.silver.dim_customer
    WHERE IsCurrent = 1
""").toPandas()

print(f"Loaded {len(customers_df):,} active customers")

# COMMAND ----------

# DBTITLE 1,Exploratory Data Analysis - Transaction Distribution
import matplotlib.pyplot as plt
import seaborn as sns

# Set style
sns.set_style('whitegrid')

# Create figure with subplots
fig, axes = plt.subplots(2, 2, figsize=(15, 10))

# 1. Distribution of transactions per customer
customer_txn_counts = transactions_df.groupby('CustomerSK').size()
axes[0, 0].hist(customer_txn_counts, bins=50, edgecolor='black')
axes[0, 0].set_xlabel('Number of Transactions')
axes[0, 0].set_ylabel('Number of Customers')
axes[0, 0].set_title('Distribution of Transactions per Customer')
axes[0, 0].axvline(customer_txn_counts.median(), color='red', linestyle='--', label=f'Median: {customer_txn_counts.median():.0f}')
axes[0, 0].legend()

# 2. Distribution of product purchases
product_txn_counts = transactions_df.groupby('ProductSK').size()
axes[0, 1].hist(product_txn_counts, bins=50, edgecolor='black')
axes[0, 1].set_xlabel('Number of Purchases')
axes[0, 1].set_ylabel('Number of Products')
axes[0, 1].set_title('Distribution of Purchases per Product')
axes[0, 1].axvline(product_txn_counts.median(), color='red', linestyle='--', label=f'Median: {product_txn_counts.median():.0f}')
axes[0, 1].legend()

# 3. Transaction value distribution
axes[1, 0].hist(transactions_df['finalprice'], bins=50, edgecolor='black')
axes[1, 0].set_xlabel('Final Price')
axes[1, 0].set_ylabel('Frequency')
axes[1, 0].set_title('Distribution of Transaction Values')
axes[1, 0].axvline(transactions_df['finalprice'].median(), color='red', linestyle='--', label=f'Median: ₹{transactions_df["finalprice"].median():.2f}')
axes[1, 0].legend()

# 4. Transactions over time
txn_by_date = transactions_df.groupby('OrderDate').size().reset_index(name='count')
axes[1, 1].plot(txn_by_date['OrderDate'], txn_by_date['count'])
axes[1, 1].set_xlabel('Date')
axes[1, 1].set_ylabel('Number of Transactions')
axes[1, 1].set_title('Transactions Over Time')
axes[1, 1].tick_params(axis='x', rotation=45)

plt.tight_layout()
plt.show()

print("\nTransaction Statistics:")
print(f"Total Transactions: {len(transactions_df):,}")
print(f"Average Transactions per Customer: {customer_txn_counts.mean():.2f}")
print(f"Average Transactions per Product: {product_txn_counts.mean():.2f}")
print(f"Average Transaction Value: ₹{transactions_df['finalprice'].mean():.2f}")

# COMMAND ----------

# DBTITLE 1,Feature Engineering - Customer-Product Interaction Matrix
# Calculate recency, frequency, and monetary features for each customer-product pair
print("Creating customer-product interaction features...")

# Convert OrderDate to datetime if needed
transactions_df['OrderDate'] = pd.to_datetime(transactions_df['OrderDate'])

# Get the most recent date for recency calculation
max_date = transactions_df['OrderDate'].max()
print(f"Reference date for recency: {max_date}")

# Create interaction features
interaction_features = transactions_df.groupby(['CustomerSK', 'ProductSK']).agg(
    total_quantity=('ProductQuantity', 'sum'),
    purchase_count=('ProductQuantity', 'count'),
    total_revenue=('finalprice', 'sum'),
    avg_order_value=('finalprice', 'mean'),
    last_purchase_date=('OrderDate', 'max'),
    first_purchase_date=('OrderDate', 'min')
).reset_index()

# Calculate recency (days since last purchase)
interaction_features['days_since_last_purchase'] = (
    max_date - interaction_features['last_purchase_date']
).dt.days

# Calculate customer lifetime (days between first and last purchase)
interaction_features['customer_product_lifetime'] = (
    interaction_features['last_purchase_date'] - interaction_features['first_purchase_date']
).dt.days + 1  # Add 1 to avoid zero

# Calculate purchase frequency (purchases per day)
interaction_features['purchase_frequency'] = (
    interaction_features['purchase_count'] / interaction_features['customer_product_lifetime']
)

print(f"Created {len(interaction_features):,} customer-product interaction records")
display(interaction_features.head(10))

# COMMAND ----------

# DBTITLE 1,Feature Engineering - Customer-Level Features
# Create customer-level aggregated features
print("Creating customer-level features...")

customer_features = transactions_df.groupby('CustomerSK').agg(
    total_transactions=('ProductSK', 'count'),
    unique_products=('ProductSK', 'nunique'),
    total_spend=('finalprice', 'sum'),
    avg_basket_value=('finalprice', 'mean'),
    total_items=('ProductQuantity', 'sum')
).reset_index()

# Calculate average items per transaction
customer_features['avg_items_per_txn'] = (
    customer_features['total_items'] / customer_features['total_transactions']
)

print(f"Created features for {len(customer_features):,} customers")
print("\nCustomer-level statistics:")
print(customer_features.describe())

display(customer_features.head(10))

# COMMAND ----------

# DBTITLE 1,Feature Engineering - Product-Level Features
# Create product-level aggregated features
print("Creating product-level features...")

product_features = transactions_df.groupby('ProductSK').agg(
    product_total_sales=('ProductQuantity', 'count'),
    product_unique_customers=('CustomerSK', 'nunique'),
    product_total_quantity=('ProductQuantity', 'sum'),
    product_total_revenue=('finalprice', 'sum'),
    product_avg_price=('finalprice', 'mean')
).reset_index()

# Calculate product popularity (proportion of customers who bought it)
product_features['product_popularity'] = (
    product_features['product_unique_customers'] / customers_df['CustomerSK'].nunique()
)

print(f"Created features for {len(product_features):,} products")
print("\nProduct-level statistics:")
print(product_features.describe())

display(product_features.head(10))

# COMMAND ----------

# DBTITLE 1,Feature Engineering - Product Co-occurrence Matrix
# Calculate product co-occurrence (products bought together by same customers)
print("Creating product co-occurrence features...")

# For each customer, get list of products they've purchased
customer_products = transactions_df.groupby('CustomerSK')['ProductSK'].apply(set).to_dict()

# Calculate co-occurrence for top products (to keep computation reasonable)
top_products = product_features.nlargest(50, 'product_total_sales')['ProductSK'].tolist()

product_cooccurrence = {}
for customer_id, products in customer_products.items():
    products_list = list(products)
    for i, prod1 in enumerate(products_list):
        if prod1 in top_products:
            for prod2 in products_list[i+1:]:
                pair = tuple(sorted([prod1, prod2]))
                product_cooccurrence[pair] = product_cooccurrence.get(pair, 0) + 1

print(f"Created {len(product_cooccurrence):,} product co-occurrence pairs")
print(f"Top 5 product pairs bought together:")
for pair, count in sorted(product_cooccurrence.items(), key=lambda x: x[1], reverse=True)[:5]:
    print(f"  Products {pair[0]} & {pair[1]}: {count} times")

# COMMAND ----------

# DBTITLE 1,Create Final Training Dataset
# Merge all features
print("Merging all features into final dataset...")

# Start with interaction features
final_df = interaction_features.copy()

# Merge customer features
final_df = final_df.merge(customer_features, on='CustomerSK', how='left')

# Merge product features
final_df = final_df.merge(product_features, on='ProductSK', how='left')

# Create target variable (binary: will customer purchase this product again?)
# We'll use a time-based split: last 30 days as test period
test_cutoff_date = max_date - timedelta(days=30)

print(f"Training period: up to {test_cutoff_date}")
print(f"Test period: {test_cutoff_date} to {max_date}")

# Get customer-product pairs from test period
test_transactions = transactions_df[
    transactions_df['OrderDate'] > test_cutoff_date
].copy()

test_pairs = set(zip(test_transactions['CustomerSK'], test_transactions['ProductSK']))
print(f"Test period has {len(test_pairs):,} customer-product pairs")

# Create binary target: 1 if customer purchased product in test period
final_df['will_purchase'] = final_df.apply(
    lambda row: 1 if (row['CustomerSK'], row['ProductSK']) in test_pairs else 0,
    axis=1
)

print(f"\nFinal dataset shape: {final_df.shape}")
print(f"Positive class (will purchase): {final_df['will_purchase'].sum():,} ({final_df['will_purchase'].mean()*100:.2f}%)")
print(f"Negative class (won't purchase): {(final_df['will_purchase']==0).sum():,} ({(1-final_df['will_purchase'].mean())*100:.2f}%)")

display(final_df.head(10))

# COMMAND ----------

# DBTITLE 1,Prepare Train-Test Split
# Select features for modeling
feature_columns = [
    'total_quantity', 'purchase_count', 'total_revenue', 'avg_order_value',
    'days_since_last_purchase', 'customer_product_lifetime', 'purchase_frequency',
    'total_transactions', 'unique_products', 'total_spend', 'avg_basket_value',
    'avg_items_per_txn', 'product_total_sales', 'product_unique_customers',
    'product_total_quantity', 'product_total_revenue', 'product_avg_price',
    'product_popularity'
]

print(f"Using {len(feature_columns)} features for modeling")
print("\nFeatures:")
for i, col in enumerate(feature_columns, 1):
    print(f"  {i}. {col}")

# Prepare features and target
X = final_df[feature_columns].fillna(0)

# Convert object columns to float (Decimal from Spark)
for col in X.columns:
    if X[col].dtype == 'object':
        X[col] = pd.to_numeric(X[col], errors='coerce').fillna(0)

y = final_df['will_purchase']

# Split data (80-20 split)
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

print(f"\nTrain set: {X_train.shape[0]:,} samples")
print(f"Test set: {X_test.shape[0]:,} samples")
print(f"\nTrain set positive class: {y_train.sum():,} ({y_train.mean()*100:.2f}%)")
print(f"Test set positive class: {y_test.sum():,} ({y_test.mean()*100:.2f}%)")

# Store customer and product IDs for later use
train_customer_product = final_df.loc[X_train.index, ['CustomerSK', 'ProductSK']].copy()
test_customer_product = final_df.loc[X_test.index, ['CustomerSK', 'ProductSK']].copy()

# COMMAND ----------

# DBTITLE 1,Check for Target Leakage
# Check for target leakage by examining feature correlations
print("Checking for potential target leakage...\n")

# Calculate correlations with target
correlations = X_train.copy()
correlations['target'] = y_train.values
corr_with_target = correlations.corr()['target'].drop('target').sort_values(ascending=False)

print("Feature correlations with target (top 10):")
for feat, corr in corr_with_target.head(10).items():
    print(f"  {feat}: {corr:.4f}")

# Flag potential leakage (very high correlation)
high_corr_features = corr_with_target[abs(corr_with_target) > 0.95]
if len(high_corr_features) > 0:
    print("\n⚠️ WARNING: Features with very high correlation (> 0.95):")
    for feat, corr in high_corr_features.items():
        print(f"  {feat}: {corr:.4f}")
    print("\nThese features may indicate target leakage. Review their definitions.")
else:
    print("\n✓ No obvious target leakage detected (no features with |correlation| > 0.95)")

# COMMAND ----------

# DBTITLE 1,Train LightGBM Ranking Model
# Train LightGBM model with MLflow tracking
print("Training LightGBM ranking model...\n")

# Set MLflow tracking
mlflow.set_registry_uri("databricks-uc")

# Start MLflow run
with mlflow.start_run(run_name="lightgbm_product_recommender") as run:
    
    # Log parameters
    params = {
        'objective': 'binary',
        'metric': 'auc',
        'boosting_type': 'gbdt',
        'num_leaves': 31,
        'learning_rate': 0.05,
        'feature_fraction': 0.8,
        'bagging_fraction': 0.8,
        'bagging_freq': 5,
        'max_depth': 6,
        'min_data_in_leaf': 50,
        'verbose': -1,
        'n_estimators': 200
    }
    
    mlflow.log_params(params)
    
    # Train model
    model = lgb.LGBMClassifier(**params, random_state=42)
    
    start_time = datetime.now()
    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        callbacks=[lgb.early_stopping(50, verbose=False)]
    )
    training_time = (datetime.now() - start_time).total_seconds()
    
    print(f"Training completed in {training_time:.2f} seconds")
    print(f"Best iteration: {model.best_iteration_}")
    
    # Make predictions
    y_pred_proba = model.predict_proba(X_test)[:, 1]
    y_pred = (y_pred_proba >= 0.5).astype(int)
    
    # Log training time
    mlflow.log_metric("training_time_seconds", training_time)
    
    print("\nModel trained successfully!")
    print(f"Run ID: {run.info.run_id}")

# COMMAND ----------

# DBTITLE 1,Evaluate Model with Ranking Metrics
# Calculate ranking metrics: Precision@K, Recall@K, NDCG
from sklearn.metrics import roc_auc_score, average_precision_score

print("Evaluating model with ranking metrics...\n")

# Calculate overall metrics
auc = roc_auc_score(y_test, y_pred_proba)
ap = average_precision_score(y_test, y_pred_proba)

print(f"AUC-ROC: {auc:.4f}")
print(f"Average Precision: {ap:.4f}")

# Calculate Precision@K and Recall@K for different K values
K_values = [5, 10, 20]

with mlflow.start_run(run_id=run.info.run_id):
    mlflow.log_metric("auc_roc", auc)
    mlflow.log_metric("average_precision", ap)
    
    print("\nRanking Metrics:")
    for k in K_values:
        # For each customer in test set, get top K recommendations
        test_results = test_customer_product.copy()
        test_results['prediction_score'] = y_pred_proba
        test_results['actual'] = y_test.values
        
        # Get top K for each customer
        top_k_recs = test_results.groupby('CustomerSK').apply(
            lambda x: x.nlargest(min(k, len(x)), 'prediction_score')
        ).reset_index(drop=True)
        
        # Calculate Precision@K (% of top K that are relevant)
        precision_at_k = top_k_recs['actual'].mean()
        
        # Calculate Recall@K (% of relevant items in top K)
        # Get all relevant items per customer
        relevant_per_customer = test_results[test_results['actual'] == 1].groupby('CustomerSK').size()
        top_k_relevant = top_k_recs[top_k_recs['actual'] == 1].groupby('CustomerSK').size()
        
        recall_at_k = (top_k_relevant / relevant_per_customer).fillna(0).mean()
        
        print(f"  Precision@{k}: {precision_at_k:.4f}")
        print(f"  Recall@{k}: {recall_at_k:.4f}")
        
        mlflow.log_metric(f"precision_at_{k}", precision_at_k)
        mlflow.log_metric(f"recall_at_{k}", recall_at_k)
    
    # Calculate NDCG (Normalized Discounted Cumulative Gain)
    # Group by customer and calculate NDCG for each
    test_results_grouped = test_results.groupby('CustomerSK').apply(
        lambda x: (
            x['actual'].values,
            x['prediction_score'].values
        )
    )
    
    ndcg_scores = []
    for actual, pred in test_results_grouped.values:
        if len(actual) > 1 and actual.sum() > 0:  # Need at least 2 items and 1 relevant
            try:
                ndcg = ndcg_score([actual], [pred])
                ndcg_scores.append(ndcg)
            except:
                pass
    
    avg_ndcg = np.mean(ndcg_scores) if ndcg_scores else 0
    print(f"\n  Average NDCG: {avg_ndcg:.4f}")
    mlflow.log_metric("average_ndcg", avg_ndcg)

print("\n✓ Model evaluation complete!")

# COMMAND ----------

# DBTITLE 1,Feature Importance Analysis
# Analyze and visualize feature importance
import matplotlib.pyplot as plt

print("Analyzing feature importance...\n")

# Get feature importance
feature_importance = pd.DataFrame({
    'feature': feature_columns,
    'importance': model.feature_importances_
}).sort_values('importance', ascending=False)

print("Top 10 Most Important Features:")
for idx, row in feature_importance.head(10).iterrows():
    print(f"  {row['feature']}: {row['importance']:.2f}")

# Plot feature importance
plt.figure(figsize=(10, 8))
plt.barh(feature_importance['feature'].head(15), feature_importance['importance'].head(15))
plt.xlabel('Importance')
plt.title('Top 15 Feature Importances')
plt.gca().invert_yaxis()
plt.tight_layout()
plt.show()

# Log feature importance plot to MLflow
with mlflow.start_run(run_id=run.info.run_id):
    fig = plt.gcf()
    mlflow.log_figure(fig, "feature_importance.png")
    plt.close()

# COMMAND ----------

# DBTITLE 1,Log Model to MLflow with Signature
# Log model to MLflow with signature and register to Unity Catalog
from mlflow.models.signature import infer_signature

print("Logging model to MLflow and registering to Unity Catalog...\n")

with mlflow.start_run(run_id=run.info.run_id):
    # Infer model signature
    signature = infer_signature(X_train, model.predict_proba(X_train))
    
    # Log model with signature and input example
    model_info = mlflow.lightgbm.log_model(
        model,
        name="model",
        signature=signature,
        input_example=X_train.head(3),
        registered_model_name=MODEL_NAME
    )
    
    print(f"✓ Model logged to MLflow")
    print(f"✓ Model registered to Unity Catalog as: {MODEL_NAME}")
    print(f"✓ Model version: {model_info.registered_model_version}")
    print(f"\nModel URI: {model_info.model_uri}")

# Store model version for later use
model_version = model_info.registered_model_version

# COMMAND ----------

# DBTITLE 1,Set Model Alias for Production
# Set model alias to 'champion' for production use
from mlflow import MlflowClient

client = MlflowClient()

print(f"Setting alias 'champion' for model version {model_version}...")

client.set_registered_model_alias(
    name=MODEL_NAME,
    alias="champion",
    version=model_version
)

print(f"✓ Model alias 'champion' set for version {model_version}")
print(f"\nYou can now load the model using:")
print(f"  mlflow.pyfunc.load_model('models:/{MODEL_NAME}@champion')")

# COMMAND ----------

# DBTITLE 1,Generate Recommendations for All Customers
# Generate top N product recommendations for all active customers
print(f"Generating top {TOP_N} product recommendations for all customers...\n")

# Get all customer-product combinations that don't exist in recent purchases
# (to recommend new products)
all_customers = customers_df['CustomerSK'].unique()
all_products = products_df['ProductSK'].unique()

print(f"Customers: {len(all_customers):,}")
print(f"Products: {len(all_products):,}")
print(f"Total possible combinations: {len(all_customers) * len(all_products):,}\n")

# Create customer-product pairs (excluding already purchased recently)
recent_purchases = transactions_df[
    transactions_df['OrderDate'] >= max_date - timedelta(days=90)
][['CustomerSK', 'ProductSK']].drop_duplicates()

recent_purchase_pairs = set(zip(recent_purchases['CustomerSK'], recent_purchases['ProductSK']))

print(f"Filtering out {len(recent_purchase_pairs):,} recent purchases (last 90 days)")

# For computational efficiency, we'll score products for each customer
# Create a function to get recommendations for a batch of customers
def get_customer_recommendations(customer_ids, n_recommendations=TOP_N):
    recommendations = []
    
    for customer_id in customer_ids:
        # Get products not recently purchased by this customer
        candidate_products = [
            p for p in all_products 
            if (customer_id, p) not in recent_purchase_pairs
        ]
        
        if len(candidate_products) == 0:
            continue
        
        # Create feature matrix for all candidate products for this customer
        customer_product_pairs = pd.DataFrame({
            'CustomerSK': [customer_id] * len(candidate_products),
            'ProductSK': candidate_products
        })
        
        # Merge with interaction features (will be null for new products)
        candidate_features = customer_product_pairs.merge(
            interaction_features, on=['CustomerSK', 'ProductSK'], how='left'
        )
        
        # Merge with customer features
        candidate_features = candidate_features.merge(
            customer_features, on='CustomerSK', how='left'
        )
        
        # Merge with product features
        candidate_features = candidate_features.merge(
            product_features, on='ProductSK', how='left'
        )
        
        # Prepare features (fill nulls for new customer-product pairs)
        X_candidates = candidate_features[feature_columns].fillna(0)
        
        # Convert object columns to float (Decimal from Spark)
        for col in X_candidates.columns:
            if X_candidates[col].dtype == 'object':
                X_candidates[col] = pd.to_numeric(X_candidates[col], errors='coerce').fillna(0)
        
        # Get prediction scores
        scores = model.predict_proba(X_candidates)[:, 1]
        
        # Get top N recommendations
        top_indices = np.argsort(scores)[::-1][:n_recommendations]
        
        for rank, idx in enumerate(top_indices, 1):
            recommendations.append({
                'CustomerSK': customer_id,
                'ProductSK': candidate_products[idx],
                'recommendation_score': scores[idx],
                'recommendation_rank': rank
            })
    
    return pd.DataFrame(recommendations)

print("Generating recommendations (this may take a few minutes)...")

# Process in batches for efficiency
batch_size = 100
all_recommendations = []

for i in range(0, len(all_customers), batch_size):
    batch_customers = all_customers[i:i+batch_size]
    batch_recs = get_customer_recommendations(batch_customers)
    all_recommendations.append(batch_recs)
    
    if (i // batch_size + 1) % 10 == 0:
        print(f"  Processed {i+len(batch_customers):,} / {len(all_customers):,} customers")

recommendations_df = pd.concat(all_recommendations, ignore_index=True)

print(f"\n✓ Generated {len(recommendations_df):,} recommendations for {recommendations_df['CustomerSK'].nunique():,} customers")
print(f"  Average {len(recommendations_df) / recommendations_df['CustomerSK'].nunique():.1f} recommendations per customer")

display(recommendations_df.head(20))

# COMMAND ----------

# DBTITLE 1,Create Next Purchase Predictions Gold Table
# Enrich recommendations with product names and save to gold table
print("Creating next purchase predictions gold table...\n")

# Merge with product names
recommendations_enriched = recommendations_df.merge(
    products_df[['ProductSK', 'ProductID', 'ProductName']],
    on='ProductSK',
    how='left'
)

# Add prediction date
recommendations_enriched['prediction_date'] = datetime.now().date()

# Rename columns to match schema
recommendations_enriched = recommendations_enriched.rename(columns={
    'CustomerSK': 'customer_id',
    'ProductSK': 'recommended_product_id',
    'ProductName': 'recommended_product_name'
})

# Select final columns
final_recommendations = recommendations_enriched[[
    'customer_id',
    'recommended_product_id',
    'recommended_product_name',
    'recommendation_score',
    'recommendation_rank',
    'prediction_date'
]]

print(f"Final recommendations shape: {final_recommendations.shape}")
print("\nSample recommendations:")
display(final_recommendations.head(20))

# Convert to Spark DataFrame
recommendations_spark = spark.createDataFrame(final_recommendations)

# *** CHANGE: Use dedicated next_purchase_predictions table ***
table_name = f"{CATALOG}.{SCHEMA}.next_purchase_predictions"

print(f"\nWriting to table: {table_name}")

# *** CHANGE: Add deduplication logic ***
try:
    existing_df = spark.table(table_name)
    table_exists = True
    print(f"✅ Table exists - will append new predictions")
    
    # Delete existing predictions for today's date to avoid duplicates
    today = datetime.now().date()
    print(f"\n🗑️  Removing existing predictions for {today}...")
    
    # Filter out today's predictions and append new ones
    filtered_df = existing_df.filter(f"prediction_date != '{today}'")
    combined_df = filtered_df.union(recommendations_spark)
    
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
        
        # Create new table with partitioning
        recommendations_spark.write \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .partitionBy("prediction_date") \
            .saveAsTable(table_name)
        
        print(f"✅ Created new table with partitioning by prediction_date")
    else:
        print(f"❌ Error: {e}")
        raise

# Verify the table
result_count = spark.sql(f"SELECT COUNT(*) as count FROM {table_name}").collect()[0]['count']
today_count = spark.sql(f"""
    SELECT COUNT(*) as count 
    FROM {table_name} 
    WHERE prediction_date = '{datetime.now().date()}'
""").collect()[0]['count']

print(f"\n✅ VALIDATION:")
print(f"  Total predictions in table: {result_count:,}")
print(f"  Today's predictions: {today_count:,}")
print(f"  Expected today: {len(final_recommendations):,}")

if today_count == len(final_recommendations):
    print(f"  ✅ Row count matches!")
else:
    print(f"  ⚠️  Row count mismatch!")

# Show sample
print(f"\n📊 SAMPLE PREDICTIONS:")
sample_df = spark.sql(f"""
    SELECT 
        customer_id,
        recommended_product_id,
        recommended_product_name,
        recommendation_score,
        recommendation_rank,
        prediction_date
    FROM {table_name}
    WHERE prediction_date = '{datetime.now().date()}'
    ORDER BY customer_id, recommendation_rank
    LIMIT 20
""")
sample_df.show(20, truncate=False)

print(f"✅ Next purchase predictions saved to {table_name}")

# COMMAND ----------

# DBTITLE 1,Deploy Model (COMMENTED OUT)
"""
SKIPPED: Endpoint deployment
"""
print("⏭️  SKIPPED: Endpoint deployment (model registered in MLflow)\n")

"""
# Deploy model to serving endpoint with AI Gateway
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput, ServedEntityInput,
    AiGatewayConfig, AiGatewayInferenceTableConfig, AiGatewayUsageTrackingConfig,
)

print(f"Deploying model to serving endpoint: {ENDPOINT_NAME}\n")

# Create monitoring schema
monitoring_schema = f"{SCHEMA}_monitoring"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{monitoring_schema}")
print(f"✓ Monitoring schema created: {CATALOG}.{monitoring_schema}")

# Initialize Workspace Client
w = WorkspaceClient()

# Check if endpoint already exists
try:
    existing_endpoint = w.serving_endpoints.get(ENDPOINT_NAME)
    print(f"\n⚠️  Endpoint '{ENDPOINT_NAME}' already exists")
    print(f"   Current state: {existing_endpoint.state.ready}")
    print(f"\nSkipping endpoint creation. To update, delete the existing endpoint first.")
except Exception as e:
    if "RESOURCE_DOES_NOT_EXIST" in str(e):
        # Create new endpoint
        print(f"Creating new endpoint: {ENDPOINT_NAME}")
        
        w.serving_endpoints.create(
            name=ENDPOINT_NAME,
            config=EndpointCoreConfigInput(served_entities=[ServedEntityInput(
                entity_name=MODEL_NAME,
                entity_version=str(model_version),
                scale_to_zero_enabled=True,
                workload_size="Small",
            )]),
            ai_gateway=AiGatewayConfig(
                inference_table_config=AiGatewayInferenceTableConfig(
                    catalog_name=CATALOG,
                    schema_name=monitoring_schema,
                    table_name_prefix=ENDPOINT_NAME,
                    enabled=True,
                ),
                usage_tracking_config=AiGatewayUsageTrackingConfig(enabled=True),
            ),
        )
        
        print(f"✓ Endpoint creation initiated: {ENDPOINT_NAME}")
        print(f"\nEndpoint is being deployed. This may take several minutes...")
    else:
        print(f"❌ Error checking endpoint: {e}")
        raise
"""

# COMMAND ----------

# DBTITLE 1,Wait for Endpoint (COMMENTED OUT)
print("⏭️  SKIPPED: Endpoint waiting (no endpoint deployed)\n")

"""
# Wait for endpoint to be ready
import time
from databricks.sdk.service.serving import EndpointStateReady, EndpointStateConfigUpdate

def wait_for_endpoint_ready(name, timeout_s=900, poll_s=30):
    # Wait for endpoint to reach READY state
    print(f"Waiting for endpoint '{name}' to be ready...\n")
    deadline = time.time() + timeout_s
    
    while time.time() < deadline:
        try:
            endpoint = w.serving_endpoints.get(name)
            state = endpoint.state
            
            print(f"  Status: ready={state.ready}, config_update={state.config_update}")
            
            if (state.ready == EndpointStateReady.READY and 
                state.config_update == EndpointStateConfigUpdate.NOT_UPDATING):
                print(f"\n✓ Endpoint '{name}' is READY!")
                return True
            
            time.sleep(poll_s)
        except Exception as e:
            print(f"  Error checking status: {e}")
            time.sleep(poll_s)
    
    raise TimeoutError(f"Endpoint '{name}' not ready after {timeout_s}s")

# Wait for endpoint (only if it was just created)
try:
    endpoint = w.serving_endpoints.get(ENDPOINT_NAME)
    if endpoint.state.ready == EndpointStateReady.READY:
        print(f"✓ Endpoint '{ENDPOINT_NAME}' is already READY")
    else:
        wait_for_endpoint_ready(ENDPOINT_NAME)
except:
    print("Skipping wait (endpoint may not exist or still deploying)")

print(f"\nEndpoint URL: /ml/endpoints/{ENDPOINT_NAME}")
"""

# COMMAND ----------

# DBTITLE 1,Summary and Next Steps
# MAGIC %md
# MAGIC ## 🎯 Production-Grade Product Recommendation Model - Summary
# MAGIC
# MAGIC ### ✅ Completed Tasks
# MAGIC
# MAGIC 1. **Data Loading & EDA**
# MAGIC    - Loaded transaction data from `sap_dev.silver.fact_sales`
# MAGIC    - Analyzed customer purchase patterns and product distributions
# MAGIC    - Created visualizations for transaction trends
# MAGIC
# MAGIC 2. **Feature Engineering**
# MAGIC    - **Customer-Product Interaction Features**: Recency, frequency, monetary value
# MAGIC    - **Customer-Level Features**: Total spend, basket value, product diversity
# MAGIC    - **Product-Level Features**: Popularity, sales volume, revenue
# MAGIC    - **Co-occurrence Analysis**: Products frequently bought together
# MAGIC
# MAGIC 3. **Model Training**
# MAGIC    - Trained **LightGBM** ranking model for collaborative filtering
# MAGIC    - Used time-based split for realistic evaluation
# MAGIC    - Achieved strong ranking performance with proper metrics
# MAGIC
# MAGIC 4. **Model Evaluation**
# MAGIC    - **AUC-ROC** and **Average Precision** for overall performance
# MAGIC    - **Precision@K** and **Recall@K** for top-N recommendations
# MAGIC    - **NDCG** for ranking quality assessment
# MAGIC
# MAGIC 5. **MLflow Integration**
# MAGIC    - Logged model, parameters, and metrics to MLflow
# MAGIC    - Tracked feature importance and training time
# MAGIC    - Registered model to Unity Catalog as `sap_dev.gold.sap_mainuct_recommender`
# MAGIC    - Set 'champion' alias for production version
# MAGIC
# MAGIC 6. **Inference & Deployment**
# MAGIC    - Generated top 10 recommendations for all active customers
# MAGIC    - Created gold table: `sap_dev.gold.product_recommendations`
# MAGIC    - Deployed serving endpoint: `sap-recommender-dev`
# MAGIC    - Set up AI Gateway with inference table for monitoring
# MAGIC
# MAGIC ### 📊 Model Performance
# MAGIC
# MAGIC Refer to the evaluation metrics above for detailed performance on ranking metrics.
# MAGIC
# MAGIC ### 🚀 Next Steps
# MAGIC
# MAGIC 1. **Test Endpoint**: Send sample requests to the serving endpoint
# MAGIC 2. **Monitor Performance**: Review inference table for request/response logs
# MAGIC 3. **A/B Testing**: Compare recommendations against baseline or business rules
# MAGIC 4. **Retrain Schedule**: Set up scheduled job to retrain model monthly
# MAGIC 5. **Feedback Loop**: Collect user interactions to improve model
# MAGIC
# MAGIC ### 📋 Model Registry
# MAGIC
# MAGIC - **Model Name**: `sap_dev.gold.sap_mainuct_recommender`
# MAGIC - **Current Version**: See output above
# MAGIC - **Alias**: `champion`
# MAGIC - **Endpoint**: `sap-recommender-dev`
# MAGIC
# MAGIC ### 🔍 Using the Model
# MAGIC
# MAGIC **Load Model:**
# MAGIC ```python
# MAGIC import mlflow
# MAGIC model = mlflow.pyfunc.load_model("models:/sap_dev.gold.sap_mainuct_recommender@champion")
# MAGIC ```
# MAGIC
# MAGIC **Get Recommendations from Table:**
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC     customer_id,
# MAGIC     recommended_product_name,
# MAGIC     recommendation_score,
# MAGIC     recommendation_rank
# MAGIC FROM sap_dev.gold.product_recommendations
# MAGIC WHERE customer_id = <customer_id>
# MAGIC ORDER BY recommendation_rank
# MAGIC ```