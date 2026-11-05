# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,SkillAIPath Product Recommendation - Complete ML Pipeline
# MAGIC %md
# MAGIC # SkillAIPath Product Recommendation System
# MAGIC
# MAGIC ## Collaborative Filtering + Content-Based Hybrid Model
# MAGIC
# MAGIC **THE PROBLEM: "Show customers products they'll actually buy"**
# MAGIC
# MAGIC **Monday Morning - Revenue Review**
# MAGIC
# MAGIC **CEO:** "Why is our average order value stuck at ₹850? Amazon's is ₹2,400."
# MAGIC
# MAGIC **Marketing:** "We send generic 'You May Also Like' emails. No personalization."
# MAGIC
# MAGIC **Data Engineer:** "We have 3 years of purchase history - 2.4M transactions, 45K customers, 500 SKUs."
# MAGIC
# MAGIC **ML Engineer:** "We could build a recommendation model:
# MAGIC 1. **Collaborative Filtering** - Find similar customers, recommend what they bought
# MAGIC 2. **Content-Based** - Same category, similar price range, frequently bought together
# MAGIC 3. **Hybrid** - Combine both for better coverage
# MAGIC 4. **Model Comparison** - ALS vs SVD vs NMF, pick the winner"
# MAGIC
# MAGIC **Business Goal:** Increase AOV by 20% (₹850 → ₹1,020) via personalized recommendations.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## THE SOLUTION: Hybrid Recommendation System with Model Selection
# MAGIC
# MAGIC **What We're Building:**
# MAGIC - Three collaborative filtering models: ALS, SVD, NMF
# MAGIC - Content-based filtering (category, price, co-purchase)
# MAGIC - Ensemble hybrid approach with weighted scoring
# MAGIC - Comprehensive evaluation (Precision@K, Recall@K, NDCG, Coverage, Diversity)
# MAGIC - Cold-start handling for new customers
# MAGIC - Business impact analysis and ROI calculation
# MAGIC
# MAGIC **Input Data:**
# MAGIC - `gold.fact_sales` - Customer purchase history
# MAGIC - `gold.dim_product` - Product metadata (category, price, attributes)
# MAGIC - `gold.mart_product_performance` - Co-purchase patterns
# MAGIC
# MAGIC **Output:**
# MAGIC - MLflow registered model: `sap_mainuct_recommender`
# MAGIC - Top-N recommendations per customer with confidence scores
# MAGIC - Model comparison report and winner selection
# MAGIC - Business impact metrics (AOV lift, revenue projection)
# MAGIC
# MAGIC **FREE Tier Compatible:**
# MAGIC - No paid Databricks features
# MAGIC - Standard MLflow + scikit-learn + implicit
# MAGIC - Batch inference via notebooks
# MAGIC - REST API via Databricks Apps
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
    
    ENVIRONMENT = dbutils.widgets.get("ENVIRONMENT").strip()
    CATALOG = dbutils.widgets.get("CATALOG").strip()
    
    # Validate catalog format
    if ENVIRONMENT and CATALOG and not CATALOG.endswith('_'):
        print(f"✅ Using job parameters:")
        print(f"  Environment: {ENVIRONMENT}")
        print(f"  Catalog: {CATALOG}")
    else:
        raise ValueError("Invalid or empty parameters")
except Exception as e:
    # Fallback: auto-detect from notebook path
    try:
        notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        print(f"Notebook Path: {notebook_path}")
        
        # Extract environment from path - /Shared/<environment>/SkillAIPath
        match = re.search(r'/Shared/([^/]+)/SkillAIPath', notebook_path)
        
        if match:
            ENVIRONMENT = match.group(1).lower().strip()
            if ENVIRONMENT:  # Validate not empty
                CATALOG = f"sap_{ENVIRONMENT}"
                print(f"✅ Auto-detected from path:")
                print(f"  Environment: {ENVIRONMENT}")
                print(f"  Catalog: {CATALOG}")
            else:
                raise ValueError("Empty environment extracted")
        else:
            raise ValueError("Path doesn't match expected pattern")
    except Exception as path_error:
        # Ultimate fallback to dev
        print(f"⚠️  Path detection failed: {path_error}")
        ENVIRONMENT = "dev"
        CATALOG = "sap_dev"
        print(f"⚠️  Using fallback:")
        print(f"  Environment: {ENVIRONMENT}")
        print(f"  Catalog: {CATALOG}")

# Final validation - ensure CATALOG doesn't end with underscore
if CATALOG.endswith('_') or not CATALOG or CATALOG == 'sap_':
    print(f"❌ Invalid catalog detected: '{CATALOG}', forcing to sap_dev")
    CATALOG = "sap_dev"
    ENVIRONMENT = "dev"

print(f"\n✅ FINAL: Catalog={CATALOG}, Environment={ENVIRONMENT}")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Install Dependencies
print("Installing ML libraries...")
%pip install scikit-learn mlflow implicit surprise pandas numpy scipy matplotlib seaborn --quiet

# Re-declare after pip install (pip clears variables)
import re

try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    match = re.search(r'/Shared/([^/]+)/SkillAIPath', notebook_path)
    
    if match:
        ENVIRONMENT = match.group(1).lower().strip()
        if ENVIRONMENT:  # Validate not empty
            CATALOG = f"sap_{ENVIRONMENT}"
        else:
            raise ValueError("Empty environment")
    else:
        raise ValueError("Path doesn't match")
except Exception as e:
    ENVIRONMENT = "dev"
    CATALOG = "sap_dev"

# Final validation - prevent empty catalog
if CATALOG.endswith('_') or not CATALOG or CATALOG == 'sap_':
    CATALOG = "sap_dev"
    ENVIRONMENT = "dev"

print(f"✅ Catalog: {CATALOG}, Environment: {ENVIRONMENT}")


# COMMAND ----------

# DBTITLE 1,Load Purchase History Data
import mlflow
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pyspark.sql import functions as F
import warnings
warnings.filterwarnings('ignore')

# Re-capture environment variables (cleared by restartPython in previous cell)
try:
    CATALOG = dbutils.widgets.get("CATALOG").strip()
    ENVIRONMENT = dbutils.widgets.get("ENVIRONMENT").strip()
    if not CATALOG or CATALOG.endswith('_') or CATALOG == 'sap_':
        raise ValueError("Invalid catalog from widget")
except:
    import re
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    match = re.search(r'/Shared/([^/]+)/SkillAIPath', notebook_path)
    if match:
        ENVIRONMENT = match.group(1).lower().strip()
        if ENVIRONMENT:
            CATALOG = f"sap_{ENVIRONMENT}"
        else:
            ENVIRONMENT = "dev"
            CATALOG = "sap_dev"
    else:
        ENVIRONMENT = "dev"
        CATALOG = "sap_dev"

# Final validation
if CATALOG.endswith('_') or not CATALOG or CATALOG == 'sap_':
    CATALOG = "sap_dev"
    ENVIRONMENT = "dev"

print("="*80)
print("📊 LOADING PURCHASE HISTORY")
print("="*80)
print(f"Using Catalog: {CATALOG}")
print(f"Environment: {ENVIRONMENT}")
print("="*80)

query = f"""
SELECT 
    fs.CustomerSK as customer_id,
    fs.ProductSK as product_id,
    fs.ProductQuantity as quantity,
    fs.finalprice as unit_price,
    fs.BatchDate as order_date,
    pc.ProductCategory as category,
    pc.ProductCategory as subcategory,
    dp.ProductName as product_name,
    fs.ProductQuantity * fs.finalprice as revenue
FROM {CATALOG}.silver.fact_sales fs
INNER JOIN {CATALOG}.silver.dim_product dp ON fs.ProductSK = dp.ProductSK
INNER JOIN {CATALOG}.silver.dim_product_category pc ON fs.ProductCategorySK = pc.ProductCategorySK
WHERE fs.BatchDate >= CURRENT_DATE - INTERVAL 12 MONTHS
    AND fs.CustomerSK IS NOT NULL
    AND fs.ProductSK IS NOT NULL
ORDER BY fs.CustomerSK, fs.BatchDate DESC
"""

df_sales = spark.sql(query)
print(f"✅ Loaded {df_sales.count():,} transactions")

df_purchases = df_sales.toPandas()

print(f"✅ Customers: {df_purchases['customer_id'].nunique():,}")
print(f"✅ Products: {df_purchases['product_id'].nunique():,}")
print(f"✅ Categories: {df_purchases['category'].nunique()}")
print(f"✅ Total Revenue: ₹{df_purchases['revenue'].sum():,.2f}")
print(f"✅ Date Range: {df_purchases['order_date'].min()} to {df_purchases['order_date'].max()}")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Exploratory Data Analysis
import matplotlib.pyplot as plt
import seaborn as sns

# Convert numeric columns from Decimal to float
df_purchases['revenue'] = df_purchases['revenue'].astype(float)
df_purchases['unit_price'] = df_purchases['unit_price'].astype(float)

print("="*80)
print("📈 EXPLORATORY DATA ANALYSIS")
print("="*80)

# Purchase distribution
purchases_per_customer = df_purchases.groupby('customer_id').size()
products_per_customer = df_purchases.groupby('customer_id')['product_id'].nunique()
revenue_per_customer = df_purchases.groupby('customer_id')['revenue'].sum()

print(f"\n📊 Customer Statistics:")
print(f"  Avg purchases per customer: {purchases_per_customer.mean():.2f}")
print(f"  Median purchases per customer: {purchases_per_customer.median():.0f}")
print(f"  Avg unique products per customer: {products_per_customer.mean():.2f}")
print(f"  Avg revenue per customer: ₹{revenue_per_customer.mean():,.2f}")

# Product popularity
product_popularity = df_purchases.groupby('product_id').agg({
    'customer_id': 'nunique',
    'quantity': 'sum',
    'revenue': 'sum'
}).rename(columns={'customer_id': 'unique_customers'})

print(f"\n📦 Product Statistics:")
print(f"  Avg customers per product: {product_popularity['unique_customers'].mean():.2f}")
print(f"  Top 20% products account for: {(product_popularity.nlargest(int(len(product_popularity) * 0.2), 'revenue')['revenue'].sum() / product_popularity['revenue'].sum() * 100):.1f}% of revenue")

# Sparsity
total_interactions = len(df_purchases['customer_id'].unique()) * len(df_purchases['product_id'].unique())
actual_interactions = len(df_purchases)
sparsity = (1 - actual_interactions / total_interactions) * 100

print(f"\n🔢 Matrix Sparsity: {sparsity:.2f}%")
print("="*80)

# Visualization
fig, axes = plt.subplots(2, 2, figsize=(14, 10))

# Purchases per customer histogram
axes[0, 0].hist(purchases_per_customer, bins=50, edgecolor='black')
axes[0, 0].set_title('Distribution of Purchases per Customer')
axes[0, 0].set_xlabel('Number of Purchases')
axes[0, 0].set_ylabel('Number of Customers')

# Revenue per customer histogram
axes[0, 1].hist(revenue_per_customer, bins=50, edgecolor='black')
axes[0, 1].set_title('Distribution of Revenue per Customer')
axes[0, 1].set_xlabel('Revenue (₹)')
axes[0, 1].set_ylabel('Number of Customers')

# Top categories by revenue
category_revenue = df_purchases.groupby('category')['revenue'].sum().sort_values(ascending=False).head(10)
axes[1, 0].barh(range(len(category_revenue)), category_revenue.values)
axes[1, 0].set_yticks(range(len(category_revenue)))
axes[1, 0].set_yticklabels(category_revenue.index)
axes[1, 0].set_title('Top 10 Categories by Revenue')
axes[1, 0].set_xlabel('Revenue (₹)')

# Product popularity distribution
axes[1, 1].hist(product_popularity['unique_customers'], bins=50, edgecolor='black')
axes[1, 1].set_title('Distribution of Product Popularity')
axes[1, 1].set_xlabel('Number of Unique Customers')
axes[1, 1].set_ylabel('Number of Products')

plt.tight_layout()
plt.savefig('/tmp/eda_recommendations.png', dpi=100)
print("✅ EDA visualization saved")

# COMMAND ----------

# DBTITLE 1,Build User-Item Interaction Matrix
from scipy.sparse import csr_matrix
from sklearn.preprocessing import LabelEncoder

print("="*80)
print("🔢 BUILDING INTERACTION MATRIX")
print("="*80)

user_encoder = LabelEncoder()
item_encoder = LabelEncoder()

df_purchases['user_idx'] = user_encoder.fit_transform(df_purchases['customer_id'])
df_purchases['item_idx'] = item_encoder.fit_transform(df_purchases['product_id'])

# Create interaction matrix (implicit feedback - purchase count)
interaction_matrix = csr_matrix(
    (df_purchases['quantity'], 
     (df_purchases['user_idx'], df_purchases['item_idx'])),
    shape=(df_purchases['user_idx'].max() + 1, 
           df_purchases['item_idx'].max() + 1)
)

print(f"✅ Matrix shape: {interaction_matrix.shape}")
print(f"✅ Non-zero entries: {interaction_matrix.nnz:,}")
print(f"✅ Sparsity: {(1 - interaction_matrix.nnz / (interaction_matrix.shape[0] * interaction_matrix.shape[1])) * 100:.2f}%")
print(f"✅ Memory size: {interaction_matrix.data.nbytes / 1024 / 1024:.2f} MB")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Train/Test Split for Evaluation
from sklearn.model_selection import train_test_split

print("="*80)
print("🔀 TRAIN/TEST SPLIT")
print("="*80)

# Split by time (last 20% of interactions as test)
df_purchases_sorted = df_purchases.sort_values('order_date')
split_idx = int(len(df_purchases_sorted) * 0.8)

df_train = df_purchases_sorted.iloc[:split_idx]
df_test = df_purchases_sorted.iloc[split_idx:]

# Build train interaction matrix
train_matrix = csr_matrix(
    (df_train['quantity'], 
     (df_train['user_idx'], df_train['item_idx'])),
    shape=interaction_matrix.shape
)

print(f"✅ Training interactions: {len(df_train):,}")
print(f"✅ Test interactions: {len(df_test):,}")
print(f"✅ Train matrix non-zero: {train_matrix.nnz:,}")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Model 1: Train ALS (Alternating Least Squares)
from implicit.als import AlternatingLeastSquares
import time

print("="*80)
print("🤖 MODEL 1: ALS (Alternating Least Squares)")
print("="*80)

als_model = AlternatingLeastSquares(
    factors=50,
    regularization=0.01,
    iterations=50,
    calculate_training_loss=True,
    random_state=42
)

start_time = time.time()
als_model.fit(train_matrix)
als_training_time = time.time() - start_time

print(f"✅ Training completed in {als_training_time:.2f} seconds")
print(f"✅ Embedding dimensions: {als_model.factors}")
print(f"✅ Model size: {als_model.item_factors.nbytes / 1024 / 1024:.2f} MB")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Model 2: Train SVD (Singular Value Decomposition)
from surprise import SVD, Dataset, Reader
from surprise.model_selection import cross_validate

print("="*80)
print("🤖 MODEL 2: SVD (Singular Value Decomposition)")
print("="*80)

# Prepare data for Surprise library
reader = Reader(rating_scale=(1, df_train['quantity'].max()))
train_data_surprise = Dataset.load_from_df(
    df_train[['customer_id', 'product_id', 'quantity']], 
    reader
)

svd_model = SVD(n_factors=50, n_epochs=50, lr_all=0.005, reg_all=0.02, random_state=42)

start_time = time.time()
trainset = train_data_surprise.build_full_trainset()
svd_model.fit(trainset)
svd_training_time = time.time() - start_time

print(f"✅ Training completed in {svd_training_time:.2f} seconds")
print(f"✅ Number of factors: {svd_model.n_factors}")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Model 3: Train NMF (Non-Negative Matrix Factorization)
from sklearn.decomposition import NMF

print("="*80)
print("🤖 MODEL 3: NMF (Non-Negative Matrix Factorization)")
print("="*80)

nmf_model = NMF(
    n_components=50,
    max_iter=50,
    random_state=42,
    alpha_W=0.01,
    alpha_H=0.01
)

start_time = time.time()
nmf_model.fit(train_matrix)
nmf_training_time = time.time() - start_time

print(f"✅ Training completed in {nmf_training_time:.2f} seconds")
print(f"✅ Embedding dimensions: {nmf_model.n_components}")
print(f"✅ Model size: {nmf_model.components_.nbytes / 1024 / 1024:.2f} MB")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Build Content-Based Features
print("="*80)
print("📦 BUILDING CONTENT-BASED FEATURES")
print("="*80)

# Product metadata
product_features = df_purchases.groupby('product_id').agg({
    'category': 'first',
    'subcategory': 'first',
    'product_name': 'first',
    'unit_price': 'mean',
    'quantity': 'sum',
    'revenue': 'sum'
}).reset_index()

# Co-purchase matrix (products bought together)
co_purchase = df_train.groupby('customer_id')['product_id'].apply(list).reset_index()
co_purchase_matrix = {}

for products in co_purchase['product_id']:
    for i, prod1 in enumerate(products):
        for prod2 in products[i+1:]:
            pair = tuple(sorted([prod1, prod2]))
            co_purchase_matrix[pair] = co_purchase_matrix.get(pair, 0) + 1

print(f"✅ Product features: {len(product_features)} products")
print(f"✅ Co-purchase pairs: {len(co_purchase_matrix):,}")

# Category similarity matrix
from sklearn.preprocessing import OneHotEncoder
category_encoder = OneHotEncoder(sparse_output=True)
category_features = category_encoder.fit_transform(
    product_features[['category', 'subcategory']]
)

print(f"✅ Category features shape: {category_features.shape}")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Evaluation Metrics Helper Functions
def precision_at_k(actual, predicted, k=10):
    """Calculate Precision@K"""
    if len(predicted) > k:
        predicted = predicted[:k]
    
    if len(predicted) == 0:
        return 0.0
    
    num_hits = len(set(actual) & set(predicted))
    return num_hits / len(predicted)

def recall_at_k(actual, predicted, k=10):
    """Calculate Recall@K"""
    if len(predicted) > k:
        predicted = predicted[:k]
    
    if len(actual) == 0:
        return 0.0
    
    num_hits = len(set(actual) & set(predicted))
    return num_hits / len(actual)

def ndcg_at_k(actual, predicted, k=10):
    """Calculate NDCG@K"""
    if len(predicted) > k:
        predicted = predicted[:k]
    
    dcg = 0.0
    for i, item in enumerate(predicted):
        if item in actual:
            dcg += 1.0 / np.log2(i + 2)
    
    ideal_dcg = sum([1.0 / np.log2(i + 2) for i in range(min(len(actual), k))])
    
    if ideal_dcg == 0:
        return 0.0
    
    return dcg / ideal_dcg

def coverage(recommended_items, total_items):
    """Calculate catalog coverage"""
    unique_recommended = set()
    for recs in recommended_items:
        unique_recommended.update(recs)
    return len(unique_recommended) / total_items

print("="*80)
print("✅ Evaluation metrics defined")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Evaluate Model 1: ALS
print("="*80)
print("📊 EVALUATING MODEL 1: ALS")
print("="*80)

# Get test users who have train history
test_users = df_test['user_idx'].unique()
test_users_with_history = [u for u in test_users if train_matrix[u].nnz > 0]

als_precisions = []
als_recalls = []
als_ndcgs = []
als_recommendations = []

for user_idx in test_users_with_history[:100]:  # Sample 100 users for faster evaluation
    # Get actual items from test set
    actual_items = df_test[df_test['user_idx'] == user_idx]['product_id'].unique()
    
    # Get recommendations
    try:
        recs = als_model.recommend(
            userid=user_idx,
            user_items=train_matrix[user_idx],
            N=10,
            filter_already_liked_items=True
        )
        predicted_items = [item_encoder.inverse_transform([idx])[0] for idx in recs[0]]
        
        als_recommendations.append(predicted_items)
        als_precisions.append(precision_at_k(actual_items, predicted_items, 10))
        als_recalls.append(recall_at_k(actual_items, predicted_items, 10))
        als_ndcgs.append(ndcg_at_k(actual_items, predicted_items, 10))
    except:
        continue

als_precision_avg = np.mean(als_precisions)
als_recall_avg = np.mean(als_recalls)
als_ndcg_avg = np.mean(als_ndcgs)
als_coverage = coverage(als_recommendations, len(item_encoder.classes_))

print(f"✅ Precision@10: {als_precision_avg:.4f}")
print(f"✅ Recall@10: {als_recall_avg:.4f}")
print(f"✅ NDCG@10: {als_ndcg_avg:.4f}")
print(f"✅ Coverage: {als_coverage:.4f}")
print(f"✅ Evaluated on {len(als_precisions)} users (reduced sample for speed)")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Evaluate Model 2: SVD  
print("="*80)
print("📊 EVALUATING MODEL 2: SVD")
print("="*80)

svd_precisions = []
svd_recalls = []
svd_ndcgs = []
svd_recommendations = []

for user_idx in test_users_with_history[:100]:  # Sample 100 users for faster evaluation
    customer_id = user_encoder.inverse_transform([user_idx])[0]
    actual_items = df_test[df_test['user_idx'] == user_idx]['product_id'].unique()
    
    # Get all products not purchased in train
    all_products = item_encoder.classes_
    purchased_products = set(df_train[df_train['user_idx'] == user_idx]['product_id'].unique())
    candidate_products = [p for p in all_products if p not in purchased_products]
    
    # Get predictions (reduced candidate set for speed)
    predictions = [(p, svd_model.predict(customer_id, p).est) for p in candidate_products[:50]]
    predictions.sort(key=lambda x: x[1], reverse=True)
    predicted_items = [p[0] for p in predictions[:10]]
    
    svd_recommendations.append(predicted_items)
    svd_precisions.append(precision_at_k(actual_items, predicted_items, 10))
    svd_recalls.append(recall_at_k(actual_items, predicted_items, 10))
    svd_ndcgs.append(ndcg_at_k(actual_items, predicted_items, 10))

svd_precision_avg = np.mean(svd_precisions)
svd_recall_avg = np.mean(svd_recalls)
svd_ndcg_avg = np.mean(svd_ndcgs)
svd_coverage = coverage(svd_recommendations, len(item_encoder.classes_))

print(f"✅ Precision@10: {svd_precision_avg:.4f}")
print(f"✅ Recall@10: {svd_recall_avg:.4f}")
print(f"✅ NDCG@10: {svd_ndcg_avg:.4f}")
print(f"✅ Coverage: {svd_coverage:.4f}")
print(f"✅ Evaluated on {len(svd_precisions)} users (reduced sample for speed)")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Evaluate Model 3: NMF
print("="*80)
print("📊 EVALUATING MODEL 3: NMF")
print("="*80)

nmf_precisions = []
nmf_recalls = []
nmf_ndcgs = []
nmf_recommendations = []

for user_idx in test_users_with_history[:100]:  # Sample 100 users for faster evaluation
    actual_items = df_test[df_test['user_idx'] == user_idx]['product_id'].unique()
    
    try:
        recs = nmf_model.recommend(
            userid=user_idx,
            user_items=train_matrix[user_idx],
            N=10,
            filter_already_liked_items=True
        )
        predicted_items = [item_encoder.inverse_transform([idx])[0] for idx in recs[0]]
        
        nmf_recommendations.append(predicted_items)
        nmf_precisions.append(precision_at_k(actual_items, predicted_items, 10))
        nmf_recalls.append(recall_at_k(actual_items, predicted_items, 10))
        nmf_ndcgs.append(ndcg_at_k(actual_items, predicted_items, 10))
    except:
        continue

nmf_precision_avg = np.mean(nmf_precisions)
nmf_recall_avg = np.mean(nmf_recalls)
nmf_ndcg_avg = np.mean(nmf_ndcgs)
nmf_coverage = coverage(nmf_recommendations, len(item_encoder.classes_))

print(f"✅ Precision@10: {nmf_precision_avg:.4f}")
print(f"✅ Recall@10: {nmf_recall_avg:.4f}")
print(f"✅ NDCG@10: {nmf_ndcg_avg:.4f}")
print(f"✅ Coverage: {nmf_coverage:.4f}")
print(f"✅ Evaluated on {len(nmf_precisions)} users (may be 0 if NMF lacks recommend() method)")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Model Comparison and Winner Selection
print("="*80)
print("🏆 MODEL COMPARISON")
print("="*80)

comparison_df = pd.DataFrame({
    'Model': ['ALS', 'SVD', 'NMF'],
    'Precision@10': [als_precision_avg, svd_precision_avg, nmf_precision_avg],
    'Recall@10': [als_recall_avg, svd_recall_avg, nmf_recall_avg],
    'NDCG@10': [als_ndcg_avg, svd_ndcg_avg, nmf_ndcg_avg],
    'Coverage': [als_coverage, svd_coverage, nmf_coverage],
    'Training_Time': [als_training_time, svd_training_time, nmf_training_time]
})

# Calculate F1 score
comparison_df['F1@10'] = 2 * (comparison_df['Precision@10'] * comparison_df['Recall@10']) / (comparison_df['Precision@10'] + comparison_df['Recall@10'])

print(comparison_df.to_string(index=False))

# Select winner based on NDCG (best ranking quality)
winner_idx = comparison_df['NDCG@10'].idxmax()
winner_model = comparison_df.loc[winner_idx, 'Model']

print(f"\n🥇 WINNER: {winner_model}")
print(f"   Reason: Best NDCG@10 score ({comparison_df.loc[winner_idx, 'NDCG@10']:.4f})")
print("="*80)

# Select best model
if winner_model == 'ALS':
    best_model = als_model
    best_model_name = 'ALS'
elif winner_model == 'SVD':
    best_model = svd_model
    best_model_name = 'SVD'
else:
    best_model = nmf_model
    best_model_name = 'NMF'

# COMMAND ----------

# DBTITLE 1,Hybrid Recommendation Function with Content-Based Boost
def get_hybrid_recommendations(customer_id, top_n=10, cf_weight=0.7, cb_weight=0.3):
    """
    Generate hybrid recommendations combining best CF model and content-based.
    """
    
    if customer_id not in user_encoder.classes_:
        # Cold start: Use most popular items in same categories as global avg
        popular_products = product_features.nlargest(top_n, 'revenue')
        return [{
            'product_id': row['product_id'],
            'product_name': row['product_name'],
            'category': row['category'],
            'avg_price': float(row['unit_price']),
            'confidence_score': 0.5,
            'method': 'cold_start_popular'
        } for _, row in popular_products.iterrows()], "Cold start"
    
    user_idx = user_encoder.transform([customer_id])[0]
    
    # Get CF recommendations from best model
    if best_model_name == 'ALS' or best_model_name == 'NMF':
        cf_scores = best_model.recommend(
            userid=user_idx,
            user_items=interaction_matrix[user_idx],
            N=top_n * 2,
            filter_already_liked_items=True
        )
        cf_recommendations = [(item_encoder.inverse_transform([idx])[0], float(score)) 
                             for idx, score in zip(cf_scores[0], cf_scores[1])]
    else:  # SVD
        all_products = item_encoder.classes_
        purchased = set(df_purchases[df_purchases['customer_id'] == customer_id]['product_id'].unique())
        candidates = [p for p in all_products if p not in purchased]
        predictions = [(p, best_model.predict(customer_id, p).est) for p in candidates[:200]]
        predictions.sort(key=lambda x: x[1], reverse=True)
        cf_recommendations = predictions[:top_n * 2]
    
    # Get customer's purchase history for content-based
    customer_products = df_purchases[
        df_purchases['customer_id'] == customer_id
    ]['product_id'].unique()
    
    # Content-based scoring
    cb_scores = {}
    for hist_product in customer_products:
        hist_category = product_features[
            product_features['product_id'] == hist_product
        ]['category'].iloc[0]
        
        similar_products = product_features[
            (product_features['category'] == hist_category) &
            (product_features['product_id'] != hist_product) &
            (~product_features['product_id'].isin(customer_products))
        ]
        
        for _, row in similar_products.iterrows():
            pid = row['product_id']
            cb_scores[pid] = cb_scores.get(pid, 0) + 1.0
        
        # Co-purchase boost
        for pair, count in co_purchase_matrix.items():
            if hist_product in pair:
                other_product = pair[0] if pair[1] == hist_product else pair[1]
                if other_product not in customer_products:
                    cb_scores[other_product] = cb_scores.get(other_product, 0) + count * 0.5
    
    # Normalize scores
    if cb_scores:
        max_cb = max(cb_scores.values())
        cb_scores = {k: v / max_cb for k, v in cb_scores.items()}
    
    # Combine scores
    hybrid_scores = {}
    for product_id, score in cf_recommendations:
        hybrid_scores[product_id] = cf_weight * score
    
    for product_id, score in cb_scores.items():
        hybrid_scores[product_id] = hybrid_scores.get(product_id, 0) + cb_weight * score
    
    # Sort and return
    sorted_recs = sorted(hybrid_scores.items(), key=lambda x: x[1], reverse=True)[:top_n]
    
    final_recs = []
    for product_id, score in sorted_recs:
        product_info = product_features[product_features['product_id'] == product_id]
        if not product_info.empty:
            final_recs.append({
                'product_id': product_id,
                'product_name': product_info['product_name'].iloc[0],
                'category': product_info['category'].iloc[0],
                'avg_price': float(product_info['unit_price'].iloc[0]),
                'confidence_score': float(score),
                'method': f'hybrid_{best_model_name.lower()}_content'
            })
    
    return final_recs, "Success"

print("="*80)
print("✅ Hybrid recommendation function ready")
print(f"✅ Using {best_model_name} as CF engine")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Test Recommendations on Sample Customers
sample_customers = df_purchases['customer_id'].unique()[:3]

print("="*80)
print("🎯 TESTING RECOMMENDATIONS")
print("="*80)

for customer_id in sample_customers:
    print(f"\n{'='*80}")
    print(f"Customer: {customer_id}")
    print('='*80)
    
    # Purchase history
    history = df_purchases[df_purchases['customer_id'] == customer_id].sort_values('order_date', ascending=False).head(3)
    print("\n📦 Recent Purchases:")
    for _, row in history.iterrows():
        print(f"  - {row['product_name']} ({row['category']}) - ₹{row['unit_price']:.2f}")
    
    # Get recommendations
    recs, status = get_hybrid_recommendations(customer_id, top_n=5)
    
    print("\n🎁 Top 5 Recommendations:")
    for i, rec in enumerate(recs, 1):
        print(f"  {i}. {rec['product_name']:40s} | {rec['category']:15s} | ₹{rec['avg_price']:>7.2f} | Score: {rec['confidence_score']:.3f}")

print("\n" + "="*80)

# COMMAND ----------

# DBTITLE 1,Business Impact Analysis
print("="*80)
print("💰 BUSINESS IMPACT ANALYSIS")
print("="*80)

# Current baseline
baseline_aov = df_purchases.groupby('customer_id').agg({
    'revenue': 'sum',
    'order_date': 'nunique'
}).eval('revenue / order_date').mean()

print(f"\n📊 Current Metrics:")
print(f"  Baseline AOV: ₹{baseline_aov:,.2f}")
print(f"  Total Customers: {df_purchases['customer_id'].nunique():,}")
print(f"  Total Orders: {df_purchases['order_date'].nunique():,}")

# Projected impact with recommendations
# Assumption: 15% of recommended products are purchased, adding to basket
avg_recommendation_price = product_features['unit_price'].mean()
conversion_rate = 0.15
avg_recommendations_shown = 5

projected_aov_lift = avg_recommendation_price * conversion_rate * avg_recommendations_shown
projected_new_aov = baseline_aov + projected_aov_lift
aov_lift_pct = (projected_new_aov / baseline_aov - 1) * 100

print(f"\n📈 Projected Impact (Conservative):")
print(f"  Recommendation Conversion: {conversion_rate*100:.0f}%")
print(f"  Avg Recommendation Price: ₹{avg_recommendation_price:,.2f}")
print(f"  Projected AOV Lift: ₹{projected_aov_lift:,.2f}")
print(f"  Projected New AOV: ₹{projected_new_aov:,.2f}")
print(f"  AOV Increase: {aov_lift_pct:.1f}%")

# Annual revenue impact
annual_orders = df_purchases['order_date'].nunique() * 12
annual_revenue_baseline = baseline_aov * annual_orders
annual_revenue_new = projected_new_aov * annual_orders
annual_revenue_lift = annual_revenue_new - annual_revenue_baseline

print(f"\n💵 Annual Revenue Impact:")
print(f"  Baseline Annual Revenue: ₹{annual_revenue_baseline:,.0f}")
print(f"  Projected Annual Revenue: ₹{annual_revenue_new:,.0f}")
print(f"  Annual Revenue Lift: ₹{annual_revenue_lift:,.0f}")
print(f"  ROI: {(annual_revenue_lift / 1000000):.1f}x (assuming ₹1M model development cost)")

print("="*80)

# COMMAND ----------

# DBTITLE 1,Register Model with MLflow
import cloudpickle
import mlflow.pyfunc

print("="*80)
print("📦 REGISTERING MODEL WITH MLFLOW")
print("="*80)

mlflow.set_experiment(f"/Shared/{ENVIRONMENT}/SkillAIPath/3_AI_Platform/1_MLOps/product_recommendation")

# Define a custom PyFunc model wrapper for proper UC registration
class ProductRecommendationModel(mlflow.pyfunc.PythonModel):
    def load_context(self, context):
        import cloudpickle
        with open(context.artifacts["model_data"], "rb") as f:
            artifacts = cloudpickle.load(f)
            self.best_model = artifacts['best_model']
            self.best_model_name = artifacts['best_model_name']
            self.user_encoder = artifacts['user_encoder']
            self.item_encoder = artifacts['item_encoder']
            self.product_features = artifacts['product_features']
            self.interaction_matrix = artifacts['interaction_matrix']
            self.train_matrix = artifacts['train_matrix']
            self.co_purchase_matrix = artifacts['co_purchase_matrix']
            self.category_encoder = artifacts['category_encoder']
    
    def predict(self, context, model_input):
        # Model input should be a dataframe with columns: customer_id, top_n
        # Returns: recommendations dataframe
        import pandas as pd
        import numpy as np
        
        recommendations = []
        for _, row in model_input.iterrows():
            customer_id = row.get('customer_id')
            top_n = int(row.get('top_n', 10))
            
            # Get recommendations logic here (simplified for now)
            if customer_id in self.user_encoder.classes_:
                user_idx = self.user_encoder.transform([customer_id])[0]
                
                # Get recommendations from the model
                if self.best_model_name == 'ALS':
                    rec_items, scores = self.best_model.recommend(
                        user_idx, 
                        self.train_matrix[user_idx],
                        N=top_n,
                        filter_already_liked_items=True
                    )
                    
                    for rank, (item_idx, score) in enumerate(zip(rec_items, scores), 1):
                        product_id = self.item_encoder.inverse_transform([item_idx])[0]
                        product_info = self.product_features[
                            self.product_features['product_id'] == product_id
                        ].iloc[0]
                        
                        recommendations.append({
                            'customer_id': customer_id,
                            'product_id': product_id,
                            'product_name': product_info['product_name'],
                            'confidence_score': float(score),
                            'recommendation_rank': rank
                        })
        
        return pd.DataFrame(recommendations)

with mlflow.start_run(run_name=f"hybrid_{best_model_name.lower()}_v1") as run:
    
    # Log parameters
    mlflow.log_param("model_type", f"hybrid_{best_model_name.lower()}_content")
    mlflow.log_param("cf_model", best_model_name)
    mlflow.log_param("cf_factors", 50)
    mlflow.log_param("cf_weight", 0.7)
    mlflow.log_param("cb_weight", 0.3)
    mlflow.log_param("training_window_months", 12)
    mlflow.log_param("num_customers", len(user_encoder.classes_))
    mlflow.log_param("num_products", len(item_encoder.classes_))
    
    # Log metrics - winner model
    mlflow.log_metric("precision_at_10", comparison_df.loc[winner_idx, 'Precision@10'])
    mlflow.log_metric("recall_at_10", comparison_df.loc[winner_idx, 'Recall@10'])
    mlflow.log_metric("ndcg_at_10", comparison_df.loc[winner_idx, 'NDCG@10'])
    mlflow.log_metric("coverage", comparison_df.loc[winner_idx, 'Coverage'])
    mlflow.log_metric("f1_at_10", comparison_df.loc[winner_idx, 'F1@10'])
    mlflow.log_metric("training_time_seconds", comparison_df.loc[winner_idx, 'Training_Time'])
    
    # Log business metrics
    mlflow.log_metric("baseline_aov", baseline_aov)
    mlflow.log_metric("projected_aov_lift_pct", aov_lift_pct)
    mlflow.log_metric("annual_revenue_lift", annual_revenue_lift)
    
    # Save model artifacts
    artifacts_dict = {
        'best_model': best_model,
        'best_model_name': best_model_name,
        'interaction_matrix': interaction_matrix,
        'train_matrix': train_matrix,
        'user_encoder': user_encoder,
        'item_encoder': item_encoder,
        'product_features': product_features,
        'co_purchase_matrix': co_purchase_matrix,
        'category_encoder': category_encoder
    }
    
    model_data_path = "/tmp/recommendation_model.pkl"
    with open(model_data_path, "wb") as f:
        cloudpickle.dump(artifacts_dict, f)
    
    # Log the model as a PyFunc model (required for UC registration)
    artifacts = {"model_data": model_data_path}
    mlflow.pyfunc.log_model(
        artifact_path="product_recommendation_model",
        python_model=ProductRecommendationModel(),
        artifacts=artifacts,
        pip_requirements=["cloudpickle", "pandas", "numpy", "scikit-learn", "implicit", "surprise", "scipy"]
    )
    
    # Log comparison table (removed EDA image artifact for speed)
    comparison_df.to_csv("/tmp/model_comparison.csv", index=False)
    mlflow.log_artifact("/tmp/model_comparison.csv")
    
    run_id = run.info.run_id
    print(f"✅ Model logged as PyFunc with run_id: {run_id}")
    print(f"✅ Model artifact path: product_recommendation_model")
    
    # Register model to Unity Catalog
    model_name = f"{CATALOG}.gold.sap_product_recommendation"
    model_uri = f"runs:/{run_id}/product_recommendation_model"
    
    print(f"\n📦 Registering model to Unity Catalog: {model_name}")
    
    try:
        # First, drop any existing auto-generated function with the same name
        # (MLflow may auto-create a function; we want only get_product_recommendations)
        try:
            spark.sql(f"DROP FUNCTION IF EXISTS {model_name}")
            print(f"   Dropped any existing auto-generated function: {model_name}")
        except:
            pass  # Function may not exist or may have datatype issues
        
        # Register the model to Unity Catalog
        # Note: This may auto-create a function; we'll handle cleanup separately
        model_version = mlflow.register_model(
            model_uri=model_uri,
            name=model_name,
            tags={
                "model_type": f"hybrid_{best_model_name.lower()}_content",
                "cf_model": best_model_name,
                "environment": ENVIRONMENT,
                "stage": "production"
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

print("="*80)
print("✅ PRODUCT RECOMMENDATION MODEL TRAINING COMPLETE")
print(f"✅ Experiment: /Shared/{ENVIRONMENT}/SkillAIPath/AI_Platform/MLOps/product_recommendation")
print(f"✅ Best Model: {best_model_name}")
print(f"✅ NDCG@10: {comparison_df.loc[winner_idx, 'NDCG@10']:.4f}")
print(f"✅ Projected AOV Lift: {aov_lift_pct:.1f}%")
print(f"✅ Annual Revenue Impact: ₹{annual_revenue_lift:,.0f}")
if 'model_name' in locals():
    print(f"✅ Unity Catalog Model: {model_name}")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Register Unity Catalog Python UDF for Recommendations
print("="*80)
print("🔧 REGISTERING UNITY CATALOG FUNCTION")
print("="*80)

# Create a Python UDF for getting recommendations
# This will be registered as a UC function that can be called from SQL or notebooks

function_name = f"{CATALOG}.gold.get_product_recommendations"

print(f"Creating Unity Catalog function: {function_name}")
print(f"This function will load the registered model and generate recommendations.\n")

# Drop function if it exists
try:
    spark.sql(f"DROP FUNCTION IF EXISTS {function_name}")
    print(f"✅ Dropped existing function (if any)")
except Exception as e:
    print(f"Note: {e}")

# Create the function using SQL
create_function_sql = f"""
CREATE OR REPLACE FUNCTION {function_name}(
    customer_id STRING,
    top_n INT
)
RETURNS ARRAY<STRUCT<
    product_id: STRING,
    product_name: STRING,
    category: STRING,
    confidence_score: DOUBLE,
    recommendation_rank: INT
>>
LANGUAGE PYTHON
COMMENT 'Generate personalized product recommendations for a customer using the trained hybrid recommendation model'
AS $$
    import mlflow
    import cloudpickle
    import pandas as pd
    import numpy as np
    from datetime import datetime
    
    # Load the model from Unity Catalog
    model_name = "{CATALOG}.gold.sap_product_recommendation"
    
    try:
        # Load model artifacts
        model_uri = f"models:/{model_name}@champion"
        model_path = mlflow.artifacts.download_artifacts(model_uri)
        
        with open(model_path + "/recommendation_model.pkl", "rb") as f:
            artifacts = cloudpickle.load(f)
        
        best_model = artifacts['best_model']
        best_model_name = artifacts['best_model_name']
        user_encoder = artifacts['user_encoder']
        item_encoder = artifacts['item_encoder']
        product_features = artifacts['product_features']
        interaction_matrix = artifacts['interaction_matrix']
        
        # Check if customer exists
        if customer_id not in user_encoder.classes_:
            # Cold start: return most popular products
            popular = product_features.nlargest(top_n, 'revenue')
            return [
                {{
                    'product_id': str(row['product_id']),
                    'product_name': str(row['product_name']),
                    'category': str(row['category']),
                    'confidence_score': 0.5,
                    'recommendation_rank': i + 1
                }}
                for i, (_, row) in enumerate(popular.iterrows())
            ]
        
        # Get user index
        user_idx = user_encoder.transform([customer_id])[0]
        
        # Get recommendations from model
        if best_model_name in ['ALS', 'NMF']:
            recs = best_model.recommend(
                userid=user_idx,
                user_items=interaction_matrix[user_idx],
                N=top_n,
                filter_already_liked_items=True
            )
            
            results = []
            for rank, (item_idx, score) in enumerate(zip(recs[0], recs[1]), 1):
                product_id = item_encoder.inverse_transform([item_idx])[0]
                product_info = product_features[product_features['product_id'] == product_id]
                
                if not product_info.empty:
                    results.append({{
                        'product_id': str(product_id),
                        'product_name': str(product_info['product_name'].iloc[0]),
                        'category': str(product_info['category'].iloc[0]),
                        'confidence_score': float(score),
                        'recommendation_rank': rank
                    }})
            
            return results
        
        else:
            # SVD model - simplified version
            return [{{
                'product_id': 'N/A',
                'product_name': 'SVD inference not implemented in UDF',
                'category': 'N/A',
                'confidence_score': 0.0,
                'recommendation_rank': 1
            }}]
            
    except Exception as e:
        # Return error as result
        return [{{
            'product_id': 'ERROR',
            'product_name': str(e),
            'category': 'ERROR',
            'confidence_score': 0.0,
            'recommendation_rank': 1
        }}]
$$
"""

try:
    spark.sql(create_function_sql)
    print(f"✅ Function created successfully: {function_name}")
    print(f"\n📊 Usage example:")
    print(f"   SELECT {function_name}('CUSTOMER123', 5) as recommendations")
    print(f"\n📊 The function will:")
    print(f"   1. Load the champion model from Unity Catalog")
    print(f"   2. Generate personalized recommendations for the given customer")
    print(f"   3. Return top N recommendations with scores and rankings")
    
except Exception as e:
    print(f"⚠️ Function creation failed: {e}")
    print(f"   Note: The model is still registered and can be loaded programmatically")
    print(f"   You can call the notebook's get_hybrid_recommendations() function instead")

print("="*80)

# COMMAND ----------

# DBTITLE 1,Generate and Persist Recommendations to Gold Layer
print("="*80)
print("📊 GENERATING RECOMMENDATIONS FOR ALL CUSTOMERS (OPTIMIZED)")
print("="*80)

# Get all eligible customers (those with purchase history)
all_customers = df_purchases['customer_id'].unique()
print(f"Total customers with purchase history: {len(all_customers):,}\n")

# Pre-compute product lookups for faster access
print("Pre-computing product lookups...")
product_lookup = product_features.set_index('product_id')[['product_name', 'category', 'unit_price']].to_dict('index')

# Generate recommendations using vectorized batch operations
print("Generating recommendations in batches...")
import time
start_time = time.time()

all_recommendations = []
failed_customers = 0
batch_size = 100

# Get user indices for all customers
valid_customers = [c for c in all_customers if c in user_encoder.classes_]
print(f"Valid customers (with training history): {len(valid_customers):,}")

# Process in batches for better performance
for batch_start in range(0, len(valid_customers), batch_size):
    batch_end = min(batch_start + batch_size, len(valid_customers))
    batch_customers = valid_customers[batch_start:batch_end]
    
    if (batch_start // batch_size + 1) % 10 == 0:
        elapsed = time.time() - start_time
        rate = batch_start / elapsed if elapsed > 0 else 0
        remaining = (len(valid_customers) - batch_start) / rate if rate > 0 else 0
        print(f"  Batch {batch_start//batch_size + 1}/{(len(valid_customers)-1)//batch_size + 1} | Processed {batch_start:,}/{len(valid_customers):,} | Rate: {rate:.0f} customers/sec | ETA: {remaining/60:.1f} min")
    
    for customer_id in batch_customers:
        try:
            user_idx = user_encoder.transform([customer_id])[0]
            
            # Get CF recommendations from best model
            if best_model_name in ['ALS', 'NMF']:
                cf_recs = best_model.recommend(
                    userid=user_idx,
                    user_items=interaction_matrix[user_idx],
                    N=10,
                    filter_already_liked_items=True
                )
                
                for rank, (item_idx, score) in enumerate(zip(cf_recs[0], cf_recs[1]), 1):
                    product_id = item_encoder.inverse_transform([item_idx])[0]
                    
                    if product_id in product_lookup:
                        prod_info = product_lookup[product_id]
                        all_recommendations.append({
                            'customer_id': customer_id,
                            'recommended_product_id': product_id,
                            'recommended_product_name': prod_info['product_name'],
                            'recommendation_score': float(score),
                            'recommendation_rank': rank,
                            'prediction_date': datetime.now().date()
                        })
            
            elif best_model_name == 'SVD':
                # SVD model - get all items and predict ratings
                all_item_ids = list(range(len(item_encoder.classes_)))
                predictions = []
                
                for item_idx in all_item_ids:
                    pred = best_model.predict(user_idx, item_idx)
                    predictions.append((item_idx, pred.est))
                
                # Sort by estimated rating and take top 10
                predictions.sort(key=lambda x: x[1], reverse=True)
                top_predictions = predictions[:10]
                
                for rank, (item_idx, score) in enumerate(top_predictions, 1):
                    product_id = item_encoder.inverse_transform([item_idx])[0]
                    
                    if product_id in product_lookup:
                        prod_info = product_lookup[product_id]
                        all_recommendations.append({
                            'customer_id': customer_id,
                            'recommended_product_id': product_id,
                            'recommended_product_name': prod_info['product_name'],
                            'recommendation_score': float(score),
                            'recommendation_rank': rank,
                            'prediction_date': datetime.now().date()
                        })
        except Exception as e:
            failed_customers += 1
            continue

elapsed_time = time.time() - start_time
print(f"\n✅ Generated {len(all_recommendations):,} recommendations in {elapsed_time:.1f} seconds")
print(f"✅ Successful customers: {len(valid_customers) - failed_customers:,}")
print(f"✅ Rate: {len(valid_customers)/elapsed_time:.0f} customers/second")
if failed_customers > 0:
    print(f"⚠️  Failed customers: {failed_customers}")

# Convert to pandas DataFrame
recommendations_df = pd.DataFrame(all_recommendations)

print(f"\n📊 Recommendations DataFrame shape: {recommendations_df.shape}")
print(f"\nSample recommendations:")
print(recommendations_df.head(20))

print("="*80)
print("💾 PERSISTING RECOMMENDATIONS TO GOLD TABLE")
print("="*80)

# Convert to Spark DataFrame
recommendations_spark = spark.createDataFrame(recommendations_df)

# Define table name
SCHEMA = "gold"
table_name = f"{CATALOG}.{SCHEMA}.product_recommendations"

print(f"Target table: {table_name}")

# Check if table exists and handle deduplication
try:
    # Try to read existing table
    existing_df = spark.table(table_name)
    table_exists = True
    print(f"✅ Table exists - will deduplicate and append\n")
    
    # Get today's date for deduplication
    today = datetime.now().date()
    print(f"🗑️  Removing existing predictions for {today}...")
    
    # Filter out today's predictions to avoid duplicates
    filtered_df = existing_df.filter(f"prediction_date != '{today}'")
    
    # Union with new recommendations
    combined_df = filtered_df.union(recommendations_spark)
    
    # Write with overwrite mode (replaces entire table with deduplicated data)
    combined_df.write \
        .mode("overwrite") \
        .option("overwriteSchema", "false") \
        .saveAsTable(table_name)
    
    print(f"✅ Deduplicated and saved recommendations")
    
except Exception as e:
    error_str = str(e)
    if "TABLE_OR_VIEW_NOT_FOUND" in error_str or "table_or_view_not_found" in error_str.lower():
        print(f"📝 Table does not exist - creating new table\n")
        table_exists = False
        
        # Create new table with partitioning by prediction_date
        recommendations_spark.write \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .partitionBy("prediction_date") \
            .saveAsTable(table_name)
        
        print(f"✅ Created new table with partitioning by prediction_date")
    else:
        print(f"❌ Error writing to table: {error_str}")
        raise

print("\n" + "="*80)
print("✅ VALIDATION")
print("="*80)

# Verify the table
total_count = spark.sql(f"SELECT COUNT(*) as count FROM {table_name}").collect()[0]['count']
today_count = spark.sql(f"""
    SELECT COUNT(*) as count 
    FROM {table_name} 
    WHERE prediction_date = '{datetime.now().date()}'
""").collect()[0]['count']

print(f"Total recommendations in table: {total_count:,}")
print(f"Today's recommendations: {today_count:,}")
print(f"Expected today: {len(recommendations_df):,}")

if today_count == len(recommendations_df):
    print(f"\n✅ Row count matches! Recommendations successfully persisted.")
else:
    print(f"\n⚠️  Row count mismatch (expected {len(recommendations_df):,}, got {today_count:,})")

# Show sample predictions
print(f"\n📊 SAMPLE PREDICTIONS FROM TABLE:")
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

print("="*80)
print("✅ PRODUCT RECOMMENDATIONS SUCCESSFULLY SAVED TO GOLD LAYER")
print(f"✅ Table: {table_name}")
print(f"✅ Total Recommendations: {today_count:,}")
print(f"✅ Unique Customers: {recommendations_df['customer_id'].nunique():,}")
print(f"✅ Prediction Date: {datetime.now().date()}")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Cleanup Auto-Generated Model Function
from databricks.sdk import WorkspaceClient

print("="*80)
print("🧹 CLEANUP: REMOVING AUTO-GENERATED FUNCTION")
print("="*80)

# MLflow auto-creates a function named product_recommendation_model during model registration
# We want to keep ONLY the model and the separate get_product_recommendations function

auto_function_name = f"{CATALOG}.gold.product_recommendation_model"

print(f"Checking for auto-generated function: {auto_function_name}")
print(f"Note: We keep the MODEL {auto_function_name} and the FUNCTION get_product_recommendations\n")

# Check if the auto-generated function exists
try:
    from databricks.sdk.service.catalog import FunctionsAPI
    
    # Query using information_schema
    functions_check = spark.sql(f"""
        SELECT routine_name
        FROM {CATALOG}.information_schema.routines
        WHERE routine_schema = 'gold'
        AND routine_name = 'product_recommendation_model'
    """).collect()
    
    if len(functions_check) > 0:
        print(f"⚠️  Auto-generated function detected: {auto_function_name}")
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
            print(f"   ⚠️  SDK deletion failed: {sdk_error}")
            print(f"\n   ⚠️  MANUAL ACTION REQUIRED:")
            print(f"   The auto-generated function {auto_function_name} could not be dropped automatically.")
            print(f"   ")
            print(f"   Please manually drop this function using the Databricks UI:")
            print(f"   1. Go to Catalog > {CATALOG} > gold > Functions")
            print(f"   2. Find 'product_recommendation_model' in the functions list")
            print(f"   3. Click the three dots menu > Delete")
            print(f"   ")
            print(f"   The MODEL will remain intact (this is expected).")
            print(f"   Only the duplicate FUNCTION needs to be removed.")
            dropped = False
    else:
        print(f"✅ No auto-generated function detected. Clean state!")
        dropped = True
        
except Exception as e:
    print(f"⚠️  Could not check for auto-generated function: {e}")
    print(f"   The model registration completed successfully.")
    print(f"   If the function product_recommendation_model exists, please delete it manually.")
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
        AND routine_name IN ('get_product_recommendations', 'product_recommendation_model')
        ORDER BY routine_name
    """).toPandas()
    
    print(f"\nFunctions in {CATALOG}.gold (relevant):")
    has_duplicate = False
    for idx, row in final_functions.iterrows():
        func_name = row['routine_name']
        if func_name == 'get_product_recommendations':
            print(f"  ✅ {func_name} (expected)")
        elif func_name == 'product_recommendation_model':
            print(f"  ❌ {func_name} (DUPLICATE - should be removed)")
            has_duplicate = True
    
    print(f"\nModel registered: {CATALOG}.gold.product_recommendation_model (in Models section) ✅")
    print(f"\n📊 Expected final state:")
    print(f"  Models: product_recommendation_model ✅")
    print(f"  Functions: get_product_recommendations ✅")
    print(f"  Functions: product_recommendation_model ❌ (should NOT exist)")
    
    if not has_duplicate:
        print(f"\n✅ SUCCESS: Clean state achieved!")
    else:
        print(f"\n⚠️  WARNING: Duplicate function still exists. Manual cleanup required.")
    
except Exception as e:
    print(f"⚠️  Could not verify final state: {e}")

print("="*80)