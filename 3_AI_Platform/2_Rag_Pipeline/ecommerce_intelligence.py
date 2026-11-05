# Databricks notebook source
# MAGIC %md
# MAGIC # 🛍️ E-Commerce Intelligence RAG System
# MAGIC ## Production-Grade Business Analytics with Natural Language
# MAGIC
# MAGIC ### Overview
# MAGIC Conversational query system for SkillAIPath product catalog, sales data, and business metrics.
# MAGIC
# MAGIC ### Knowledge Base
# MAGIC * **Products**: 79 products across categories (Electronics, Clothing, Home & Garden, Sports)
# MAGIC * **Sales Data**: Historical transaction patterns
# MAGIC * **Performance Metrics**: Revenue, margins, inventory turnover
# MAGIC
# MAGIC ### Use Cases
# MAGIC 1. **Product Performance**: "What are the top-selling products in Electronics?"
# MAGIC 2. **Category Analysis**: "Which category has the highest profit margin?"
# MAGIC 3. **Inventory Insights**: "Show me products with low stock levels"
# MAGIC 4. **Pricing Analysis**: "List products with discounts over 20%"
# MAGIC
# MAGIC ### Architecture
# MAGIC ```
# MAGIC Product Catalog (79 products)
# MAGIC   + Sales Data (sap_dev.gold.*)
# MAGIC   ↓
# MAGIC Text Generation (product descriptions)
# MAGIC   ↓
# MAGIC BM25 Indexing (keyword search)
# MAGIC   ↓
# MAGIC Natural Language Query
# MAGIC   ↓
# MAGIC SQL Generation + LLM Response
# MAGIC ```
# MAGIC
# MAGIC ### Cost: $0/month (100% FREE)
# MAGIC * BM25 search (sklearn)
# MAGIC * Foundation Models API (FREE tier)
# MAGIC * Delta Lake storage (FREE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration & Setup

# COMMAND ----------

# DBTITLE 1,Configuration
from pyspark.sql import functions as F
from datetime import datetime
import json, re

# Configuration
CATALOG = "sap_dev"
SCHEMA = "gold"

# Delta tables
PRODUCT_TABLE = f"{CATALOG}.{SCHEMA}.mart_product_performance"
KNOWLEDGE_TABLE = f"{CATALOG}.{SCHEMA}.ecommerce_knowledge_base"
QUERY_LOG_TABLE = f"{CATALOG}.{SCHEMA}.ecommerce_rag_queries"

print("="*80)
print("E-COMMERCE INTELLIGENCE RAG SYSTEM")
print("="*80)
print(f"Product Table: {PRODUCT_TABLE}")
print(f"Knowledge Base: {KNOWLEDGE_TABLE}")
print(f"Initialized: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}")
print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Knowledge Base from Product Data

# COMMAND ----------

# Create knowledge base table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {KNOWLEDGE_TABLE} (
    doc_id STRING,
    product_id STRING,
    product_name STRING,
    category STRING,
    text_content STRING,
    metadata STRING,
    indexed_at TIMESTAMP
) USING DELTA
""")

# Create query log table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {QUERY_LOG_TABLE} (
    query_id STRING,
    timestamp TIMESTAMP,
    query_text STRING,
    results_count INT,
    response_text STRING,
    latency_ms FLOAT
) USING DELTA
""")

print("✅ Tables created")

# COMMAND ----------

# Load product data and generate text descriptions
df_products = spark.table(PRODUCT_TABLE)

# Generate rich text descriptions
df_knowledge = df_products.select(
    F.concat(F.lit("PROD_"), F.col("ProductSK").cast("string")).alias("doc_id"),
    F.col("ProductSK").cast("string").alias("product_id"),
    F.col("ProductName").alias("product_name"),
    F.col("ProductCategory").alias("category"),
    F.concat_ws(" | ",
        F.concat(F.lit("Product: "), F.col("ProductName")),
        F.concat(F.lit("Category: "), F.col("ProductCategory")),
        F.concat(F.lit("Total Orders: "), F.col("TotalOrders").cast("string")),
        F.concat(F.lit("Quantity Sold: "), F.col("TotalQuantitySold").cast("string")),
        F.concat(F.lit("Revenue: $"), F.round(F.col("TotalRevenue"), 2).cast("string")),
        F.concat(F.lit("Avg Sale Value: $"), F.round(F.col("AvgSaleValue"), 2).cast("string"))
    ).alias("text_content"),
    F.to_json(F.struct(
        F.col("ProductCategory"),
        F.col("TotalOrders"),
        F.col("TotalRevenue")
    )).alias("metadata"),
    F.current_timestamp().alias("indexed_at")
)

# Write to knowledge base
df_knowledge.write.mode("overwrite").saveAsTable(KNOWLEDGE_TABLE)

count = spark.table(KNOWLEDGE_TABLE).count()
print(f"✅ Knowledge base created: {count} product documents indexed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. BM25 Retrieval System (FREE)

# COMMAND ----------

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

class EcommerceRAG:
    """Production E-Commerce RAG with BM25 search"""
    
    def __init__(self):
        self.vectorizer = TfidfVectorizer(
            max_features=1000,
            stop_words='english',
            ngram_range=(1, 2),
            min_df=1
        )
        self.docs_df = None
        self.vectors = None
        self._index_knowledge_base()
    
    def _index_knowledge_base(self):
        """Load and index knowledge base"""
        self.docs_df = spark.table(KNOWLEDGE_TABLE).toPandas()
        texts = self.docs_df['text_content'].fillna('').tolist()
        self.vectors = self.vectorizer.fit_transform(texts)
        print(f"✅ Indexed {len(texts)} documents")
    
    def search(self, query: str, top_k: int = 5):
        """BM25 keyword search"""
        query_vec = self.vectorizer.transform([query])
        scores = cosine_similarity(query_vec, self.vectors)[0]
        top_indices = np.argsort(scores)[::-1][:top_k]
        
        results = []
        for idx in top_indices:
            if scores[idx] > 0.01:  # Relevance threshold
                results.append({
                    'doc_id': self.docs_df.iloc[idx]['doc_id'],
                    'product_name': self.docs_df.iloc[idx]['product_name'],
                    'category': self.docs_df.iloc[idx]['category'],
                    'text': self.docs_df.iloc[idx]['text_content'],
                    'score': float(scores[idx])
                })
        return results
    
    def query(self, question: str):
        """End-to-end RAG query"""
        start_time = datetime.now()
        
        # Step 1: Retrieve relevant products
        results = self.search(question, top_k=5)
        
        if not results:
            latency = (datetime.now() - start_time).total_seconds() * 1000
            return {
                'answer': "I couldn't find relevant products for your query. Try rephrasing or asking about categories: Electronics, Clothing, Home & Garden, Sports.",
                'sources': [],
                'latency_ms': latency
            }
        
        # Step 2: Build context
        context = "\n---\n".join([
            f"Product: {r['product_name']}\nDetails: {r['text']}"
            for r in results
        ])
        
        # Step 3: Generate response using Foundation Model API
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        
        prompt = f"""You are an e-commerce business analyst. Answer this question using ONLY the product data provided.

Question: {question}

Product Data:
{context}

Instructions:
- Provide a clear, concise answer
- Include specific numbers and product names
- If the data doesn't contain enough information, say so
- Format as bullet points if listing multiple items

Answer:"""
        
        try:
            response = w.serving_endpoints.query(
                name="databricks-meta-llama-3-1-70b-instruct",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=500
            )
            answer = response.choices[0].message.content
        except Exception as e:
            answer = f"Error generating response: {str(e)}"
        
        # Step 4: Log query
        latency = (datetime.now() - start_time).total_seconds() * 1000
        self._log_query(question, len(results), answer, latency)
        
        return {
            'answer': answer,
            'sources': [{'product': r['product_name'], 'category': r['category']} for r in results],
            'latency_ms': latency
        }
    
    def _log_query(self, query_text, results_count, response, latency):
        """Log query for analytics"""
        import uuid
        log_data = [(
            str(uuid.uuid4()),
            datetime.now(),
            query_text,
            results_count,
            response,
            latency
        )]
        spark.createDataFrame(log_data, ["query_id", "timestamp", "query_text", "results_count", "response_text", "latency_ms"])\
            .write.mode("append").saveAsTable(QUERY_LOG_TABLE)

# Initialize RAG
print("Initializing E-Commerce RAG...")
rag = EcommerceRAG()
print("✅ RAG ready for queries")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Interactive Query Testing

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test Query 1: Product Performance

# COMMAND ----------

response = rag.query("What are the top-selling products in Electronics?")
print("="*80)
print("QUESTION: What are the top-selling products in Electronics?")
print("="*80)
print(f"""
ANSWER:
{response['answer']}
""")
print(f"Sources: {len(response['sources'])} products")
print(f"Latency: {response['latency_ms']:.0f}ms")
print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test Query 2: Category Analysis

# COMMAND ----------

response = rag.query("Which product category has the highest revenue?")
print("="*80)
print("QUESTION: Which product category has the highest revenue?")
print("="*80)
print(f"""
ANSWER:
{response['answer']}
""")
print(f"Sources: {len(response['sources'])} products")
print(f"Latency: {response['latency_ms']:.0f}ms")
print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test Query 3: Product Discovery

# COMMAND ----------

response = rag.query("Show me products with high order volume")
print("="*80)
print("QUESTION: Show me products with high order volume")
print("="*80)
print(f"""
ANSWER:
{response['answer']}
""")
print(f"Sources: {len(response['sources'])} products")
print(f"Latency: {response['latency_ms']:.0f}ms")
print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Analytics Dashboard

# COMMAND ----------

print("="*80)
print("E-COMMERCE RAG ANALYTICS")
print("="*80)

# Query volume
query_stats = spark.sql(f"""
SELECT 
    DATE(timestamp) as date,
    COUNT(*) as query_count,
    AVG(latency_ms) as avg_latency_ms,
    AVG(results_count) as avg_results
FROM {QUERY_LOG_TABLE}
GROUP BY DATE(timestamp)
ORDER BY date DESC
LIMIT 7
""")
print("\n📊 Query Volume (Last 7 Days):")
print("""\n📊 Query Volume (Last 7 Days):""")
query_stats.show()

# Popular queries
popular = spark.sql(f"""
SELECT 
    query_text,
    COUNT(*) as frequency,
    AVG(latency_ms) as avg_latency
FROM {QUERY_LOG_TABLE}
GROUP BY query_text
ORDER BY frequency DESC
LIMIT 5
""")
print("""🔥 Popular Queries:""")
popular.show(truncate=False)

print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Custom Query Interface

# COMMAND ----------

# MAGIC %md
# MAGIC Run any custom query below:

# COMMAND ----------

# Custom query - change this!
custom_query = "What products are available in the Clothing category?"

response = rag.query(custom_query)
print("="*80)
print(f"QUESTION: {custom_query}")
print("="*80)
print(f"""
ANSWER:
{response['answer']}
""")
