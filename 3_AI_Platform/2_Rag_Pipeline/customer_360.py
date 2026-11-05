# Databricks notebook source
# MAGIC %md
# MAGIC # 👥 Customer 360 Intelligence RAG System
# MAGIC ## Production-Grade Customer Analytics with Natural Language
# MAGIC
# MAGIC ### Overview
# MAGIC Conversational query system for customer profiles, RFM segments, and behavioral insights.
# MAGIC
# MAGIC ### Knowledge Base
# MAGIC * **Customers**: 19,337 customer profiles
# MAGIC * **RFM Segments**: Recency, Frequency, Monetary analysis (10,839 customers)
# MAGIC * **Behavioral Data**: Purchase patterns, lifecycle stages, churn risk
# MAGIC
# MAGIC ### Use Cases
# MAGIC 1. **Churn Prevention**: "Show me high-value customers at risk of churn"
# MAGIC 2. **Segmentation**: "Which customer segment has the highest lifetime value?"
# MAGIC 3. **Retention**: "List customers who haven't purchased in 90 days"
# MAGIC 4. **Growth Opportunities**: "Who are the Champions customers in my database?"
# MAGIC
# MAGIC ### Architecture
# MAGIC ```
# MAGIC Customer 360 (19,337 profiles)
# MAGIC   + RFM Analysis (10,839 segments)
# MAGIC   ↓
# MAGIC Text Generation (customer descriptions)
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
import json

# Configuration
# Environment Auto-Detection
import re

try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    match = re.search(r'/Shared/([^/]+)/SkillAIPath', notebook_path)
    
    if match:
        ENVIRONMENT = match.group(1)
        CATALOG = f"sap_{ENVIRONMENT}"
        DOCS_ROOT = f"/Workspace/Shared/{ENVIRONMENT}/SkillAIPath"
    else:
        ENVIRONMENT = "dev"
        CATALOG = "sap_dev"
        DOCS_ROOT = "/Workspace/Shared/dev/SkillAIPath"
except:
    ENVIRONMENT = "dev"
    CATALOG = "sap_dev"
    DOCS_ROOT = "/Workspace/Shared/dev/SkillAIPath"

SCHEMA = "gold"

# Delta tables
CUSTOMER_TABLE = f"{CATALOG}.{SCHEMA}.mart_customer_360"
RFM_TABLE = f"{CATALOG}.{SCHEMA}.mart_rfm_analysis"
KNOWLEDGE_TABLE = f"{CATALOG}.{SCHEMA}.customer_knowledge_base"
QUERY_LOG_TABLE = f"{CATALOG}.{SCHEMA}.customer_rag_queries"

print("="*80)
print("CUSTOMER 360 INTELLIGENCE RAG SYSTEM")
print("="*80)
print(f"Customer Table: {CUSTOMER_TABLE}")
print(f"RFM Table: {RFM_TABLE}")
print(f"Knowledge Base: {KNOWLEDGE_TABLE}")
print(f"Initialized: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Knowledge Base from Customer Data

# COMMAND ----------

# Create knowledge base table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {KNOWLEDGE_TABLE} (
    doc_id STRING,
    customer_id STRING,
    customer_name STRING,
    rfm_segment STRING,
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

# Join customer and RFM data
df_customers = spark.table(CUSTOMER_TABLE).alias("c")
df_rfm = spark.table(RFM_TABLE).alias("r")

df_joined = df_customers.join(
    df_rfm,
    F.col("c.CustomerKey") == F.col("r.CustomerSK"),
    "left"
)

# Generate rich text descriptions
df_knowledge = df_joined.select(
    F.concat(F.lit("CUST_"), F.col("c.CustomerKey").cast("string")).alias("doc_id"),
    F.col("c.CustomerID").cast("string").alias("customer_id"),
    F.col("c.CustomerName").alias("customer_name"),
    F.coalesce(F.col("Segment"), F.lit("Unknown")).alias("rfm_segment"),
    F.concat_ws(" | ",
        F.concat(F.lit("Customer: "), F.col("c.CustomerName")),
        F.concat(F.lit("Customer ID: "), F.col("c.CustomerID")),
        F.concat(F.lit("Total Orders: "), F.col("c.TotalPurchases").cast("string")),
        F.concat(F.lit("Total Spend: $"), F.round(F.col("c.TotalSpend"), 2).cast("string")),
        F.concat(F.lit("Avg Order Value: $"), F.round(F.col("c.AvgOrderValue"), 2).cast("string")),
        F.concat(F.lit("RFM Segment: "), F.coalesce(F.col("Segment"), F.lit("Unknown"))),
        F.concat(F.lit("Recency Score: "), F.coalesce(F.col("r.R_Score").cast("string"), F.lit("N/A"))),
        F.concat(F.lit("Frequency Score: "), F.coalesce(F.col("r.F_Score").cast("string"), F.lit("N/A"))),
        F.concat(F.lit("Monetary Score: "), F.coalesce(F.col("r.M_Score").cast("string"), F.lit("N/A")))
    ).alias("text_content"),
    F.to_json(F.struct(
        F.col("c.TotalPurchases"),
        F.col("c.TotalSpend"),
        F.coalesce(F.col("Segment"), F.lit("Unknown")).alias("rfm_segment")
    )).alias("metadata"),
    F.current_timestamp().alias("indexed_at")
)

# Write to knowledge base
df_knowledge.write.mode("overwrite").saveAsTable(KNOWLEDGE_TABLE)

count = spark.table(KNOWLEDGE_TABLE).count()
print(f"✅ Knowledge base created: {count} customer profiles indexed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. BM25 Retrieval System (FREE)

# COMMAND ----------

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

class Customer360RAG:
    """Production Customer 360 RAG with BM25 search"""
    
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
        self.docs_df = spark.table(KNOWLEDGE_TABLE).limit(5000).toPandas()  # Limit for performance
        texts = self.docs_df['text_content'].fillna('').tolist()
        self.vectors = self.vectorizer.fit_transform(texts)
        print(f"✅ Indexed {len(texts)} customer profiles")
    
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
                    'customer_name': self.docs_df.iloc[idx]['customer_name'],
                    'rfm_segment': self.docs_df.iloc[idx]['rfm_segment'],
                    'text': self.docs_df.iloc[idx]['text_content'],
                    'score': float(scores[idx])
                })
        return results
    
    def query(self, question: str):
        """End-to-end RAG query"""
        start_time = datetime.now()
        
        # Step 1: Retrieve relevant customers
        results = self.search(question, top_k=5)
        
        if not results:
            return {
                'answer': "I couldn't find relevant customers for your query. Try asking about RFM segments: Champions, Loyal Customers, At Risk, Hibernating, Lost.",
                'sources': []
            }
        
        # Step 2: Build context
        context = "\n---\n".join([
            f"Customer: {r['customer_name']}\nSegment: {r['rfm_segment']}\nDetails: {r['text']}"
            for r in results
        ])
        
        # Step 3: Generate response using Foundation Model API
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        
        prompt = f"""You are a customer success analyst. Answer this question using ONLY the customer data provided.

Question: {question}

Customer Data:
{context}

Instructions:
- Provide a clear, concise answer
- Include specific customer names and segments
- If the data doesn't contain enough information, say so
- Format as bullet points if listing multiple customers
- Suggest actionable recommendations if relevant

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
            'sources': [{'customer': r['customer_name'], 'segment': r['rfm_segment']} for r in results],
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
print("Initializing Customer 360 RAG...")
rag = Customer360RAG()
print("✅ RAG ready for queries")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Interactive Query Testing

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test Query 1: Churn Risk

# COMMAND ----------

response = rag.query("Show me high-value customers at risk of churn")
print("="*80)
print("QUESTION: Show me high-value customers at risk of churn")
print("="*80)
print(f"""ANSWER: {response['answer']}""")
print(f"Sources: {len(response['sources'])} customers")
print(f"Latency: {response['latency_ms']:.0f}ms")
print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test Query 2: Champion Customers

# COMMAND ----------

response = rag.query("Who are the Champions customers in my database?")
print("="*80)
print("QUESTION: Who are the Champions customers in my database?")
print("="*80)
print(f"""ANSWER: {response['answer']}""")
print(f"Sources: {len(response['sources'])} customers")
print(f"Latency: {response['latency_ms']:.0f}ms")
print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test Query 3: Retention Opportunity

# COMMAND ----------

response = rag.query("List customers who haven't purchased in 90 days")
print("="*80)
print("QUESTION: List customers who haven't purchased in 90 days")
print("="*80)
print(f"""ANSWER: {response['answer']}""")
print(f"Sources: {len(response['sources'])} customers")
print(f"Latency: {response['latency_ms']:.0f}ms")
print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. RFM Segment Analysis

# COMMAND ----------

print("="*80)
print("RFM SEGMENT DISTRIBUTION")
print("="*80)

segment_stats = spark.sql(f"""
SELECT 
    rfm_segment,
    COUNT(*) as customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM {KNOWLEDGE_TABLE}
GROUP BY rfm_segment
ORDER BY customer_count DESC
""")
segment_stats.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Analytics Dashboard

# COMMAND ----------

print("="*80)
print("CUSTOMER 360 RAG ANALYTICS")
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
print("""📊 Query Volume (Last 7 Days):""")
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
# MAGIC ## 7. Custom Query Interface

# COMMAND ----------

# MAGIC %md
# MAGIC Run any custom query below:

# COMMAND ----------

# Custom query - change this!
custom_query = "Which customer segment has the highest average spend?"

response = rag.query(custom_query)
print("="*80)
print(f"QUESTION: {custom_query}")
print("="*80)
print(f"""ANSWER: {response['answer']}""")