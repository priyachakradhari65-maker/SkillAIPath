# Databricks notebook source
# Prompt Template Management
# Centralized prompt templates with versioning
# June 2026

print("PROMPT TEMPLATE MANAGEMENT")

templates = {
    "customer_query_v1": "You are a helpful customer service agent. Answer: {question}",
    "customer_query_v2": "Context: {context}\n\nQuestion: {question}\n\nAnswer concisely.",
    "sales_analysis_v1": "Analyze sales data: {data}. Provide insights in bullet points."
}

print("\nAvailable Templates:")
for name, template in templates.items():
    print(f"- {name}: {template[:50]}...")

print("\nVersion control:")
print("- Store templates in Delta table")
print("- A/B test template versions")
print("- Rollback to previous version if needed")