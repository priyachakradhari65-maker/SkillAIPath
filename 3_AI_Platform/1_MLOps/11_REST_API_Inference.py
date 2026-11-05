# Databricks notebook source
# ML Model Inference via REST API - Production Grade
# FREE EDITION COMPATIBLE
# PURPOSE: Call serving endpoints with retry logic, batch inference, and monitoring
# Updated: June 2026 - Latest inference patterns

import requests
import json
import time
from datetime import datetime

print("=" * 80)
print("ML MODEL INFERENCE CLIENT")
print("=" * 80)

# Configuration - Parameterized for plug-and-play
class InferenceConfig:
    # Endpoint Configuration
    WORKSPACE_URL = "https://your-workspace.cloud.databricks.com"
    ENDPOINT_NAME = "sap-churn-realtime"

    # Authentication
    # Use Databricks secrets in production:
    # TOKEN = dbutils.secrets.get(scope="ml-serving", key="endpoint-token")
    TOKEN = "<YOUR_DATABRICKS_TOKEN>"  # Replace with actual token

    # Request Configuration
    TIMEOUT_SECONDS = 30
    MAX_RETRIES = 3
    BACKOFF_FACTOR = 2
    BATCH_SIZE = 100  # Max records per request

    # Monitoring
    LOG_REQUESTS = True
    LOG_TABLE = "sap_dev.gold.inference_client_logs"

# Build inference URL
inference_url = f"{InferenceConfig.WORKSPACE_URL}/serving-endpoints/{InferenceConfig.ENDPOINT_NAME}/invocations"

print(f"Endpoint: {InferenceConfig.ENDPOINT_NAME}")
print(f"URL: {inference_url}")
print(f"Timeout: {InferenceConfig.TIMEOUT_SECONDS}s")
print(f"Max Retries: {InferenceConfig.MAX_RETRIES}")

# Create HTTP session with retry logic
def create_session_with_retries():
    """Create requests session with exponential backoff retry strategy"""
    session = requests.Session()

    retry_strategy = Retry(
        total=InferenceConfig.MAX_RETRIES,
        backoff_factor=InferenceConfig.BACKOFF_FACTOR,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["POST"]
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session

session = create_session_with_retries()

# Single Record Inference
def predict_single(features: Dict[str, Any]) -> Dict[str, Any]:
    """Send single inference request to endpoint"""
    headers = {
        "Authorization": f"Bearer {InferenceConfig.TOKEN}",
        "Content-Type": "application/json"
    }

    payload = {
        "dataframe_records": [features]
    }

    try:
        start_time = time.time()
        response = session.post(
            inference_url,
            headers=headers,
            json=payload,
            timeout=InferenceConfig.TIMEOUT_SECONDS
        )
        latency_ms = (time.time() - start_time) * 1000

        response.raise_for_status()
        predictions = response.json()

        result = {
            "success": True,
            "predictions": predictions,
            "latency_ms": round(latency_ms, 2),
            "status_code": response.status_code
        }

        return result

    except requests.exceptions.Timeout:
        return {
            "success": False,
            "error": "Request timeout",
            "latency_ms": InferenceConfig.TIMEOUT_SECONDS * 1000
        }
    except requests.exceptions.HTTPError as e:
        return {
            "success": False,
            "error": f"HTTP {e.response.status_code}: {e.response.text}",
            "status_code": e.response.status_code
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

# Batch Inference
def predict_batch(features_list: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Send batch inference request (up to BATCH_SIZE records)"""
    if len(features_list) > InferenceConfig.BATCH_SIZE:
        raise ValueError(f"Batch size {len(features_list)} exceeds maximum {InferenceConfig.BATCH_SIZE}")

    headers = {
        "Authorization": f"Bearer {InferenceConfig.TOKEN}",
        "Content-Type": "application/json"
    }

    payload = {
        "dataframe_records": features_list
    }

    try:
        start_time = time.time()
        response = session.post(
            inference_url,
            headers=headers,
            json=payload,
            timeout=InferenceConfig.TIMEOUT_SECONDS
        )
        latency_ms = (time.time() - start_time) * 1000

        response.raise_for_status()
        predictions = response.json()

        return {
            "success": True,
            "predictions": predictions["predictions"],
            "num_records": len(features_list),
            "latency_ms": round(latency_ms, 2),
            "avg_latency_per_record_ms": round(latency_ms / len(features_list), 2)
        }

    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "num_records": len(features_list)
        }

# Example 1: Single Prediction
print("\n" + "=" * 80)
print("EXAMPLE 1: Single Record Inference")
print("=" * 80)

sample_customer = {
    "recency": 30,
    "frequency": 5,
    "monetary": 50000
}

print(f"Input features: {sample_customer}")
result = predict_single(sample_customer)

if result["success"]:
    print(f"\nPrediction: {result['predictions']}")
    print(f"Latency: {result['latency_ms']} ms")
    print(f"Status Code: {result['status_code']}")
else:
    print(f"\nERROR: {result['error']}")

# Example 2: Batch Prediction
print("\n" + "=" * 80)
print("EXAMPLE 2: Batch Inference (10 records)")
print("=" * 80)

batch_customers = [
    {"recency": 30, "frequency": 5, "monetary": 50000},
    {"recency": 90, "frequency": 1, "monetary": 5000},
    {"recency": 15, "frequency": 10, "monetary": 120000},
    {"recency": 60, "frequency": 3, "monetary": 25000},
    {"recency": 45, "frequency": 7, "monetary": 80000},
    {"recency": 120, "frequency": 1, "monetary": 3000},
    {"recency": 10, "frequency": 12, "monetary": 150000},
    {"recency": 75, "frequency": 2, "monetary": 15000},
    {"recency": 20, "frequency": 8, "monetary": 95000},
    {"recency": 100, "frequency": 1, "monetary": 8000},
]

print(f"Batch size: {len(batch_customers)} records")
batch_result = predict_batch(batch_customers)

if batch_result["success"]:
    print(f"\nPredictions: {batch_result['predictions']}")
    print(f"Total latency: {batch_result['latency_ms']} ms")
    print(f"Avg latency per record: {batch_result['avg_latency_per_record_ms']} ms")
    print(f"Throughput: {1000 / batch_result['avg_latency_per_record_ms']:.1f} records/second")
else:
    print(f"\nERROR: {batch_result['error']}")

# Example 3: Large-Scale Batch Processing with Chunking
print("\n" + "=" * 80)
print("EXAMPLE 3: Large-Scale Batch Processing (1000 records)")
print("=" * 80)

def process_large_batch(features_list: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Process large batches by chunking into smaller requests"""
    all_predictions = []
    total_latency = 0
    failed_chunks = 0

    num_chunks = (len(features_list) + InferenceConfig.BATCH_SIZE - 1) // InferenceConfig.BATCH_SIZE

    print(f"Processing {len(features_list)} records in {num_chunks} chunks of {InferenceConfig.BATCH_SIZE}...")

    for i in range(0, len(features_list), InferenceConfig.BATCH_SIZE):
        chunk = features_list[i:i + InferenceConfig.BATCH_SIZE]
        chunk_num = i // InferenceConfig.BATCH_SIZE + 1

        print(f"  Chunk {chunk_num}/{num_chunks} ({len(chunk)} records)...", end=" ")

        result = predict_batch(chunk)

        if result["success"]:
            all_predictions.extend(result["predictions"])
            total_latency += result["latency_ms"]
            print(f"OK ({result['latency_ms']:.0f}ms)")
        else:
            failed_chunks += 1
            print(f"FAILED - {result['error']}")

    return {
        "total_records": len(features_list),
        "successful_predictions": len(all_predictions),
        "failed_chunks": failed_chunks,
        "total_latency_ms": round(total_latency, 2),
        "avg_latency_per_record_ms": round(total_latency / len(all_predictions), 2) if all_predictions else 0,
        "predictions": all_predictions
    }

# Simulate 1000 customer records
large_batch = [
    {
        "recency": 30 + (i % 100),
        "frequency": 1 + (i % 15),
        "monetary": 5000 + (i * 100)
    }
    for i in range(1000)
]

large_result = process_large_batch(large_batch)

print(f"\nProcessing Summary:")
print(f"  Total records: {large_result['total_records']}")
print(f"  Successful: {large_result['successful_predictions']}")
print(f"  Failed chunks: {large_result['failed_chunks']}")
print(f"  Total latency: {large_result['total_latency_ms'] / 1000:.2f} seconds")
print(f"  Avg latency per record: {large_result['avg_latency_per_record_ms']:.2f} ms")
print(f"  Throughput: {1000 / large_result['avg_latency_per_record_ms']:.1f} records/second")

# Example 4: Error Handling and Monitoring
print("\n" + "=" * 80)
print("EXAMPLE 4: Error Handling Test")
print("=" * 80)

# Test with invalid data
invalid_data = {
    "invalid_field": 123,
    "another_invalid": "abc"
}

print("Testing with invalid schema...")
error_result = predict_single(invalid_data)

if not error_result["success"]:
    print(f"\nExpected error caught: {error_result['error']}")
    print("Error handling working correctly.")

# Production Patterns
print("\n" + "=" * 80)
print("PRODUCTION DEPLOYMENT PATTERNS")
print("=" * 80)
print("""
1. Authentication:
   - Store tokens in Databricks Secrets
   - Use service principal tokens for automated systems
   - Rotate tokens every 90 days

2. Error Handling:
   - Implement exponential backoff for retries
   - Log all failed requests for debugging
   - Set up alerts for error rate > 1%

3. Batch Processing:
   - Use batch size = 100 for optimal throughput
   - Chunk large datasets to stay under batch limits
   - Process batches in parallel for high-volume workloads

4. Monitoring:
   - Track latency p50, p95, p99
   - Monitor error rates by error type
   - Alert on latency spikes > 2x baseline
   - Log inference requests to Delta table for audit

5. Cost Optimization:
   - Use batch inference for non-real-time workloads
   - Cache predictions for frequently requested features
   - Use smaller batch sizes during low-traffic periods

6. Load Testing:
   - Test endpoint with 10x expected peak load
   - Measure latency under concurrent requests
   - Verify scale-to-zero startup time (cold start)

7. Fallback Strategy:
   - Implement circuit breaker pattern
   - Use cached predictions when endpoint unavailable
   - Have backup endpoint for critical applications
""")

print("\nInference examples complete.")