# Cost Monitoring for Federated Queries
# Track data transfer costs across clouds
# June 2026

print("COST MONITORING FOR FEDERATED QUERIES")

# Simulated cost data
query_costs = [
    ("Query 1: Cross-region S3 scan", 450_000_000, 0.023),  # bytes, cost_usd
    ("Query 2: Snowflake federation", 120_000_000, 0.008),
    ("Query 3: PostgreSQL join", 30_000_000, 0.001),
]

print("\nQuery Cost Analysis:")
for query, bytes_transferred, cost in query_costs:
    mb = bytes_transferred / 1024 / 1024
    print(f"{query}")
    print(f"  Data transfer: {mb:.1f} MB")
    print(f"  Cost: ${cost:.4f}")

print("\nCost optimization tips:")
print("1. Cache results of expensive queries (CACHE TABLE)")
print("2. Use partitioning in source system")
print("3. Minimize cross-region transfers")
print("4. Pre-aggregate data in source before federating")
