# Databricks notebook source
# Fallback Routing Strategy
# Automatically fallback to cheaper model on primary failure
# June 2026

print("FALLBACK ROUTING STRATEGY")

routes = [
    {"priority": 1, "model": "gpt-4o", "fallback_on": ["5xx errors", "timeout"]},
    {"priority": 2, "model": "gpt-4o-mini", "fallback_on": ["5xx errors", "rate limit"]},
    {"priority": 3, "model": "local-llama", "fallback_on": ["all errors"]},
]

print("\nFallback Chain:")
for route in routes:
    print(f"{route['priority']}. {route['model']} (fallback on: {', '.join(route['fallback_on'])})")

print("\nBenefits:")
print("- 99.9 percent availability")
print("- Graceful degradation")
print("- Cost optimization (use cheaper model as fallback)")