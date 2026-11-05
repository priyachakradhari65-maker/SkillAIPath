# Databricks notebook source
# Response Streaming
# Stream LLM responses token-by-token
# June 2026

print("RESPONSE STREAMING")
print("\nConfiguration:")
print("- Enable: stream=true in request")
print("- Client: Server-Sent Events (SSE)")
print("- Benefit: Lower perceived latency")
print("\nUse case: Chatbots, long-form content generation")
print("User sees response appear word-by-word (better UX)")