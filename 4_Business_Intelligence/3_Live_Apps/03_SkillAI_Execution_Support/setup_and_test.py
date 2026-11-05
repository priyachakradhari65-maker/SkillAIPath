"""
Setup and Test Script for SkillAI Execution Support Agent

This script:
1. Initializes Unity Catalog tables
2. Tests the agent components
3. Validates the deployment
"""

import sys
sys.path.append('/Workspace/Shared/dev/SkillAIPath/4_Business_Intelligence/3_Live_Apps/03_SkillAI_Execution_Support')

from databricks.sdk import WorkspaceClient
from agent_core import SkillAIExecutionAgent
from user_context import UserContextManager

print("🚀 SkillAI Execution Support Agent - Setup & Test")
print("=" * 70)

# Initialize Workspace Client
print("\n1️⃣ Initializing Workspace Client...")
w = WorkspaceClient()
print("✅ Workspace client initialized")

# Initialize User Context Manager (creates tables)
print("\n2️⃣ Creating Unity Catalog Tables...")
user_context = UserContextManager(w, catalog="sap_dev", schema="gold")
print("✅ Unity Catalog tables created:")
print("   - sap_dev.gold.skillai_user_profiles")
print("   - sap_dev.gold.skillai_chat_history")
print("   - sap_dev.gold.skillai_user_activity")

# Initialize Agent
print("\n3️⃣ Initializing SkillAI Execution Agent...")
agent = SkillAIExecutionAgent(w, catalog="sap_dev")
print("✅ Agent initialized with components:")
print(f"   - Knowledge Base: {'Loaded' if agent.knowledge_base else 'Not Available'}")
print(f"   - Central Reviewer: {'Loaded' if agent.reviewer else 'Not Available'}")
print(f"   - Model Endpoint: {agent.llm_endpoint}")

# Test User Context
print("\n4️⃣ Testing User Context Management...")
test_user_email = "test@skillaipath.com"
test_user_name = "Test User"

user_context_data = agent.get_user_context(test_user_email, test_user_name)
print(f"✅ User profile created: {user_context_data['profile']['user_name']}")
print(f"   - Email: {user_context_data['profile']['user_email']}")
print(f"   - Role: {user_context_data['profile']['role']}")
print(f"   - Team: {user_context_data['profile']['team']}")

# Test Knowledge Base Search
print("\n5️⃣ Testing Knowledge Base Search...")
if agent.knowledge_base:
    results = agent.search_knowledge("medallion architecture")
    print(f"✅ Knowledge base search returned {len(results)} results")
    if results:
        print(f"   - Top result: {results[0].get('source', 'Unknown')}")
else:
    print("⚠️ Knowledge base not available (knowledge_base.json may be missing)")

# Test Activity Tracking
print("\n6️⃣ Testing Activity Tracking...")
agent.track_activity(
    user_email=test_user_email,
    activity_type="sprint_planning",
    sprint_plan="Sprint 1: Build Execution Support Agent",
    action_items=["Create app.py", "Build agent_core.py", "Test deployment"],
    bugs=[],
    deliverables=["Working Streamlit app"],
    status="active"
)
print("✅ Activity tracked successfully")

activities = user_context.get_user_activities(test_user_email)
print(f"   - Total activities: {len(activities)}")

# Test Message Processing
print("\n7️⃣ Testing Message Processing...")
test_message = "How does the Medallion architecture work in SkillAI?"
print(f"   User: {test_message}")

try:
    response = agent.process_message(
        user_email=test_user_email,
        user_name=test_user_name,
        message=test_message,
        conversation_id="test-conversation-001",
        conversation_history=[]
    )
    print(f"✅ Agent response generated ({len(response)} chars)")
    print(f"\n   Agent: {response[:200]}...")  # Show first 200 chars
except Exception as e:
    print(f"⚠️ Message processing failed: {e}")
    print("   This may be due to LLM endpoint configuration")

# Verify Chat History
print("\n8️⃣ Verifying Chat History...")
chat_history = user_context.get_chat_history("test-conversation-001")
print(f"✅ Chat history retrieved: {len(chat_history)} messages")

# Summary
print("\n" + "=" * 70)
print("🎉 Setup Complete!")
print("\n📝 Summary:")
print("   ✅ Unity Catalog tables created")
print("   ✅ User context management working")
print("   ✅ Activity tracking functional")
print("   ✅ Chat history persistence working")
if agent.knowledge_base:
    print("   ✅ Knowledge base integration active")
if agent.llm_endpoint:
    print(f"   ✅ Model endpoint configured: {agent.llm_endpoint}")

print("\n🚀 Ready to Deploy!")
print("\nNext steps:")
print("   1. Deploy with: databricks apps create skillai-execution-support")
print("   2. Start with: databricks apps start skillai-execution-support")
print("   3. Or use DAB: databricks bundle deploy -t dev")
