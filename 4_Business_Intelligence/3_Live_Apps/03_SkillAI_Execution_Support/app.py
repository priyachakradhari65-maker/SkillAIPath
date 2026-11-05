"""
SkillAI Execution Support Agent - Streamlit UI
Autonomous AI Assistant with Chat History and User Tracking
"""

import html
import streamlit as st
import uuid
from datetime import datetime
from databricks.sdk import WorkspaceClient
from agent_core import SkillAIExecutionAgent, ModelEndpointError

# Page configuration
st.set_page_config(
    page_title="SkillAI Execution Support Agent",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
.main-header {
    font-size: 2.5rem;
    font-weight: bold;
    color: #FF3621;
    text-align: center;
    padding: 1rem 0;
}
.sub-header {
    font-size: 1.2rem;
    color: #8A8F98;
    text-align: center;
    padding-bottom: 2rem;
}
.user-message {
    background-color: #E3F2FD;
    color: #0F1B2D;
    padding: 1rem;
    border-radius: 10px;
    margin: 0.5rem 0;
}
.assistant-message {
    background-color: #F5F5F5;
    color: #1F2937;
    padding: 1rem;
    border-radius: 10px;
    margin: 0.5rem 0;
}
.user-message strong, .assistant-message strong {
    color: inherit;
}
.stButton > button {
    background-color: #FF3621;
    color: white;
    border-radius: 5px;
    padding: 0.5rem 2rem;
}
</style>
""", unsafe_allow_html=True)

# Initialize Workspace Client
@st.cache_resource
def get_workspace_client():
    return WorkspaceClient()

# Initialize Agent
@st.cache_resource
def get_agent():
    w = get_workspace_client()
    return SkillAIExecutionAgent(w, catalog="sap_dev")

# Initialize session state
if "conversation_id" not in st.session_state:
    st.session_state.conversation_id = str(uuid.uuid4())

if "messages" not in st.session_state:
    st.session_state.messages = []

if "user_email" not in st.session_state:
    # Identity resolution order:
    # 1. Logged-in human forwarded by Databricks Apps (user authorization is
    #    enabled). w.current_user.me() inside an app returns the app's SERVICE
    #    PRINCIPAL, not the person browsing - never use it as the user identity
    #    when forwarded headers are available.
    # 2. App service principal fallback (labelled as such in the sidebar).
    fwd_email = st.context.headers.get("X-Forwarded-Email")
    fwd_name = st.context.headers.get("X-Forwarded-Preferred-Username")
    if fwd_email:
        st.session_state.user_email = fwd_email
        st.session_state.user_name = fwd_name or fwd_email.split("@")[0]
        st.session_state.identity_source = "Logged-in user (forwarded by Databricks Apps)"
    else:
        try:
            w = get_workspace_client()
            current_user = w.current_user.me()
            st.session_state.user_email = current_user.user_name or (
                current_user.emails[0].value if current_user.emails else None)
            st.session_state.user_name = current_user.display_name or (st.session_state.user_email or "Unknown").split("@")[0]
            st.session_state.identity_source = "App service principal (no forwarded user)"
        except Exception:
            st.session_state.user_email = None
            st.session_state.user_name = None
            st.session_state.identity_source = "Unavailable"

if "agent" not in st.session_state:
    st.session_state.agent = get_agent()

# Surface any error from the previous chat turn (survives the rerun below)
if st.session_state.get("last_error"):
    st.error(st.session_state.last_error)
    st.session_state.last_error = None

# Sidebar - User Context
with st.sidebar:
    st.markdown("### 👤 User Context")
    
    try:
        user_context = st.session_state.agent.get_user_context(
            st.session_state.user_email,
            st.session_state.user_name
        )
        
        if user_context.get("error"):
            st.warning(f"User context is temporarily unavailable: {user_context['error']}")
        
        profile = user_context.get("profile", {})
        
        # Fall back to the resolved session identity when the profile row
        # cannot be loaded - only show 'Unavailable' when identity is truly absent
        st.write(f"**Name:** {profile.get('user_name') or st.session_state.get('user_name') or 'Unavailable'}")
        st.write(f"**Email:** {profile.get('user_email') or st.session_state.get('user_email') or 'Unavailable'}")
        st.write(f"**Role:** {profile.get('role', 'Team Member')}")
        st.write(f"**Team:** {profile.get('team', 'Engineering')}")
        st.write(f"**Interactions:** {profile.get('total_interactions', 0)}")
        
        st.markdown("---")
        
        # Recent Activities
        st.markdown("### 📋 Recent Activities")
        activities = user_context.get("recent_activities", [])
        
        if activities:
            for activity in activities[:3]:
                st.write(f"**{activity.get('activity_type', 'Unknown')}**")
                st.caption(f"{activity.get('timestamp', 'N/A')}")
        else:
            st.info("No recent activities")
        
    except Exception as e:
        st.error(f"Failed to load user context: {e}")
    
    st.markdown("---")
    
    # Quick Actions
    st.markdown("### ⚡ Quick Actions")
    
    if st.button("🔄 New Conversation"):
        st.session_state.conversation_id = str(uuid.uuid4())
        st.session_state.messages = []
        st.rerun()
    
    if st.button("📊 View Sprint Plan"):
        st.info("Sprint plan tracking coming soon!")
    
    if st.button("🐛 Report Bug"):
        st.info("Bug tracking coming soon!")
    
    st.markdown("---")
    
    # System Info
    st.markdown("### ℹ️ System Info")
    st.caption(f"**Conversation ID:** {st.session_state.conversation_id[:8]}...")
    st.caption(f"**Identity:** {st.session_state.get('identity_source', 'Unknown')}")
    st.caption(f"**Catalog:** sap_dev")
    st.caption(f"**Knowledge Base:** 7,322 entries")

# Main UI
st.markdown('<div class="main-header">🤖 SkillAI Execution Support Agent</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">Your Autonomous Lead Engineer & Senior Architect</div>', unsafe_allow_html=True)

# Welcome message
if len(st.session_state.messages) == 0:
    welcome_message = f"""
👋 **Welcome, {st.session_state.user_name}!**

I'm your SkillAI Execution Support Agent - your autonomous Lead Engineer and Senior Architect. I'm here to:

✅ **Guide** you through technical challenges with senior-level expertise  
✅ **Remember** your sprint plans, action items, bugs, and deliverables  
✅ **Search** the 7,322-entry SkillAI codebase knowledge base  
✅ **Review** your code with context-aware feedback  
✅ **Track** your progress and help you stay on course  

I have access to:
- 🧠 **Knowledge Base**: SkillAIPath codebase, architecture patterns, business rules
- 📊 **Google Sheets**: Team tasks and portfolio tracking
- 💬 **Chat History**: Your conversation history is saved and remembered
- 👤 **User Context**: Your role, team, and activity tracking

What can I help you with today?
    """
    
    st.markdown(welcome_message)
    st.markdown("---")

# Display chat history
for message in st.session_state.messages:
    role = message["role"]
    content = message["content"]
    
    safe_content = html.escape(content).replace("\n", "<br>")
    if role == "user":
        st.markdown(f'<div class="user-message"><strong>You:</strong><br>{safe_content}</div>', unsafe_allow_html=True)
    else:
        st.markdown(f'<div class="assistant-message"><strong>🤖 SkillAI Agent:</strong><br>{safe_content}</div>', unsafe_allow_html=True)

# Chat input
user_input = st.chat_input("Ask me anything about SkillAI, get code reviews, track your sprint...")

if user_input:
    if not st.session_state.get("user_email"):
        # Identity is genuinely unavailable - be honest instead of attributing
        # the conversation to a placeholder
        st.session_state.last_error = "User identity is unavailable, so this message cannot be attributed. Check the app's authentication configuration."
        st.rerun()

    # Add user message to chat
    st.session_state.messages.append({"role": "user", "content": user_input})

    # Generate response
    with st.spinner("🤖 Thinking..."):
        try:
            response = st.session_state.agent.process_message(
                user_email=st.session_state.user_email,
                user_name=st.session_state.user_name,
                message=user_input,
                conversation_id=st.session_state.conversation_id,
                conversation_history=st.session_state.messages[:-1]  # Exclude current message
            )
            # Add assistant response to chat
            st.session_state.messages.append({"role": "assistant", "content": response})
        except ModelEndpointError as e:
            # Model failures are shown as actionable errors - never stored
            # as assistant replies, so chat history stays clean and honest
            st.session_state.last_error = f"Model endpoint error: {e}"
        except Exception as e:
            st.session_state.last_error = f"Unexpected error: {e}"

    # Force rerun to update chat (renders full history + any error banner)
    st.rerun()

# Footer
st.markdown("---")
st.caption("© 2026 SkillAIPath | Autonomous Execution Support | Powered by Databricks")
