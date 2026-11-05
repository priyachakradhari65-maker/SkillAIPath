"""
SkillAI Execution Support Agent - Core Logic
Integrates RAG, LLM, and User Context for Autonomous Support
"""

import os
import sys
import json
import requests
from typing import Dict, List, Optional
from databricks.sdk import WorkspaceClient

# Add Knowledge Hub to path (actual location: 02_Knowledge_Hub with
# RagSystem.py / CentralReviewer.py - the old path/names never existed,
# so these imports always failed and the knowledge base was silently disabled)
KNOWLEDGE_HUB_PATH = "/Workspace/Shared/dev/SkillAIPath/3_AI_Platform/3_AI_Agents/02_Knowledge_Hub"
sys.path.append(KNOWLEDGE_HUB_PATH)

try:
    from RagSystem import SkillAIPathKnowledgeBase
    from CentralReviewer import CentralReviewer
except Exception as import_err:
    print(f"⚠️ Warning: Could not import Knowledge Hub modules: {import_err}")
    SkillAIPathKnowledgeBase = None
    CentralReviewer = None

from user_context import UserContextManager


class ModelEndpointError(Exception):
    """Raised when the model serving endpoint cannot be reached or misbehaves.

    Surfaced to the UI as an actionable error - never converted into a
    fake assistant reply.
    """


class SkillAIExecutionAgent:
    """Autonomous Execution Support Agent with RAG, User Tracking, and LLM"""
    
    def __init__(self, workspace_client: WorkspaceClient, catalog: str = "sap_dev"):
        self.w = workspace_client
        self.catalog = catalog
        
        # Initialize components
        self.user_context = UserContextManager(workspace_client, catalog)
        
        # Initialize RAG Knowledge Base
        self.knowledge_base = None
        if SkillAIPathKnowledgeBase:
            try:
                self.knowledge_base = SkillAIPathKnowledgeBase()
            except Exception as e:
                print(f"⚠️ Knowledge Base initialization failed: {e}")
        
        # Initialize Central Reviewer
        self.reviewer = None
        if CentralReviewer:
            try:
                self.reviewer = CentralReviewer(rag_system=self.knowledge_base)
            except Exception as e:
                print(f"⚠️ Central Reviewer initialization failed: {e}")
        
        # Model endpoint - verified READY foundation-model endpoint in this
        # workspace. Optional override via SKILLAI_MODEL_ENDPOINT env var.
        # Invocations use the app's WorkspaceClient credentials: Apps V2
        # injects DATABRICKS_CLIENT_ID/SECRET but NOT DATABRICKS_TOKEN, so the
        # old OpenAI(api_key=os.environ.get("DATABRICKS_TOKEN")) never worked.
        self.llm_endpoint = os.environ.get("SKILLAI_MODEL_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct")
        self.llm_timeout = int(os.environ.get("SKILLAI_MODEL_TIMEOUT", "120"))
    
    def get_user_context(self, user_email: str, user_name: str = None) -> Dict:
        """Get or create user profile with full context.

        Context is optional for chat: on failure, return an empty context
        with an 'error' flag instead of breaking the conversation.
        """
        try:
            user_profile = self.user_context.get_or_create_user(user_email, user_name)
            activities = self.user_context.get_user_activities(user_email, limit=5)
            return {
                "profile": user_profile,
                "recent_activities": activities
            }
        except Exception as e:
            print(f"⚠️ User context unavailable: {e}")
            return {
                "profile": {},
                "recent_activities": [],
                "error": str(e)
            }
    
    def search_knowledge(self, query: str, filters: Dict = None) -> List[Dict]:
        """Search the Knowledge Base (7,322 code chunks + Google Sheets)"""
        if not self.knowledge_base:
            return []
        
        try:
            results = self.knowledge_base.search(query, top_k=5, filters=filters)
            return results
        except Exception as e:
            print(f"⚠️ Knowledge search failed: {e}")
            return []
    
    def review_code(self, file_path: str, code: str, context: Dict = None) -> Dict:
        """Review code using Central Reviewer"""
        if not self.reviewer:
            return {"status": "UNAVAILABLE", "feedback": "Code reviewer not available"}
        
        try:
            return self.reviewer.review_code(file_path, code, context)
        except Exception as e:
            print(f"⚠️ Code review failed: {e}")
            return {"status": "ERROR", "feedback": str(e)}
    
    def generate_response(
        self,
        user_message: str,
        user_context: Dict,
        conversation_history: List[Dict],
        knowledge_results: List[Dict] = None
    ) -> str:
        """Generate AI response using LLM with full context"""
        
        if not self.llm_endpoint:
            raise ModelEndpointError("No model endpoint configured (set SKILLAI_MODEL_ENDPOINT)")
        
        # Build system prompt with user context
        user_profile = user_context.get("profile", {})
        role = user_profile.get("role", "Team Member")
        team = user_profile.get("team", "Engineering")
        user_name = user_profile.get("user_name", "Developer")
        total_interactions = user_profile.get("total_interactions", 0)
        
        # Build knowledge context
        knowledge_context = ""
        if knowledge_results:
            knowledge_context = "\n\n**Relevant Knowledge Base Entries:**\n"
            for idx, result in enumerate(knowledge_results[:3], 1):
                knowledge_context += f"\n{idx}. **{result.get('source', 'Unknown')}**\n{result.get('text', '')[:300]}...\n"
        
        system_prompt = f"""
You are the **SkillAI Execution Support Agent** - an autonomous Lead Engineer and Senior Architect guiding the SkillAI team.

**Current User Context:**
- Name: {user_name}
- Role: {role}
- Team: {team}
- Total Interactions: {total_interactions}
- Relationship: You've been working with this person, remember their context and history.

**Your Responsibilities:**
1. **Technical Leadership**: Provide senior-level architectural and engineering guidance
2. **Autonomous Execution Support**: Proactively suggest solutions, debug issues, and guide implementation
3. **Context Awareness**: Remember user's sprint plans, action items, bugs, and deliverables
4. **Knowledge Integration**: Leverage the 7,322-entry SkillAI codebase knowledge base
5. **Personalized Guidance**: Address the user by name and adapt to their level and context

**Available Knowledge Base:**
- SkillAIPath codebase (Python, SQL, notebooks)
- Architecture patterns (Medallion, Unity Catalog, DLT)
- Business rules (EduFin, SAP, Portfolio analysis)
- Google Sheets task tracking
{knowledge_context}

**Tone & Style:**
- Senior engineer mentoring a team member
- Direct, actionable, and technically precise
- Proactive - suggest next steps and potential issues
- Empathetic - understand context and constraints
- Use their name occasionally to personalize

**Response Guidelines:**
- Start with understanding the user's current goal
- Provide specific, actionable guidance
- Reference relevant code patterns from the knowledge base
- Anticipate follow-up questions and blockers
- Track their progress and celebrate wins
"""
        
        # Build messages for LLM
        messages = [{"role": "system", "content": system_prompt}]
        
        # Add conversation history (last 10 messages)
        for msg in conversation_history[-10:]:
            messages.append({
                "role": msg.get("role", "user"),
                "content": msg.get("content", "")
            })
        
        # Add current user message
        messages.append({"role": "user", "content": user_message})
        
        # Generate response via the serving-endpoint REST API, authenticated
        # with this app's WorkspaceClient credentials (auto-refreshed per call).
        try:
            host = self.w.config.host.rstrip("/")
            headers = self.w.config.authenticate()
            url = f"{host}/serving-endpoints/{self.llm_endpoint}/invocations"
            resp = requests.post(
                url,
                json={"messages": messages, "max_tokens": 1500, "temperature": 0.7},
                headers=headers,
                timeout=self.llm_timeout,
            )
            if resp.status_code != 200:
                try:
                    detail = resp.json().get("message", resp.text[:300])
                except Exception:
                    detail = resp.text[:300]
                raise ModelEndpointError(
                    f"Model endpoint '{self.llm_endpoint}' request failed "
                    f"(HTTP {resp.status_code}): {detail} "
                    f"Verify the endpoint exists, is READY, and this app can query it."
                )
            data = resp.json()
            choices = data.get("choices") or []
            if not choices:
                raise ModelEndpointError(
                    f"Model endpoint '{self.llm_endpoint}' returned no choices: {json.dumps(data)[:300]}"
                )
            content = (choices[0].get("message") or {}).get("content")
            if not content:
                raise ModelEndpointError("Model endpoint '{self.llm_endpoint}' returned an empty response")
            return content
        except ModelEndpointError:
            raise
        except Exception as e:
            raise ModelEndpointError(
                f"Model endpoint '{self.llm_endpoint}' invocation error: {e}"
            )
    
    def process_message(
        self,
        user_email: str,
        user_name: str,
        message: str,
        conversation_id: str,
        conversation_history: List[Dict] = None
    ) -> str:
        """Main entry point - process user message and generate response"""
        
        conversation_history = conversation_history or []
        
        # 1. Get user context
        user_context = self.get_user_context(user_email, user_name)
        
        # 2. Search knowledge base for relevant context
        knowledge_results = self.search_knowledge(message)
        
        # 3. Generate response
        response = self.generate_response(
            user_message=message,
            user_context=user_context,
            conversation_history=conversation_history,
            knowledge_results=knowledge_results
        )
        
        # 4. Save message to history
        try:
            self.user_context.save_message(
                conversation_id=conversation_id,
                user_email=user_email,
                role="user",
                content=message
            )
            
            self.user_context.save_message(
                conversation_id=conversation_id,
                user_email=user_email,
                role="assistant",
                content=response
            )
        except Exception as e:
            print(f"⚠️ Failed to save message history: {e}")
        
        return response
    
    def track_activity(
        self,
        user_email: str,
        activity_type: str,
        sprint_plan: str = None,
        action_items: List[str] = None,
        bugs: List[Dict] = None,
        deliverables: List[str] = None,
        status: str = "active"
    ):
        """Track user activity: sprints, tasks, bugs, deliverables"""
        
        activity_data = {
            'sprint_plan': sprint_plan or '',
            'action_items': action_items or [],
            'bugs': bugs or [],
            'deliverables': deliverables or [],
            'status': status
        }
        
        try:
            self.user_context.update_user_activity(user_email, activity_type, activity_data)
        except Exception as e:
            print(f"⚠️ Failed to track activity: {e}")
