"""
RAG Query Module for SkillAIPath PR Reviewer
Queries the unified knowledge base for context-aware code reviews
NO SPARK DEPENDENCY - Uses pre-exported JSON
"""

import json
import os
from typing import List, Dict, Optional

# Knowledge base JSON file
KB_JSON_PATH = os.path.join(os.path.dirname(__file__), "knowledge_base.json")


class SkillAIPathKnowledgeBase:
    """Query interface for SkillAIPath unified knowledge base"""
    
    def __init__(self):
        self.chunks = []
        self.load_knowledge_base()
    
    def load_knowledge_base(self):
        """Load knowledge base from JSON file"""
        try:
            if os.path.exists(KB_JSON_PATH):
                with open(KB_JSON_PATH, 'r') as f:
                    self.chunks = json.load(f)
                print(f"✅ Loaded {len(self.chunks)} knowledge base chunks from JSON")
            else:
                print(f"⚠️ Knowledge base file not found: {KB_JSON_PATH}")
                self.chunks = []
        except Exception as e:
            print(f"⚠️ Failed to load knowledge base: {e}")
            self.chunks = []
    
    def search(self, query: str, top_k: int = 5, filters: Optional[Dict] = None) -> List[Dict]:
        """
        Search knowledge base with keyword matching
        
        Args:
            query: Search query text
            top_k: Number of results to return
            filters: Optional filters (e.g., {"chunk_type": "py"})
        
        Returns:
            List of matching chunks with context
        """
        if not self.chunks:
            return []
        
        # Extract search terms
        search_terms = [term.lower().strip() for term in query.split() if len(term) > 3]
        
        # Score each chunk
        scored_chunks = []
        for chunk in self.chunks:
            score = 0
            chunk_text_lower = chunk.get("chunk_text", "").lower()
            
            # Apply type filter if specified
            if filters and "chunk_type" in filters:
                if chunk.get("chunk_type") != filters["chunk_type"]:
                    continue
            
            # Keyword matching
            for term in search_terms:
                if term in chunk_text_lower:
                    score += chunk_text_lower.count(term)
            
            if score > 0:
                scored_chunks.append((score, chunk))
        
        # Sort by score and take top_k
        scored_chunks.sort(key=lambda x: x[0], reverse=True)
        top_chunks = [chunk for score, chunk in scored_chunks[:top_k]]
        
        return [
            {
                "chunk_id": chunk.get("chunk_id"),
                "text": chunk.get("chunk_text"),
                "source": chunk.get("parent_context"),
                "type": chunk.get("chunk_type"),
                "keywords": chunk.get("keywords", [])
            }
            for chunk in top_chunks
        ]
    
    def query_architecture_standards(self, layer: str = None) -> str:
        """Query architecture standards (Medallion, Unity Catalog, etc.)"""
        query = f"medallion architecture {layer if layer else ''} unity catalog table structure"
        results = self.search(query, top_k=3, filters={"chunk_type": "md"})
        
        if not results:
            return "No specific architecture standards found in knowledge base."
        
        context = "\n\n".join([
            f"**From {r['source']}**:\n{r['text'][:400]}..."
            for r in results
        ])
        
        return f"Architecture Standards:\n{context}"
    
    def query_edufin_rules(self, analysis_type: str = None) -> str:
        """Query EduFin business rules and patterns"""
        query = f"edufin {analysis_type if analysis_type else ''} portfolio risk cibil loan default"
        results = self.search(query, top_k=3)
        
        if not results:
            return "No EduFin-specific rules found in knowledge base."
        
        context = "\n\n".join([
            f"**From {r['source']}**:\n{r['text'][:400]}..."
            for r in results
        ])
        
        return f"EduFin Business Rules:\n{context}"
    
    def query_code_patterns(self, language: str, pattern_type: str) -> str:
        """Query code patterns and best practices"""
        query = f"{language} {pattern_type} pattern example code"
        filters = {"chunk_type": "py"} if language == "python" else {"chunk_type": "sql"} if language == "sql" else {}
        
        results = self.search(query, top_k=3, filters=filters)
        
        if not results:
            return f"No {language} {pattern_type} patterns found in knowledge base."
        
        context = "\n\n".join([
            f"**From {r['source']}**:\n{r['text'][:500]}..."
            for r in results
        ])
        
        return f"{language.title()} {pattern_type} Patterns:\n{context}"


# Global instance
_kb_instance = None

def get_knowledge_base() -> SkillAIPathKnowledgeBase:
    """Get or create knowledge base instance"""
    global _kb_instance
    if _kb_instance is None:
        _kb_instance = SkillAIPathKnowledgeBase()
    return _kb_instance


# Convenience functions
def query_architecture(layer: str = None) -> str:
    """Query architecture standards"""
    kb = get_knowledge_base()
    return kb.query_architecture_standards(layer)

def query_edufin(analysis_type: str = None) -> str:
    """Query EduFin business rules"""
    kb = get_knowledge_base()
    return kb.query_edufin_rules(analysis_type)

def query_patterns(language: str, pattern: str) -> str:
    """Query code patterns"""
    kb = get_knowledge_base()
    return kb.query_code_patterns(language, pattern)

def search_knowledge(query: str) -> str:
    """General knowledge search"""
    kb = get_knowledge_base()
    results = kb.search(query, top_k=5)
    if not results:
        return f"No results found for: {query}"
    return "\n\n".join([f"**{r['source']}**: {r['text'][:300]}..." for r in results])
