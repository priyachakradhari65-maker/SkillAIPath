"""Training System - Learns from User Edits

Captures patterns from user edits and updates system knowledge.
"""

import json
from datetime import datetime
import os


class TrainingSystem:
    """Learns from user edits and improves content generation."""
    
    def __init__(self):
        self.training_db_path = "/Workspace/Shared/dev/SkillAIPath/4_Business_Intelligence/3_Live_Apps/ContentStudio/state/training_patterns.json"
        self.patterns = self._load_patterns()
        self.commercial_architecture = self._load_commercial_architecture()
    
    def _load_patterns(self):
        """Load existing training patterns."""
        try:
            if os.path.exists(self.training_db_path):
                with open(self.training_db_path, 'r') as f:
                    return json.load(f)
        except:
            pass
        
        return {
            "hooks": [],
            "tone_patterns": [],
            "quality_rules": [],
            "code_patterns": [],
            "edit_history": []
        }
    
    def _save_patterns(self):
        """Persist patterns to disk."""
        try:
            os.makedirs(os.path.dirname(self.training_db_path), exist_ok=True)
            with open(self.training_db_path, 'w') as f:
                json.dump(self.patterns, f, indent=2)
        except Exception as e:
            print(f"Error saving patterns: {e}")
    
    def add_pattern(self, pattern_type, content):
        """Add a new training pattern manually."""
        
        pattern_entry = {
            "type": pattern_type,
            "content": content,
            "added_at": datetime.now().isoformat(),
            "source": "manual_input"
        }
        
        # Route to appropriate category
        if "Hook" in pattern_type:
            self.patterns["hooks"].append(pattern_entry)
        elif "Quality" in pattern_type:
            self.patterns["quality_rules"].append(pattern_entry)
        elif "Code" in pattern_type:
            self.patterns["code_patterns"].append(pattern_entry)
        else:
            self.patterns["tone_patterns"].append(pattern_entry)
        
        self._save_patterns()
    
    def learn_from_edit(self, original, edited, content_id):
        """Learn from user edits.
        
        Analyzes what changed and extracts patterns.
        """
        
        # Record edit
        edit_record = {
            "content_id": content_id,
            "original": original,
            "edited": edited,
            "timestamp": datetime.now().isoformat(),
            "diff": self._analyze_diff(original, edited)
        }
        
        self.patterns["edit_history"].append(edit_record)
        
        # Extract patterns
        patterns_learned = self._extract_patterns(original, edited)
        
        # Update pattern database
        for pattern in patterns_learned:
            if pattern['type'] == 'hook':
                self.patterns["hooks"].append(pattern)
            elif pattern['type'] == 'tone':
                self.patterns["tone_patterns"].append(pattern)
        
        self._save_patterns()
        
        return patterns_learned
    
    def _analyze_diff(self, original, edited):
        """Analyze differences between original and edited content."""
        
        diff = {
            "added_phrases": [],
            "removed_phrases": [],
            "length_change": len(edited) - len(original),
            "tone_shift": None
        }
        
        # Simple diff - in production this would be more sophisticated
        orig_sentences = set(original.split('.'))
        edit_sentences = set(edited.split('.'))
        
        diff["added_phrases"] = list(edit_sentences - orig_sentences)[:3]
        diff["removed_phrases"] = list(orig_sentences - edit_sentences)[:3]
        
        return diff
    
    def _extract_patterns(self, original, edited):
        """Extract learnable patterns from edit."""
        
        patterns = []
        
        # Pattern: If user made content shorter and more direct
        if len(edited) < len(original) * 0.8:
            patterns.append({
                "type": "tone",
                "pattern": "prefer_shorter_direct",
                "confidence": 0.8,
                "learned_at": datetime.now().isoformat()
            })
        
        # Pattern: If user added specific numbers/metrics
        orig_has_numbers = any(char.isdigit() for char in original)
        edit_has_numbers = any(char.isdigit() for char in edited)
        
        if not orig_has_numbers and edit_has_numbers:
            patterns.append({
                "type": "tone",
                "pattern": "add_specific_metrics",
                "confidence": 0.9,
                "learned_at": datetime.now().isoformat()
            })
        
        # Pattern: New hook phrases
        # Extract unique phrases user added
        edited_lower = edited.lower()
        original_lower = original.lower()
        
        # Simple phrase extraction - find sentences unique to edited version
        edit_sentences = [s.strip() for s in edited.split('.') if s.strip()]
        orig_sentences = [s.strip() for s in original.split('.') if s.strip()]
        
        new_sentences = [s for s in edit_sentences if s not in orig_sentences]
        
        for sentence in new_sentences[:2]:  # Take top 2
            if len(sentence) > 10:  # Meaningful length
                patterns.append({
                    "type": "hook",
                    "content": sentence,
                    "confidence": 0.7,
                    "learned_at": datetime.now().isoformat()
                })
        
        return patterns
    
    def get_learned_hooks(self, category=None):
        """Get hooks learned from user edits."""
        
        hooks = self.patterns.get("hooks", [])
        
        if category:
            # Filter by category if needed
            hooks = [h for h in hooks if h.get('category') == category]
        
        return hooks
    
    def get_quality_rules(self):
        """Get learned quality rules."""
        return self.patterns.get("quality_rules", [])