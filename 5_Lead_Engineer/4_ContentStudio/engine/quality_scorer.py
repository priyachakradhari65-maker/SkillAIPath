"""Quality Scorer - 2026 Content Philosophy Enforcement

Scores content against new strategy: subtle aggressive, professional capability,
real communication (not marketing machine).
"""

import re


class QualityScorer:
    """Scores content against brain pack quality standards."""
    
    PUBLISHING_THRESHOLD = 9.5  # Minimum score for evening posts
    MORNING_THRESHOLD = 8.0  # Morning posts: no CTA required
    
    def __init__(self):
        # Load standards from brain pack
        self.ai_slop_phrases = [
            "dive into", "let's explore", "game-changer", "revolutionary",
            "cutting-edge", "state-of-the-art", "journey", "delve",
            "in today's fast-paced world", "it's no secret that"
        ]
        
        self.selling_phrases = [
            "limited time", "don't miss", "act now", "special offer",
            "hurry", "last chance", "exclusive deal"
        ]
        
        self.chasing_phrases = [
            "please consider", "we'd love to", "hoping you'll",
            "would you be interested", "may we"
        ]
    
    def score(self, content, time_slot="evening"):
        """Score content against all quality criteria.
        
        Args:
            content: Content to score
            time_slot: "morning"/"evening"/"mixed" (affects pass threshold)
        
        Returns dict with:
        - score: float (0-10)
        - breakdown: dict of criterion scores
        - passed: bool (meets time-slot appropriate threshold)
        """
        
        breakdown = {}
        
        # Criterion 1: No AI Slop (2 points)
        ai_slop_count = sum(1 for phrase in self.ai_slop_phrases if phrase in content.lower())
        breakdown['no_ai_slop'] = max(0, 2 - (ai_slop_count * 0.5))
        
        # Criterion 2: No Selling/Chasing (2 points)
        selling_count = sum(1 for phrase in self.selling_phrases if phrase in content.lower())
        chasing_count = sum(1 for phrase in self.chasing_phrases if phrase in content.lower())
        breakdown['no_selling_chasing'] = max(0, 2 - ((selling_count + chasing_count) * 0.5))
        
        # Criterion 3: Proof Present (2 points)
        has_numbers = bool(re.search(r'\d+', content))
        has_specifics = any(word in content.lower() for word in 
                           ["module", "pipeline", "dashboard", "agent", "deployed", "production"])
        breakdown['proof_present'] = (1 if has_numbers else 0) + (1 if has_specifics else 0)
        
        # Criterion 4: Strong CTA (2 points)
        has_cta = any(word in content.lower() for word in 
                     ["join", "start", "link", "session", "book"])
        cta_direct = any(phrase in content.lower() for phrase in 
                        ["start building", "join founder", "live office"])
        breakdown['strong_cta'] = (1 if has_cta else 0) + (1 if cta_direct else 0)
        
        # Criterion 5: Founder Tone (2 points)
        # Short sentences, direct, no fluff
        sentences = content.split('.')
        avg_sentence_length = sum(len(s.split()) for s in sentences) / max(len(sentences), 1)
        is_concise = avg_sentence_length < 20
        is_direct = not any(word in content.lower() for word in 
                           ["maybe", "perhaps", "might", "could be"])
        breakdown['founder_tone'] = (1 if is_concise else 0.5) + (1 if is_direct else 0.5)
        
        # Total score
        total_score = sum(breakdown.values())
        
        # Context-aware threshold
        threshold = self.MORNING_THRESHOLD if time_slot == "morning" else self.PUBLISHING_THRESHOLD
        
        return {
            'score': total_score,
            'breakdown': breakdown,
            'passed': total_score >= threshold,
            'threshold': threshold,
            'time_slot': time_slot
        }