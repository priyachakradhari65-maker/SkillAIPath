"""
INTELLIGENT DOMAIN MATCHER
Selects relevant SkillAIPath components based on domain context
Standards (testing, monitoring, orchestration) = Always included
Apps/Dashboards/Models = Domain-aware selection
"""
from typing import Dict, List, Set

class DomainMatcher:
    """Intelligently match SkillAIPath components to domain"""
    
    # UNIVERSAL COMPONENTS - Always replicate regardless of domain
    UNIVERSAL_STANDARDS = {
        'orchestration': [
            '1_Canvas/4_Orchestration/Data_Platform/00_Data_Platform_Orchestrator.py',
            '1_Canvas/4_Orchestration/Data_Platform/01_Bronze_Layer.py',
            '1_Canvas/4_Orchestration/Data_Platform/02_Dimensions.py',
            '1_Canvas/4_Orchestration/Data_Platform/03_Facts_Part1.py',
            '1_Canvas/4_Orchestration/Data_Platform/06_Gold_Part1.py'
        ],
        'testing': [
            '1_Canvas/6_Pytest_Framework/',
            '1_Canvas/8_Validation_Framework/'
        ],
        'monitoring': [
            '1_Canvas/3_Monitoring_Testing/',
            '4_Business_Intelligence/1_Monitoring_Observability/'
        ],
        'quality_gates': [
            '1_Canvas/2_Quality_Gates/'
        ],
        'dbt': [
            '2_Data_Production/03_Dbt/'
        ],
        'streaming': [
            '2_Data_Production/02_Medallion_Architecture/03_Streaming_Cdc/'
        ]
    }
    
    # DOMAIN-SPECIFIC MAPPINGS
    DOMAIN_APPS = {
        'healthcare': [
            'patient_portal',
            'clinical_analytics',
            'provider_dashboard',
            'facility_management'
        ],
        'retail': [
            'inventory_management',
            'pos_analytics',
            'customer_portal',
            'supply_chain_dashboard'
        ],
        'education': [
            'student_portal',
            'learning_analytics',
            'trainer_dashboard',
            'enrollment_management'
        ],
        'finance': [
            'transaction_monitor',
            'fraud_detection_dashboard',
            'risk_analytics',
            'compliance_portal'
        ],
        'manufacturing': [
            'production_monitor',
            'quality_dashboard',
            'maintenance_tracker',
            'oee_analytics'
        ]
    }
    
    DOMAIN_MLOPS = {
        'healthcare': [
            'readmission_prediction',
            'length_of_stay_forecast',
            'patient_risk_scoring'
        ],
        'retail': [
            'demand_forecasting',
            'product_recommendation',
            'churn_prediction'
        ],
        'education': [
            'at_risk_student_detection',
            'enrollment_prediction',
            'course_recommendation'
        ],
        'finance': [
            'fraud_detection',
            'credit_scoring',
            'transaction_anomaly'
        ],
        'manufacturing': [
            'predictive_maintenance',
            'defect_prediction',
            'yield_optimization'
        ]
    }
    
    @classmethod
    def select_components(cls, domain_name: str, domain_type: str) -> Dict[str, List]:
        """Select components for domain"""
        
        selected = {
            'universal_standards': [],
            'domain_specific': {
                'apps': [],
                'mlops': [],
                'dashboards': []
            }
        }
        
        # ALWAYS include universal standards
        for category, components in cls.UNIVERSAL_STANDARDS.items():
            selected['universal_standards'].extend(components)
        
        # Select domain-specific components
        domain_key = cls._normalize_domain(domain_type)
        
        if domain_key in cls.DOMAIN_APPS:
            selected['domain_specific']['apps'] = cls.DOMAIN_APPS[domain_key]
        
        if domain_key in cls.DOMAIN_MLOPS:
            selected['domain_specific']['mlops'] = cls.DOMAIN_MLOPS[domain_key]
        
        return selected
    
    @classmethod
    def _normalize_domain(cls, domain_type: str) -> str:
        """Normalize domain name"""
        domain_lower = domain_type.lower()
        
        # Map variations
        if 'health' in domain_lower or 'medical' in domain_lower or 'hospital' in domain_lower:
            return 'healthcare'
        elif 'retail' in domain_lower or 'ecommerce' in domain_lower or 'store' in domain_lower:
            return 'retail'
        elif 'education' in domain_lower or 'learning' in domain_lower or 'school' in domain_lower:
            return 'education'
        elif 'finance' in domain_lower or 'bank' in domain_lower or 'payment' in domain_lower:
            return 'finance'
        elif 'manufactur' in domain_lower or 'factory' in domain_lower or 'production' in domain_lower:
            return 'manufacturing'
        else:
            return 'education'  # Default
    
    @classmethod
    def print_selection_report(cls, domain_name: str, domain_type: str):
        """Print component selection report"""
        selected = cls.select_components(domain_name, domain_type)
        
        print(f"\n{'='*100}")
        print(f"🎯 COMPONENT SELECTION FOR: {domain_name} ({domain_type})")
        print(f"{'='*100}\n")
        
        print("✅ UNIVERSAL STANDARDS (Always Included):")
        print(f"   • Orchestration patterns")
        print(f"   • Testing frameworks (Pytest)")
        print(f"   • Monitoring & alerting")
        print(f"   • Quality gates")
        print(f"   • DBT integration (246 components)")
        print(f"   • Streaming/CDC patterns (17 components)")
        print(f"   Total: {len(selected['universal_standards'])} components\n")
        
        print("🎨 DOMAIN-SPECIFIC CUSTOMIZATIONS:")
        apps = selected['domain_specific']['apps']
        mlops = selected['domain_specific']['mlops']
        
        if apps:
            print(f"   Apps ({len(apps)}):")
            for app in apps:
                print(f"      • {app}")
        
        if mlops:
            print(f"\n   MLOps Models ({len(mlops)}):")
            for model in mlops:
                print(f"      • {model}")
        
        print(f"\n{'='*100}\n")

if __name__ == '__main__':
    # Test with different domains
    DomainMatcher.print_selection_report("Learning Platform", "Education")
    DomainMatcher.print_selection_report("Hospital Analytics", "Healthcare")
