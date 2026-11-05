"""
TEMPLATE EXTRACTOR V2
Scans SkillAIPath architecture and extracts reusable patterns
Enables autonomous replication into new domains
"""
import os
import re
from pathlib import Path
from typing import Dict, List, Set

class TemplateExtractor:
    """Extract patterns from SkillAIPath reference architecture"""
    
    def __init__(self, skillaipath_root: str):
        self.root = Path(skillaipath_root)
        self.patterns = {
            'dimensions': [],
            'facts': [],
            'streaming': [],
            'gold': [],
            'mlops': [],
            'monitoring': [],
            'orchestration': [],
            'apps': []
        }
    
    def scan_architecture(self) -> Dict:
        """Scan entire SkillAIPath and catalog all components"""
        print("🔍 Scanning SkillAIPath Architecture...")
        
        # Scan medallion architecture
        self._scan_dimensions()
        self._scan_facts()
        self._scan_streaming()
        self._scan_gold()
        
        # Scan AI platform
        self._scan_mlops()
        
        # Scan monitoring
        self._scan_monitoring()
        
        # Scan apps
        self._scan_apps()
        
        return self.get_inventory()
    
    def _scan_dimensions(self):
        """Extract all dimension patterns"""
        dim_path = self.root / "2_Data_Production/02_Medallion_Architecture/01_Dimensions"
        if dim_path.exists():
            for file in dim_path.glob("*.py"):
                pattern_type = self._classify_dimension(file.name)
                self.patterns['dimensions'].append({
                    'name': file.stem,
                    'file': str(file),
                    'type': pattern_type,
                    'complexity': self._estimate_complexity(file)
                })
    
    def _scan_facts(self):
        """Extract all fact patterns"""
        fact_path = self.root / "2_Data_Production/02_Medallion_Architecture/02_Facts"
        if fact_path.exists():
            for file in fact_path.glob("*.py"):
                pattern_type = self._classify_fact(file.name)
                self.patterns['facts'].append({
                    'name': file.stem,
                    'file': str(file),
                    'type': pattern_type,
                    'complexity': self._estimate_complexity(file)
                })
    
    def _scan_streaming(self):
        """Extract streaming/CDC patterns"""
        stream_path = self.root / "2_Data_Production/02_Medallion_Architecture/03_Streaming_Cdc"
        if stream_path.exists():
            for file in stream_path.glob("*.py"):
                self.patterns['streaming'].append({
                    'name': file.stem,
                    'file': str(file),
                    'type': 'streaming_cdc',
                    'complexity': self._estimate_complexity(file)
                })
    
    def _scan_gold(self):
        """Extract gold layer patterns"""
        gold_path = self.root / "2_Data_Production/02_Medallion_Architecture/04_Gold"
        if gold_path.exists():
            for file in gold_path.glob("*.py"):
                self.patterns['gold'].append({
                    'name': file.stem,
                    'file': str(file),
                    'complexity': self._estimate_complexity(file)
                })
    
    def _scan_mlops(self):
        """Extract MLOps patterns"""
        mlops_path = self.root / "3_AI_Platform/1_MLOps"
        if mlops_path.exists():
            for file in mlops_path.glob("*.py"):
                self.patterns['mlops'].append({
                    'name': file.stem,
                    'file': str(file),
                    'complexity': self._estimate_complexity(file)
                })
    
    def _scan_monitoring(self):
        """Extract monitoring patterns"""
        mon_path = self.root / "4_Business_Intelligence/1_Monitoring_Observability"
        if mon_path.exists():
            for subdir in mon_path.iterdir():
                if subdir.is_dir():
                    for file in subdir.glob("*.py"):
                        self.patterns['monitoring'].append({
                            'name': file.stem,
                            'file': str(file),
                            'category': subdir.name
                        })
    
    def _scan_apps(self):
        """Extract app patterns"""
        app_path = self.root / "4_Business_Intelligence/2_Databricks_Apps"
        if app_path.exists():
            for subdir in app_path.iterdir():
                if subdir.is_dir() and not subdir.name.startswith('.'):
                    self.patterns['apps'].append({
                        'name': subdir.name,
                        'path': str(subdir)
                    })
    
    def _classify_dimension(self, filename: str) -> str:
        """Classify dimension type"""
        if 'scd1' in filename.lower():
            return 'SCD1'
        elif 'scd2' in filename.lower():
            return 'SCD2'
        elif 'truncate' in filename.lower():
            return 'Full Truncate'
        else:
            return 'Simple'
    
    def _classify_fact(self, filename: str) -> str:
        """Classify fact pattern"""
        if 'upsert' in filename.lower():
            return 'Upsert'
        elif 'full_reload' in filename.lower():
            return 'Full Reload'
        elif 'bridge' in filename.lower():
            return 'Bridge Table'
        else:
            return 'Append'
    
    def _estimate_complexity(self, file: Path) -> str:
        """Estimate pattern complexity based on file size"""
        try:
            size = file.stat().st_size
            if size < 5000:
                return 'Simple'
            elif size < 15000:
                return 'Medium'
            else:
                return 'Complex'
        except:
            return 'Unknown'
    
    def get_inventory(self) -> Dict:
        """Return full inventory of patterns"""
        return {
            'dimensions': {
                'count': len(self.patterns['dimensions']),
                'items': self.patterns['dimensions']
            },
            'facts': {
                'count': len(self.patterns['facts']),
                'items': self.patterns['facts']
            },
            'streaming': {
                'count': len(self.patterns['streaming']),
                'items': self.patterns['streaming']
            },
            'gold': {
                'count': len(self.patterns['gold']),
                'items': self.patterns['gold']
            },
            'mlops': {
                'count': len(self.patterns['mlops']),
                'items': self.patterns['mlops']
            },
            'monitoring': {
                'count': len(self.patterns['monitoring']),
                'items': self.patterns['monitoring']
            },
            'apps': {
                'count': len(self.patterns['apps']),
                'items': self.patterns['apps']
            }
        }
    
    def export_catalog(self, output_path: str):
        """Export pattern catalog to YAML"""
        import yaml
        with open(output_path, 'w') as f:
            yaml.dump(self.get_inventory(), f, default_flow_style=False)
        print(f"✅ Pattern catalog exported to {output_path}")

def main():
    skillaipath_root = "/Workspace/Users/user@example.com/SkillAIPath"
    extractor = TemplateExtractor(skillaipath_root)
    inventory = extractor.scan_architecture()
    
    print("\n" + "="*80)
    print("📊 SKILLAIPATH PATTERN INVENTORY")
    print("="*80)
    
    for category, data in inventory.items():
        print(f"\n{category.upper()}: {data['count']} patterns")
    
    return inventory

if __name__ == '__main__':
    main()
