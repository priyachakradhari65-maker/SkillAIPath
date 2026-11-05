"""
DocumentParser - Main Orchestrator (Production Grade)
Parses legacy folder and generates complete YAML configuration

Usage:
    python document_parser.py --source /path/to/legacy/folder --domain learning_platform

This is the main entry point for document-driven architecture generation.
"""

import argparse
import sys
from pathlib import Path
from typing import Dict, List, Any

# Import our parsers
from schema_extractor import SchemaExtractor
from yaml_generator import YAMLGenerator


class DocumentParser:
    """Main orchestrator for document parsing pipeline"""
    
    def __init__(self, source_folder: str, domain_name: str, catalog: str = "sap_dev"):
        self.source_folder = Path(source_folder)
        self.domain_name = domain_name
        self.catalog = catalog
        
        # Initialize components
        self.schema_extractor = SchemaExtractor(source_folder)
        self.yaml_generator = YAMLGenerator(domain_name, catalog)
        
        self.schemas = []
    
    def parse_all(self) -> Dict[str, Any]:
        """Run complete parsing pipeline"""
        
        print("\n" + "="*80)
        print(f"📋 DOCUMENT PARSER - {self.domain_name.upper()}")
        print("="*80)
        print(f"Source: {self.source_folder}")
        print(f"Domain: {self.domain_name}")
        print(f"Catalog: {self.catalog}")
        print("="*80)
        
        # Step 1: Extract schemas from CSV/XLSX
        print("\n🔍 STEP 1: Extracting Schemas from CSV/XLSX files")
        print("-"*80)
        self.schemas = self.schema_extractor.extract_all()
        
        if not self.schemas:
            print("\n⚠️  No data files found. Exiting.")
            return {}
        
        # Step 2: Generate YAML configuration
        print("\n📝 STEP 2: Generating YAML Configuration")
        print("-"*80)
        
        self.yaml_generator.add_bronze_sources(self.schemas)
        self.yaml_generator.add_silver_models(self.schemas)
        self.yaml_generator.add_gold_metrics(self.schemas)
        self.yaml_generator.add_dashboard_template(self.schemas)
        self.yaml_generator.add_jobs_config()
        self.yaml_generator.add_governance(self.schemas)
        
        return self.yaml_generator.config
    
    def save_config(self, output_path: str = None) -> str:
        """Save generated YAML configuration"""
        
        if output_path is None:
            output_path = f"../configs/config_{self.domain_name}.yaml"
        
        output_file = self.yaml_generator.generate(output_path)
        
        print("\n" + "="*80)
        print("✅ YAML CONFIGURATION GENERATED!")
        print("="*80)
        print(f"Output: {output_file}")
        
        # Print summary
        config = self.yaml_generator.config
        print(f"\nSummary:")
        print(f"  • Bronze sources: {len(config.get('bronze', {}).get('sources', []))}")
        print(f"  • Silver models: {len(config.get('silver', {}).get('models', []))}")
        print(f"  • Gold metrics: {len(config.get('gold', {}).get('metrics', []))}")
        print(f"  • Dashboards: {len(config.get('dashboards', []))}")
        
        print(f"\n🚀 NEXT STEP: Run the generators!")
        print(f"   python domain_customizer.py \\")
        print(f"       --domain {self.domain_name} \\")
        print(f"       --config {output_file} \\")
        print(f"       --deploy-all")
        
        return output_file
    
    def print_summary(self) -> None:
        """Print detailed summary of parsed data"""
        
        print("\n" + "="*80)
        print("📊 PARSED DATA SUMMARY")
        print("="*80)
        
        # Group by table type
        facts = [s for s in self.schemas if s['table_type'] == 'fact']
        dims = [s for s in self.schemas if s['table_type'] == 'dimension']
        
        print(f"\nFact Tables ({len(facts)}):")
        for fact in facts:
            pk = fact.get('primary_key', 'None')
            fks = ', '.join(fact.get('foreign_keys', [])) or 'None'
            print(f"  • {fact['table_name']} (PK: {pk}, FKs: {fks})")
        
        print(f"\nDimension Tables ({len(dims)}):")
        for dim in dims:
            pk = dim.get('primary_key', 'None')
            print(f"  • {dim['table_name']} (PK: {pk})")
        
        print("\n" + "="*80)


def main():
    """Main entry point"""
    
    parser = argparse.ArgumentParser(
        description='Parse legacy data files and generate YAML configuration',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Parse Learning Platform data
  python document_parser.py \\
      --source /path/to/LearningPlatform \\
      --domain learning_platform
  
  # Specify custom catalog
  python document_parser.py \\
      --source /path/to/data \\
      --domain fintech \\
      --catalog prod_catalog
        """
    )
    
    parser.add_argument(
        '--source',
        required=True,
        help='Path to source folder containing CSV/XLSX files'
    )
    
    parser.add_argument(
        '--domain',
        required=True,
        help='Domain name (e.g., learning_platform, fintech, healthcare)'
    )
    
    parser.add_argument(
        '--catalog',
        default='sap_dev',
        help='Unity Catalog name (default: sap_dev)'
    )
    
    parser.add_argument(
        '--output',
        help='Output YAML file path (default: ../configs/config_<domain>.yaml)'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Print detailed summary'
    )
    
    args = parser.parse_args()
    
    try:
        # Create parser
        doc_parser = DocumentParser(
            source_folder=args.source,
            domain_name=args.domain,
            catalog=args.catalog
        )
        
        # Parse all documents
        config = doc_parser.parse_all()
        
        if not config:
            sys.exit(1)
        
        # Save configuration
        output_file = doc_parser.save_config(args.output)
        
        # Print detailed summary if requested
        if args.verbose:
            doc_parser.print_summary()
        
        print("\n✅ Done!\n")
        
    except Exception as e:
        print(f"\n❌ Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
