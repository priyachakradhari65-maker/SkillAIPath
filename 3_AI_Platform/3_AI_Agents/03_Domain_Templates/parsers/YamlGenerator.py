"""
YAMLGenerator - Production Grade
Generates complete YAML configuration from parsed metadata

Takes schemas from SchemaExtractor and generates:
- Bronze source configurations
- Silver model templates
- Gold metrics suggestions
- Dashboard widget specs
- Governance tags
- Job scheduling
"""

import yaml
from typing import Dict, List, Any
from datetime import datetime


class YAMLGenerator:
    """Generate complete YAML config from parsed metadata"""
    
    def __init__(self, domain_name: str, catalog: str = "sap_dev"):
        self.domain_name = domain_name
        self.catalog = catalog
        self.config = {
            'domain': {
                'name': domain_name,
                'description': f'Auto-generated from legacy data files',
                'catalog': catalog,
                'created_at': datetime.now().isoformat()
            }
        }
    
    def add_bronze_sources(self, schemas: List[Dict[str, Any]]) -> None:
        """Add bronze layer configuration from schemas"""
        
        bronze_sources = []
        
        for schema in schemas:
            source = {
                'name': schema['table_name'],
                'source_file': schema['source_file'],
                'format': schema['format'],
                'table_type': schema['table_type'],
                'schema': []
            }
            
            # Add column definitions
            for col in schema['columns']:
                source['schema'].append({
                    'name': col['name'],
                    'type': col['type'],
                    'nullable': col['nullable'],
                    'description': f"Auto-detected from {schema['source_file']}"
                })
            
            # Add primary key if detected
            if schema.get('primary_key'):
                source['primary_key'] = schema['primary_key']
            
            # Add foreign keys if detected
            if schema.get('foreign_keys'):
                source['foreign_keys'] = schema['foreign_keys']
            
            bronze_sources.append(source)
        
        self.config['bronze'] = {
            'sources': bronze_sources,
            'ingestion_method': 'auto_loader',
            'checkpoint_location': f'/mnt/{self.catalog}/checkpoints/bronze'
        }
        
        print(f"✅ Added {len(bronze_sources)} bronze sources")
    
    def add_silver_models(self, schemas: List[Dict[str, Any]]) -> None:
        """Generate silver model templates"""
        
        # Group by fact/dimension
        facts = [s for s in schemas if s['table_type'] == 'fact']
        dims = [s for s in schemas if s['table_type'] == 'dimension']
        
        silver_models = []
        
        # Create enriched fact tables
        for fact in facts:
            model_name = f"enriched_{fact['table_name']}"
            
            model = {
                'name': model_name,
                'description': f'Enriched {fact["table_name"]} with dimension joins',
                'source': f'bronze.{fact["table_name"]}',
                'joins': [],
                'transformations': [
                    {
                        'type': 'quality_check',
                        'rule': f'{fact.get("primary_key", "id")} IS NOT NULL',
                        'action': 'filter'
                    }
                ],
                'materialization': 'incremental'
            }
            
            # Auto-detect potential joins based on foreign keys
            for fk in fact.get('foreign_keys', []):
                # Try to match with dimension tables
                for dim in dims:
                    dim_pk = dim.get('primary_key', '')
                    # If FK name matches dimension PK, suggest join
                    if fk.lower() == dim_pk.lower():
                        model['joins'].append({
                            'table': f'bronze.{dim["table_name"]}',
                            'type': 'left',
                            'on': fk
                        })
            
            silver_models.append(model)
        
        # Add dimension tables as-is (with SCD Type 2 placeholder)
        for dim in dims:
            model = {
                'name': dim['table_name'],
                'description': f'Dimension: {dim["table_name"]}',
                'source': f'bronze.{dim["table_name"]}',
                'scd_type': 2,  # Slowly Changing Dimension Type 2
                'materialization': 'table'
            }
            silver_models.append(model)
        
        self.config['silver'] = {
            'models': silver_models,
            'quality_checks': [
                {'name': 'row_count', 'threshold': 0, 'severity': 'error'},
                {'name': 'freshness', 'max_hours': 24, 'severity': 'warn'}
            ]
        }
        
        print(f"✅ Added {len(silver_models)} silver models")
    
    def add_gold_metrics(self, schemas: List[Dict[str, Any]]) -> None:
        """Generate gold metrics based on fact tables"""
        
        facts = [s for s in schemas if s['table_type'] == 'fact']
        
        metrics = []
        
        for fact in facts:
            # Generate common metrics for fact tables
            pk = fact.get('primary_key', 'id')
            
            # Count metric
            metrics.append({
                'name': f'total_{fact["table_name"]}_count',
                'description': f'Total count of {fact["table_name"]} records',
                'expression': f'COUNT(DISTINCT {pk})',
                'source': f'silver.enriched_{fact["table_name"]}'
            })
            
            # Look for numeric columns for SUM/AVG
            for col in fact['columns']:
                if col['type'] in ['BIGINT', 'DOUBLE']:
                    col_name = col['name']
                    
                    # Sum metric
                    metrics.append({
                        'name': f'total_{col_name}',
                        'description': f'Total sum of {col_name}',
                        'expression': f'SUM({col_name})',
                        'source': f'silver.enriched_{fact["table_name"]}'
                    })
                    
                    # Average metric
                    metrics.append({
                        'name': f'avg_{col_name}',
                        'description': f'Average {col_name}',
                        'expression': f'AVG({col_name})',
                        'source': f'silver.enriched_{fact["table_name"]}'
                    })
        
        self.config['gold'] = {
            'metrics': metrics[:10],  # Limit to top 10 for now
            'aggregation_level': 'daily',
            'retention_days': 730
        }
        
        print(f"✅ Added {len(metrics[:10])} gold metrics")
    
    def add_dashboard_template(self, schemas: List[Dict[str, Any]]) -> None:
        """Generate basic dashboard template"""
        
        # Get first metric table for dashboard
        facts = [s for s in schemas if s['table_type'] == 'fact']
        
        if not facts:
            return
        
        fact = facts[0]  # Use first fact table
        
        widgets = []
        
        # Add counter for total records
        widgets.append({
            'type': 'counter',
            'title': f'Total {fact["table_name"].replace("_", " ").title()}',
            'metric': f'COUNT(DISTINCT {fact.get("primary_key", "id")})',
            'position': {'x': 0, 'y': 0, 'width': 2, 'height': 2}
        })
        
        # Add bar chart if we have categorical columns
        categorical_cols = [c for c in fact['columns'] if c['type'] == 'STRING']
        if categorical_cols:
            cat_col = categorical_cols[0]
            widgets.append({
                'type': 'bar',
                'title': f'Distribution by {cat_col["name"].replace("_", " ").title()}',
                'x_axis': cat_col['name'],
                'y_axis': 'COUNT(*)',
                'position': {'x': 2, 'y': 0, 'width': 4, 'height': 4}
            })
        
        # Add line chart if we have date columns
        date_cols = [c for c in fact['columns'] 
                     if c['type'] in ['DATE', 'TIMESTAMP']]
        if date_cols:
            date_col = date_cols[0]
            widgets.append({
                'type': 'line',
                'title': 'Trend Over Time',
                'x_axis': date_col['name'],
                'y_axis': 'COUNT(*)',
                'position': {'x': 0, 'y': 4, 'width': 6, 'height': 4}
            })
        
        self.config['dashboards'] = [{
            'name': f'{self.domain_name.title()} Executive Dashboard',
            'description': 'Auto-generated dashboard from legacy data',
            'table': f'{self.catalog}.gold.mart_{fact["table_name"]}',
            'widgets': widgets
        }]
        
        print(f"✅ Added dashboard template with {len(widgets)} widgets")
    
    def add_jobs_config(self) -> None:
        """Add job scheduling configuration"""
        
        self.config['jobs'] = {
            'schedule': {
                'bronze_ingestion': {'cron': '0 */6 * * *', 'description': 'Every 6 hours'},
                'silver_transformation': {'cron': '0 1 * * *', 'description': 'Daily at 1 AM'},
                'gold_aggregation': {'cron': '0 2 * * *', 'description': 'Daily at 2 AM'}
            },
            'cluster': {
                'node_type': 'i3.xlarge',
                'num_workers': 2,
                'spark_version': '13.3.x-scala2.12'
            }
        }
        
        print("✅ Added jobs configuration")
    
    def add_governance(self, schemas: List[Dict[str, Any]]) -> None:
        """Add governance configuration"""
        
        # Detect potential PII columns
        pii_patterns = [
            r'.*email.*', r'.*phone.*', r'.*ssn.*', 
            r'.*address.*', r'.*name$'
        ]
        
        pii_columns = []
        for schema in schemas:
            for col in schema['columns']:
                col_name_lower = col['name'].lower()
                for pattern in pii_patterns:
                    import re
                    if re.search(pattern, col_name_lower):
                        pii_columns.append({
                            'table': f"{self.catalog}.bronze.{schema['table_name']}",
                            'column': col['name'],
                            'classification': 'PII'
                        })
        
        self.config['governance'] = {
            'tags': [
                {'key': 'domain', 'value': self.domain_name},
                {'key': 'generated_by', 'value': 'domain_templates_v2'},
                {'key': 'data_classification', 'value': 'internal'}
            ],
            'pii_columns': pii_columns[:5]  # Limit for readability
        }
        
        print(f"✅ Added governance config ({len(pii_columns)} PII columns detected)")
    
    def generate(self, output_path: str) -> str:
        """Generate and save YAML file"""
        
        yaml_content = yaml.dump(self.config, default_flow_style=False, sort_keys=False)
        
        with open(output_path, 'w') as f:
            # Add header
            f.write(f"# Auto-generated YAML Configuration\n")
            f.write(f"# Domain: {self.domain_name}\n")
            f.write(f"# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"# ========================================\n\n")
            f.write(yaml_content)
        
        return output_path
