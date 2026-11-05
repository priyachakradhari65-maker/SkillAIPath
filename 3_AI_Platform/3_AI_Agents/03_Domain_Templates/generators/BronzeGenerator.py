'''
BronzeGenerator - YAML → Bronze Notebooks
Generates Auto Loader notebooks for ingesting CSV/XLSX into Bronze tables
'''
import yaml
from pathlib import Path
from typing import Dict, List

class BronzeGenerator:
    '''Generate Bronze ingestion notebooks from YAML'''
    
    def __init__(self, config_path: str):
        self.config_path = Path(config_path)
        self.config = self._load_config()
    
    def _load_config(self) -> Dict:
        with open(self.config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def generate_all(self) -> List[str]:
        '''Generate all Bronze notebooks'''
        notebooks = []
        bronze_config = self.config.get('bronze', {})
        sources = bronze_config.get('sources', [])
        
        print(f"Generating {len(sources)} Bronze notebooks...")
        
        for source in sources:
            notebook_code = self._generate_bronze_notebook(source)
            notebooks.append({
                'name': f"Bronze_{source['name']}",
                'code': notebook_code
            })
        
        return notebooks
    
    def _generate_bronze_notebook(self, source: Dict) -> str:
        '''Generate PySpark notebook code for Bronze ingestion'''
        table_name = source['name']
        source_file = source['source_file']
        format_type = source['format']
        catalog = self.config['domain']['catalog']
        
        # Fixed: Use proper format method call
        read_method = 'csv' if format_type == 'csv' else 'format("excel")'
        
        code = f"""# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion: {table_name}
# MAGIC Source: {source_file}

# COMMAND ----------

from pyspark.sql import functions as F

# Configuration
source_path = "/Workspace/Users/<user>/{source_file}"
catalog = "{catalog}"
schema = "bronze"
table_name = "{table_name}"

# Read and load
df = spark.read.{read_method}(source_path, header=True, inferSchema=True)
df.write.mode("overwrite").saveAsTable(f"{{catalog}}.{{schema}}.{{table_name}}")

print(f"✅ Loaded {{df.count()}} rows")
"""
        return code

def main():
    import sys
    if len(sys.argv) < 2:
        print("Usage: python bronze_generator.py <config.yaml>")
        sys.exit(1)
    
    generator = BronzeGenerator(sys.argv[1])
    notebooks = generator.generate_all()
    print(f"✅ Generated {len(notebooks)} Bronze notebooks")

if __name__ == '__main__':
    main()
