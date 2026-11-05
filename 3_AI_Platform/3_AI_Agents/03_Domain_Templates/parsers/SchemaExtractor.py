"""
SchemaExtractor - Production Grade
Extracts table schemas from CSV and XLSX files automatically

Features:
- Auto-detects data types (string, int, float, date, boolean)
- Identifies primary keys and foreign keys by naming patterns
- Classifies fact vs dimension tables
- Handles mixed CSV/XLSX formats
- Samples data intelligently (first 1000 rows)
"""

import pandas as pd
import os
from pathlib import Path
from typing import Dict, List, Any, Optional
import re


class SchemaExtractor:
    """Extract table schemas from CSV/XLSX files"""
    
    # Common patterns for table classification
    FACT_PATTERNS = [r'^fact_', r'_fact$', r'^f_']
    DIM_PATTERNS = [r'^dim_', r'_dim$', r'^d_']
    
    # Common primary key patterns
    PK_PATTERNS = [r'.*_id$', r'^id$', r'.*_key$', r'^key$']
    
    def __init__(self, source_folder: str, sample_size: int = 1000):
        self.source_folder = Path(source_folder)
        self.sample_size = sample_size
        self.schemas = []
        
    def extract_all(self) -> List[Dict[str, Any]]:
        """Extract schemas from all CSV/XLSX files in folder"""
        
        if not self.source_folder.exists():
            raise FileNotFoundError(f"Source folder not found: {self.source_folder}")
        
        # Find all data files
        data_files = []
        data_files.extend(self.source_folder.glob("*.csv"))
        data_files.extend(self.source_folder.glob("*.xlsx"))
        
        print(f"\n📊 Found {len(data_files)} data files to parse")
        print("="*80)
        
        for file_path in sorted(data_files):
            try:
                schema = self._extract_schema(file_path)
                if schema:
                    self.schemas.append(schema)
                    print(f"✅ {file_path.name}: {len(schema['columns'])} columns")
            except Exception as e:
                print(f"⚠️  {file_path.name}: Error - {str(e)}")
        
        print("="*80)
        print(f"✅ Extracted {len(self.schemas)} schemas\n")
        
        return self.schemas
    
    def _extract_schema(self, file_path: Path) -> Optional[Dict[str, Any]]:
        """Extract schema from a single file"""
        
        # Read file based on extension
        if file_path.suffix == '.csv':
            df = pd.read_csv(file_path, nrows=self.sample_size)
        elif file_path.suffix == '.xlsx':
            df = pd.read_excel(file_path, nrows=self.sample_size)
        else:
            return None
        
        # Skip empty files
        if df.empty:
            return None
        
        # Extract metadata
        table_name = self._generate_table_name(file_path.stem)
        table_type = self._classify_table(file_path.stem)
        columns = self._extract_columns(df)
        primary_key = self._detect_primary_key(columns)
        foreign_keys = self._detect_foreign_keys(columns)
        
        return {
            'source_file': file_path.name,
            'table_name': table_name,
            'table_type': table_type,  # 'fact' or 'dimension'
            'row_count_sample': len(df),
            'columns': columns,
            'primary_key': primary_key,
            'foreign_keys': foreign_keys,
            'format': file_path.suffix[1:]  # Remove dot
        }
    
    def _generate_table_name(self, file_stem: str) -> str:
        """Generate clean table name from filename"""
        # Remove common prefixes like 'lp_'
        name = re.sub(r'^[a-z]+_', '', file_stem.lower())
        # Replace spaces and special chars with underscores
        name = re.sub(r'[^a-z0-9_]', '_', name)
        # Remove consecutive underscores
        name = re.sub(r'_+', '_', name)
        return name.strip('_')
    
    def _classify_table(self, file_stem: str) -> str:
        """Classify as fact or dimension table"""
        name_lower = file_stem.lower()
        
        # Check for fact patterns
        for pattern in self.FACT_PATTERNS:
            if re.search(pattern, name_lower):
                return 'fact'
        
        # Check for dimension patterns
        for pattern in self.DIM_PATTERNS:
            if re.search(pattern, name_lower):
                return 'dimension'
        
        # Default to dimension (safer assumption)
        return 'dimension'
    
    def _extract_columns(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Extract column metadata with type inference"""
        columns = []
        
        for col_name in df.columns:
            col_data = df[col_name]
            
            # Infer Spark SQL data type
            spark_type = self._infer_spark_type(col_data)
            
            # Calculate basic stats
            null_count = col_data.isna().sum()
            null_percentage = (null_count / len(col_data)) * 100
            
            columns.append({
                'name': col_name,
                'type': spark_type,
                'nullable': null_count > 0,
                'null_percentage': round(null_percentage, 2),
                'sample_values': col_data.dropna().head(3).tolist()
            })
        
        return columns
    
    def _infer_spark_type(self, series: pd.Series) -> str:
        """Infer Spark SQL data type from pandas series"""
        dtype = series.dtype
        
        # Check for datetime
        if pd.api.types.is_datetime64_any_dtype(dtype):
            return 'TIMESTAMP'
        
        # Check for integers
        if pd.api.types.is_integer_dtype(dtype):
            return 'BIGINT'
        
        # Check for floats
        if pd.api.types.is_float_dtype(dtype):
            return 'DOUBLE'
        
        # Check for booleans
        if pd.api.types.is_bool_dtype(dtype):
            return 'BOOLEAN'
        
        # Try to parse dates from strings
        if pd.api.types.is_string_dtype(dtype):
            non_null = series.dropna()
            if len(non_null) > 0:
                # Try parsing first few values as dates
                try:
                    pd.to_datetime(non_null.head(10), errors='raise')
                    return 'DATE'
                except:
                    pass
        
        # Default to STRING
        return 'STRING'
    
    def _detect_primary_key(self, columns: List[Dict]) -> Optional[str]:
        """Detect likely primary key column"""
        for col in columns:
            col_name_lower = col['name'].lower()
            
            # Check against PK patterns
            for pattern in self.PK_PATTERNS:
                if re.search(pattern, col_name_lower):
                    # Primary key should be non-nullable
                    if not col['nullable'] or col['null_percentage'] < 5:
                        return col['name']
        
        return None
    
    def _detect_foreign_keys(self, columns: List[Dict]) -> List[str]:
        """Detect likely foreign key columns"""
        foreign_keys = []
        
        for col in columns:
            col_name_lower = col['name'].lower()
            
            # Foreign keys usually end with _id or _key
            if re.search(r'.*_id$', col_name_lower) or re.search(r'.*_key$', col_name_lower):
                # Skip if already identified as primary key
                if col['name'] not in foreign_keys:
                    foreign_keys.append(col['name'])
        
        return foreign_keys
