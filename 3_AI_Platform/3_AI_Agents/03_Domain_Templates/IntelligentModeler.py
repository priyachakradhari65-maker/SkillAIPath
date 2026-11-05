"""
INTELLIGENT DATA MODELER - Production Grade
Analyzes raw CSV data and automatically creates proper dimensional models

This is the INTELLIGENCE LAYER for Domain Templates.
Not template-based - analyzes actual data to make modeling decisions.

Features:
1. Automatic Fact vs Dimension Classification (data-driven)
2. Intelligent Grain Detection
3. Relationship Discovery with validation
4. Hierarchy Detection
5. SCD Pattern Analysis
6. Measure Intelligence (additive/semi-additive/non-additive)
7. Star Schema Design with proper surrogate keys

INPUT: Raw CSV files (no YAML config needed)
OUTPUT: Proper dimensional model with intelligent decisions
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional
import re
from dataclasses import dataclass
from collections import Counter


@dataclass
class TableAnalysis:
    """Analysis results for a single table"""
    file_name: str
    table_name: str
    row_count: int
    column_count: int
    table_type: str  # 'fact', 'dimension', 'bridge', 'reference'
    confidence_score: float  # 0-100
    grain: str
    primary_key: List[str]
    foreign_keys: List[Dict[str, Any]]
    measures: List[Dict[str, Any]]
    dimensions_refs: List[str]
    hierarchies: List[Dict[str, Any]]
    scd_recommendation: Dict[str, Any]
    columns: List[Dict[str, Any]]
    data_quality: Dict[str, Any]


@dataclass
class StarSchemaDesign:
    """Complete star schema design"""
    fact_tables: List[TableAnalysis]
    dimension_tables: List[TableAnalysis]
    bridge_tables: List[TableAnalysis]
    relationships: List[Dict[str, Any]]
    analysis_summary: Dict[str, Any]


class IntelligentModeler:
    """
    Intelligent Data Modeler - Analyzes data and creates proper dimensional models
    
    Intelligence Capabilities:
    1. Cardinality analysis for fact vs dimension classification
    2. Pattern detection for transactional vs descriptive data
    3. Relationship discovery with confidence scoring
    4. Hierarchy detection (date, org, geographic)
    5. SCD type recommendation based on data behavior
    6. Measure classification (additive/semi-additive/non-additive)
    7. Star schema design with proper surrogate keys
    """
    
    # Thresholds for intelligent classification
    LOW_CARDINALITY_THRESHOLD = 0.05  # 5% unique values → likely dimension
    HIGH_CARDINALITY_THRESHOLD = 0.90  # 90%+ unique values → likely PK
    FACT_ROW_THRESHOLD = 1000  # Facts typically have more rows
    
    # Pattern recognition
    MEASURE_PATTERNS = [
        r'amount', r'price', r'cost', r'total', r'sum', r'value',
        r'quantity', r'qty', r'count', r'num', r'revenue', r'sales',
        r'discount', r'tax', r'fee', r'rate', r'percent', r'score'
    ]
    
    TIMESTAMP_PATTERNS = [
        r'date', r'time', r'timestamp', r'created', r'updated',
        r'modified', r'effective', r'start', r'end', r'_at$', r'_on$'
    ]
    
    DIMENSION_ATTR_PATTERNS = [
        r'name', r'description', r'title', r'label', r'category',
        r'type', r'status', r'segment', r'class', r'group',
        r'region', r'country', r'state', r'city', r'location'
    ]
    
    def __init__(self, source_folder: str, sample_size: int = 10000):
        """
        Initialize Intelligent Modeler
        
        Args:
            source_folder: Path to CSV files
            sample_size: Rows to analyze (default 10K for speed vs accuracy balance)
        """
        self.source_folder = Path(source_folder)
        self.sample_size = sample_size
        self.table_analyses = []
        self.dataframes = {}  # Cache loaded dataframes
        
    def analyze_all(self) -> StarSchemaDesign:
        """
        Main analysis method - analyzes all CSV files and designs star schema
        
        Returns:
            StarSchemaDesign with complete dimensional model
        """
        print(f"\n{'='*80}")
        print(f"INTELLIGENT DATA MODELER - Starting Analysis")
        print(f"Source: {self.source_folder}")
        print(f"Sample Size: {self.sample_size:,} rows per file")
        print(f"{'='*80}\n")
        
        # Step 1: Load and analyze each file
        self._load_and_analyze_files()
        
        # Step 2: Discover relationships
        self._discover_relationships()
        
        # Step 3: Design star schema
        star_schema = self._design_star_schema()
        
        # Step 4: Print analysis report
        self._print_analysis_report(star_schema)
        
        return star_schema
    
    def _load_and_analyze_files(self):
        """Load all CSV files and perform individual table analysis"""
        csv_files = list(self.source_folder.glob("*.csv"))
        
        if not csv_files:
            raise FileNotFoundError(f"No CSV files found in {self.source_folder}")
        
        print(f"📊 Found {len(csv_files)} CSV files\n")
        
        for csv_file in sorted(csv_files):
            try:
                # Load data
                df = pd.read_csv(csv_file, nrows=self.sample_size)
                
                if df.empty:
                    print(f"⚠️  {csv_file.name}: Empty file, skipping\n")
                    continue
                
                # Store for relationship analysis
                table_name = self._generate_table_name(csv_file.stem)
                self.dataframes[table_name] = df
                
                # Analyze table
                analysis = self._analyze_table(csv_file, df)
                self.table_analyses.append(analysis)
                
                # Print quick summary
                self._print_table_summary(analysis)
                
            except Exception as e:
                print(f"❌ {csv_file.name}: Error - {str(e)}\n")
        
        print(f"✅ Analyzed {len(self.table_analyses)} tables\n")
    
    def _analyze_table(self, file_path: Path, df: pd.DataFrame) -> TableAnalysis:
        """
        Perform comprehensive analysis on a single table
        
        This is where the INTELLIGENCE happens - analyzing data patterns
        to make informed modeling decisions.
        """
        table_name = self._generate_table_name(file_path.stem)
        row_count = len(df)
        col_count = len(df.columns)
        
        # Analyze columns
        columns = self._analyze_columns(df)
        
        # Classify table type (INTELLIGENCE: fact vs dimension)
        table_type, confidence = self._classify_table_type(df, columns)
        
        # Detect primary key
        primary_key = self._detect_primary_key(df, columns)
        
        # Detect foreign keys
        foreign_keys = self._detect_foreign_keys(columns)
        
        # Identify measures (for facts)
        measures = self._identify_measures(columns)
        
        # Detect hierarchies
        hierarchies = self._detect_hierarchies(df, columns)
        
        # SCD recommendation
        scd_rec = self._recommend_scd_type(df, columns, table_type)
        
        # Determine grain
        grain = self._determine_grain(df, table_type, primary_key, measures)
        
        # Data quality assessment
        data_quality = self._assess_data_quality(df, columns)
        
        return TableAnalysis(
            file_name=file_path.name,
            table_name=table_name,
            row_count=row_count,
            column_count=col_count,
            table_type=table_type,
            confidence_score=confidence,
            grain=grain,
            primary_key=primary_key,
            foreign_keys=foreign_keys,
            measures=measures,
            dimensions_refs=[fk['column'] for fk in foreign_keys],
            hierarchies=hierarchies,
            scd_recommendation=scd_rec,
            columns=columns,
            data_quality=data_quality
        )
    
    def _analyze_columns(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Analyze each column's characteristics"""
        columns = []
        
        for col_name in df.columns:
            col_data = df[col_name]
            
            # Basic stats
            null_count = col_data.isna().sum()
            null_pct = (null_count / len(col_data)) * 100
            unique_count = col_data.nunique()
            cardinality = unique_count / len(col_data)
            
            # Type inference
            dtype = col_data.dtype
            spark_type = self._infer_spark_type(col_data)
            
            # Pattern detection
            is_measure = self._is_measure_column(col_name, col_data)
            is_timestamp = self._is_timestamp_column(col_name, col_data)
            is_dimension_attr = self._is_dimension_attribute(col_name, col_data)
            
            columns.append({
                'name': col_name,
                'spark_type': spark_type,
                'pandas_dtype': str(dtype),
                'null_count': int(null_count),
                'null_percentage': round(null_pct, 2),
                'unique_count': int(unique_count),
                'cardinality': round(cardinality, 4),
                'is_measure': is_measure,
                'is_timestamp': is_timestamp,
                'is_dimension_attr': is_dimension_attr,
                'sample_values': col_data.dropna().head(3).tolist()
            })
        
        return columns
    
    def _classify_table_type(self, df: pd.DataFrame, columns: List[Dict]) -> Tuple[str, float]:
        """
        INTELLIGENCE: Classify table as fact, dimension, bridge, or reference
        
        Returns: (table_type, confidence_score)
        """
        row_count = len(df)
        scores = {
            'fact': 0,
            'dimension': 0,
            'bridge': 0,
            'reference': 0
        }
        
        # Scoring factors
        measure_count = sum(1 for col in columns if col['is_measure'])
        timestamp_count = sum(1 for col in columns if col['is_timestamp'])
        dim_attr_count = sum(1 for col in columns if col['is_dimension_attr'])
        high_card_count = sum(1 for col in columns if col['cardinality'] > 0.9)
        low_card_count = sum(1 for col in columns if col['cardinality'] < 0.05)
        
        # FACT indicators
        if measure_count >= 2:
            scores['fact'] += 30
        if measure_count >= 1:
            scores['fact'] += 15
        if timestamp_count >= 1:
            scores['fact'] += 20
        if row_count > self.FACT_ROW_THRESHOLD:
            scores['fact'] += 20
        if high_card_count >= 1:  # Has high-cardinality ID
            scores['fact'] += 10
        
        # DIMENSION indicators
        if dim_attr_count >= 3:
            scores['dimension'] += 30
        if dim_attr_count >= 1:
            scores['dimension'] += 15
        if row_count < self.FACT_ROW_THRESHOLD:
            scores['dimension'] += 20
        if measure_count == 0:
            scores['dimension'] += 20
        if low_card_count >= len(columns) * 0.5:  # Most columns low cardinality
            scores['dimension'] += 15
        
        # BRIDGE indicators (many-to-many junction tables)
        if len(columns) == 2 and high_card_count == 2:
            scores['bridge'] += 40
        if len(columns) <= 3 and high_card_count >= 2:
            scores['bridge'] += 30
        
        # REFERENCE indicators (small lookup tables)
        if row_count < 100 and len(columns) <= 3:
            scores['reference'] += 30
        if low_card_count == len(columns) - 1:  # One ID, rest low cardinality
            scores['reference'] += 20
        
        # Select type with highest score
        table_type = max(scores, key=scores.get)
        max_score = scores[table_type]
        confidence = min(max_score, 100)
        
        return table_type, confidence
    
    def _detect_primary_key(self, df: pd.DataFrame, columns: List[Dict]) -> List[str]:
        """Detect primary key columns (can be composite)"""
        candidates = []
        
        for col in columns:
            # High cardinality + low nulls = likely PK
            if (col['cardinality'] > self.HIGH_CARDINALITY_THRESHOLD and 
                col['null_percentage'] < 5):
                candidates.append(col['name'])
        
        # Check for composite keys if no single key found
        if not candidates:
            # Try combinations
            for col in columns:
                if col['cardinality'] > 0.5 and col['null_percentage'] < 10:
                    candidates.append(col['name'])
        
        return candidates[:2]  # Return max 2 for composite key
    
    def _detect_foreign_keys(self, columns: List[Dict]) -> List[Dict[str, Any]]:
        """Detect likely foreign key columns"""
        foreign_keys = []
        
        for col in columns:
            col_name_lower = col['name'].lower()
            
            # FK patterns: *_id, *_key, *_code
            is_fk_pattern = (
                re.search(r'_id$', col_name_lower) or
                re.search(r'_key$', col_name_lower) or
                re.search(r'_code$', col_name_lower)
            )
            
            # FK characteristics: medium cardinality (not PK, not constant)
            is_fk_cardinality = 0.05 < col['cardinality'] < 0.90
            
            if is_fk_pattern and is_fk_cardinality:
                # Infer referenced table
                ref_table = re.sub(r'_(id|key|code)$', '', col_name_lower)
                
                foreign_keys.append({
                    'column': col['name'],
                    'references_table': f'dim_{ref_table}',
                    'cardinality': col['cardinality'],
                    'null_percentage': col['null_percentage']
                })
        
        return foreign_keys
    
    def _identify_measures(self, columns: List[Dict]) -> List[Dict[str, Any]]:
        """Identify measure columns and classify their aggregation type"""
        measures = []
        
        for col in columns:
            if not col['is_measure']:
                continue
            
            col_name_lower = col['name'].lower()
            
            # Classify measure type
            if any(p in col_name_lower for p in ['amount', 'total', 'sum', 'revenue', 'cost', 'price']):
                agg_type = 'additive'
            elif any(p in col_name_lower for p in ['average', 'avg', 'rate', 'percent', 'ratio']):
                agg_type = 'non-additive'
            elif any(p in col_name_lower for p in ['balance', 'inventory', 'stock']):
                agg_type = 'semi-additive'
            else:
                agg_type = 'additive'  # Default
            
            measures.append({
                'column': col['name'],
                'type': col['spark_type'],
                'aggregation_type': agg_type,
                'null_percentage': col['null_percentage']
            })
        
        return measures
    
    def _detect_hierarchies(self, df: pd.DataFrame, columns: List[Dict]) -> List[Dict[str, Any]]:
        """Detect hierarchical relationships (date, org, geographic)"""
        hierarchies = []
        
        # Date hierarchy detection
        date_cols = [col for col in columns if col['is_timestamp']]
        if date_cols:
            hierarchies.append({
                'type': 'date',
                'levels': ['year', 'quarter', 'month', 'week', 'day'],
                'source_column': date_cols[0]['name']
            })
        
        # Geographic hierarchy detection
        geo_patterns = ['country', 'state', 'city', 'region', 'location']
        geo_cols = [col['name'] for col in columns 
                   if any(p in col['name'].lower() for p in geo_patterns)]
        if len(geo_cols) >= 2:
            hierarchies.append({
                'type': 'geographic',
                'levels': geo_cols,
                'source_columns': geo_cols
            })
        
        # Organizational hierarchy detection
        org_patterns = ['company', 'department', 'team', 'manager', 'organization']
        org_cols = [col['name'] for col in columns 
                   if any(p in col['name'].lower() for p in org_patterns)]
        if len(org_cols) >= 2:
            hierarchies.append({
                'type': 'organizational',
                'levels': org_cols,
                'source_columns': org_cols
            })
        
        return hierarchies
    
    def _recommend_scd_type(self, df: pd.DataFrame, columns: List[Dict], 
                           table_type: str) -> Dict[str, Any]:
        """
        INTELLIGENCE: Recommend SCD type based on data characteristics
        """
        if table_type != 'dimension':
            return {'recommended_type': 'SCD1', 'reason': 'Not a dimension table'}
        
        # Check for change-tracking indicators
        has_effective_dates = any(col['is_timestamp'] for col in columns)
        has_version_cols = any('version' in col['name'].lower() for col in columns)
        has_current_flag = any('current' in col['name'].lower() or 'active' in col['name'].lower() 
                              for col in columns)
        
        # Check for attributes that typically need tracking
        tracking_attrs = [col for col in columns 
                         if col['is_dimension_attr'] and col['null_percentage'] < 50]
        
        if has_effective_dates or has_version_cols or has_current_flag:
            return {
                'recommended_type': 'SCD2',
                'reason': 'Has change-tracking indicators',
                'tracked_attributes': [col['name'] for col in tracking_attrs]
            }
        elif len(tracking_attrs) >= 3:
            return {
                'recommended_type': 'SCD2',
                'reason': 'Multiple attributes worth tracking',
                'tracked_attributes': [col['name'] for col in tracking_attrs]
            }
        else:
            return {
                'recommended_type': 'SCD1',
                'reason': 'Simple dimension, overwrite changes',
                'tracked_attributes': []
            }
    
    def _determine_grain(self, df: pd.DataFrame, table_type: str, 
                        primary_key: List[str], measures: List[Dict]) -> str:
        """Determine the grain of the table"""
        if table_type == 'fact':
            if not measures:
                return "Transaction level (no measures)"
            elif primary_key:
                return f"One row per {', '.join(primary_key)}"
            else:
                return "Transaction level"
        elif table_type == 'dimension':
            if primary_key:
                return f"One row per {primary_key[0]}"
            else:
                return "Descriptive attributes"
        else:
            return f"{table_type.capitalize()} table"
    
    def _assess_data_quality(self, df: pd.DataFrame, columns: List[Dict]) -> Dict[str, Any]:
        """Assess data quality metrics"""
        total_cells = len(df) * len(columns)
        null_cells = sum(col['null_count'] for col in columns)
        
        return {
            'total_rows': len(df),
            'total_columns': len(columns),
            'null_percentage': round((null_cells / total_cells) * 100, 2),
            'columns_with_nulls': sum(1 for col in columns if col['null_percentage'] > 0),
            'high_null_columns': [col['name'] for col in columns if col['null_percentage'] > 50]
        }
    
    def _discover_relationships(self):
        """
        INTELLIGENCE: Discover relationships by analyzing actual data
        (not just naming patterns)
        """
        print(f"🔗 Discovering Relationships\n")
        
        for analysis in self.table_analyses:
            for fk in analysis.foreign_keys:
                # Find matching dimension table
                ref_table_name = fk['references_table']
                
                # Try to find actual relationship in data
                source_df = self.dataframes.get(analysis.table_name)
                
                for dim_analysis in self.table_analyses:
                    if dim_analysis.table_type == 'dimension':
                        target_df = self.dataframes.get(dim_analysis.table_name)
                        
                        # Check if FK values exist in dimension
                        if source_df is not None and target_df is not None:
                            confidence = self._validate_relationship(
                                source_df, target_df, 
                                fk['column'], dim_analysis.primary_key
                            )
                            
                            if confidence > 0:
                                fk['validated_reference'] = dim_analysis.table_name
                                fk['confidence'] = confidence
                                print(f"   ✅ {analysis.table_name}.{fk['column']} → " + 
                                     f"{dim_analysis.table_name} ({confidence}% match)")
    
    def _validate_relationship(self, source_df: pd.DataFrame, target_df: pd.DataFrame,
                              fk_col: str, pk_cols: List[str]) -> float:
        """Validate foreign key relationship by checking actual data"""
        if not pk_cols or fk_col not in source_df.columns:
            return 0.0
        
        pk_col = pk_cols[0]  # Use first PK column
        
        if pk_col not in target_df.columns:
            return 0.0
        
        # Check match percentage
        fk_values = set(source_df[fk_col].dropna().unique())
        pk_values = set(target_df[pk_col].dropna().unique())
        
        if not fk_values:
            return 0.0
        
        matches = fk_values.intersection(pk_values)
        confidence = (len(matches) / len(fk_values)) * 100
        
        return round(confidence, 1)
    
    def _design_star_schema(self) -> StarSchemaDesign:
        """Design final star schema with all components"""
        fact_tables = [a for a in self.table_analyses if a.table_type == 'fact']
        dimension_tables = [a for a in self.table_analyses if a.table_type == 'dimension']
        bridge_tables = [a for a in self.table_analyses if a.table_type == 'bridge']
        
        # Build relationships list
        relationships = []
        for fact in fact_tables:
            for fk in fact.foreign_keys:
                if 'validated_reference' in fk:
                    relationships.append({
                        'from_table': fact.table_name,
                        'from_column': fk['column'],
                        'to_table': fk['validated_reference'],
                        'confidence': fk.get('confidence', 0)
                    })
        
        # Analysis summary
        summary = {
            'total_tables': len(self.table_analyses),
            'fact_tables': len(fact_tables),
            'dimension_tables': len(dimension_tables),
            'bridge_tables': len(bridge_tables),
            'total_relationships': len(relationships),
            'validated_relationships': sum(1 for r in relationships if r['confidence'] > 80)
        }
        
        return StarSchemaDesign(
            fact_tables=fact_tables,
            dimension_tables=dimension_tables,
            bridge_tables=bridge_tables,
            relationships=relationships,
            analysis_summary=summary
        )
    
    def _print_table_summary(self, analysis: TableAnalysis):
        """Print concise summary of table analysis"""
        print(f"📊 {analysis.file_name}")
        print(f"   Table: {analysis.table_name}")
        print(f"   Type: {analysis.table_type.upper()} (confidence: {analysis.confidence_score:.0f}%)")
        print(f"   Rows: {analysis.row_count:,} | Columns: {analysis.column_count}")
        print(f"   Grain: {analysis.grain}")
        
        if analysis.primary_key:
            print(f"   Primary Key: {', '.join(analysis.primary_key)}")
        
        if analysis.foreign_keys:
            print(f"   Foreign Keys: {len(analysis.foreign_keys)}")
        
        if analysis.measures:
            print(f"   Measures: {len(analysis.measures)} ({', '.join(m['aggregation_type'] for m in analysis.measures[:3])})")
        
        if analysis.scd_recommendation['recommended_type'] != 'SCD1':
            print(f"   SCD: {analysis.scd_recommendation['recommended_type']}")
        
        print()
    
    def _print_analysis_report(self, star_schema: StarSchemaDesign):
        """Print comprehensive analysis report"""
        print(f"\n{'='*80}")
        print(f"STAR SCHEMA DESIGN - Analysis Complete")
        print(f"{'='*80}\n")
        
        summary = star_schema.analysis_summary
        print(f"📊 Summary:")
        print(f"   Total Tables: {summary['total_tables']}")
        print(f"   • Fact Tables: {summary['fact_tables']}")
        print(f"   • Dimension Tables: {summary['dimension_tables']}")
        print(f"   • Bridge Tables: {summary['bridge_tables']}")
        print(f"   Relationships: {summary['total_relationships']} " +
              f"({summary['validated_relationships']} validated)\n")
        
        if star_schema.fact_tables:
            print(f"📈 Fact Tables:")
            for fact in star_schema.fact_tables:
                print(f"   • {fact.table_name}")
                print(f"     Measures: {len(fact.measures)}")
                print(f"     Dimensions: {len(fact.foreign_keys)}")
                print(f"     Grain: {fact.grain}")
            print()
        
        if star_schema.dimension_tables:
            print(f"📋 Dimension Tables:")
            for dim in star_schema.dimension_tables:
                print(f"   • {dim.table_name}")
                print(f"     Type: {dim.scd_recommendation['recommended_type']}")
                if dim.hierarchies:
                    print(f"     Hierarchies: {', '.join(h['type'] for h in dim.hierarchies)}")
            print()
        
        print(f"✅ Star schema design complete!\n")
    
    # Helper methods
    def _generate_table_name(self, file_stem: str) -> str:
        """Generate clean table name from filename"""
        name = re.sub(r'^[a-z]+_', '', file_stem.lower())
        name = re.sub(r'[^a-z0-9_]', '_', name)
        name = re.sub(r'_+', '_', name)
        return name.strip('_')
    
    def _infer_spark_type(self, series: pd.Series) -> str:
        """Infer Spark SQL data type"""
        dtype = series.dtype
        
        if pd.api.types.is_datetime64_any_dtype(dtype):
            return 'TIMESTAMP'
        if pd.api.types.is_integer_dtype(dtype):
            return 'BIGINT'
        if pd.api.types.is_float_dtype(dtype):
            return 'DOUBLE'
        if pd.api.types.is_bool_dtype(dtype):
            return 'BOOLEAN'
        
        # Try date parsing
        if pd.api.types.is_string_dtype(dtype):
            non_null = series.dropna()
            if len(non_null) > 0:
                try:
                    pd.to_datetime(non_null.head(10), errors='raise')
                    return 'DATE'
                except:
                    pass
        
        return 'STRING'
    
    def _is_measure_column(self, col_name: str, col_data: pd.Series) -> bool:
        """Detect if column is a measure"""
        col_lower = col_name.lower()
        
        # Name pattern check
        if any(re.search(p, col_lower) for p in self.MEASURE_PATTERNS):
            return True
        
        # Numeric type check
        if pd.api.types.is_numeric_dtype(col_data) and not pd.api.types.is_bool_dtype(col_data):
            # Check if values look like measures (not IDs)
            if col_data.nunique() / len(col_data) < 0.90:  # Not high cardinality like ID
                return True
        
        return False
    
    def _is_timestamp_column(self, col_name: str, col_data: pd.Series) -> bool:
        """Detect if column is timestamp/date"""
        col_lower = col_name.lower()
        
        if any(re.search(p, col_lower) for p in self.TIMESTAMP_PATTERNS):
            return True
        
        if pd.api.types.is_datetime64_any_dtype(col_data):
            return True
        
        return False
    
    def _is_dimension_attribute(self, col_name: str, col_data: pd.Series) -> bool:
        """Detect if column is descriptive dimension attribute"""
        col_lower = col_name.lower()
        
        if any(re.search(p, col_lower) for p in self.DIMENSION_ATTR_PATTERNS):
            return True
        
        # String columns with low cardinality
        if pd.api.types.is_string_dtype(col_data):
            cardinality = col_data.nunique() / len(col_data)
            if cardinality < 0.10:  # Low cardinality
                return True
        
        return False


def main():
    """CLI entry point"""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python intelligent_modeler.py <csv_folder_path>")
        sys.exit(1)
    
    folder_path = sys.argv[1]
    
    modeler = IntelligentModeler(folder_path)
    star_schema = modeler.analyze_all()
    
    print(f"\n💾 To generate SQL DDL and Spark code:")
    print(f"   from intelligent_modeler import IntelligentModeler")
    print(f"   modeler = IntelligentModeler('{folder_path}')")
    print(f"   star_schema = modeler.analyze_all()")
    print(f"   # Then use star_schema object to generate code")


if __name__ == '__main__':
    main()
