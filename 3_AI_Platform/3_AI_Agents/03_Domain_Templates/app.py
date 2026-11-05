"""Domain Templates Deployer - Premium No-Code Interface
Deploy production-grade data platforms by uploading CSV/Excel files.
"""
import streamlit as st
import pandas as pd
import sys
import time
from pathlib import Path
import io
import yaml
from datetime import datetime

# Databricks imports
from pyspark.sql import SparkSession

# Domain Templates imports
sys.path.insert(0, '/Workspace/Users/user@example.com/Domain_Templates')
from template_extractor import TemplateExtractor
from domain_matcher import DomainMatcher
from generators.bronze_generator import BronzeGenerator
from generators.silver_generator import SilverGenerator
from generators.gold_generator import GoldGenerator

# Page config
st.set_page_config(
    page_title="Domain Templates Deployer",
    page_icon="🚀",
    layout="wide"
)

# Initialize session state
if 'uploaded_files' not in st.session_state:
    st.session_state.uploaded_files = {}
if 'analyzed' not in st.session_state:
    st.session_state.analyzed = False
if 'deploying' not in st.session_state:
    st.session_state.deploying = False
if 'deployment_complete' not in st.session_state:
    st.session_state.deployment_complete = False
if 'deployment_result' not in st.session_state:
    st.session_state.deployment_result = None

# Initialize Spark
@st.cache_resource
def get_spark():
    return SparkSession.builder.getOrCreate()

spark = get_spark()

# Initialize SkillAIPath scanner
@st.cache_resource
def scan_skillaipath():
    """Scan SkillAIPath for available patterns"""
    skillaipath = "/Workspace/Users/user@example.com/SkillAIPath"
    extractor = TemplateExtractor(skillaipath)
    inventory = extractor.scan_architecture()
    return inventory, skillaipath

# === UI COMPONENTS ===

def render_header():
    """Render header section"""
    st.title("🚀 Domain Templates Deployer")
    st.markdown("### Deploy production-grade data platforms in minutes")
    
    # System status
    try:
        inventory, _ = scan_skillaipath()
        total_patterns = sum(v['count'] for v in inventory.values())
        st.success(f"✅ SkillAIPath: {total_patterns} patterns available")
    except:
        st.warning("⚠️ SkillAIPath: Scanning patterns...")

def analyze_uploaded_files(files_dict):
    """Analyze uploaded files and detect schema"""
    analysis_results = {}
    
    for filename, file_bytes in files_dict.items():
        try:
            # Read file based on extension
            if filename.endswith('.csv'):
                df = pd.read_csv(io.BytesIO(file_bytes))
            elif filename.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(io.BytesIO(file_bytes))
            else:
                continue
            
            # Detect table type (dimension vs fact)
            table_name = Path(filename).stem
            is_dimension = table_name.startswith('dim_') or 'dimension' in table_name.lower()
            is_fact = table_name.startswith('fact_') or 'fact' in table_name.lower()
            
            if is_dimension:
                table_type = 'dimension'
            elif is_fact:
                table_type = 'fact'
            else:
                # Infer from data: <100 rows likely dimension
                table_type = 'dimension' if len(df) < 100 else 'fact'
            
            # Store analysis
            analysis_results[filename] = {
                'table_name': table_name,
                'table_type': table_type,
                'rows': len(df),
                'columns': len(df.columns),
                'schema': df.dtypes.to_dict(),
                'sample': df.head(3),
                'df': df
            }
            
        except Exception as e:
            st.error(f"Error analyzing {filename}: {str(e)}")
    
    return analysis_results

def suggest_skillaipath_patterns(analysis_results):
    """Suggest SkillAIPath patterns for detected tables"""
    inventory, _ = scan_skillaipath()
    suggestions = {}
    
    for filename, analysis in analysis_results.items():
        table_name = analysis['table_name']
        table_type = analysis['table_type']
        
        if table_type == 'dimension':
            # Suggest dimension patterns
            if 'customer' in table_name.lower() or 'member' in table_name.lower():
                pattern = 'dim_customer_scd2'
            elif 'product' in table_name.lower() or 'project' in table_name.lower():
                pattern = 'dim_product'
            elif 'date' in table_name.lower():
                pattern = 'dim_date'
            else:
                pattern = 'dim_agent'  # generic dimension
        else:
            # Suggest fact patterns
            if 'sales' in table_name.lower() or 'session' in table_name.lower():
                pattern = 'fact_sales'
            elif 'task' in table_name.lower() or 'assignment' in table_name.lower():
                pattern = 'fact_purchases'
            else:
                pattern = 'fact_sales'  # generic fact
        
        suggestions[filename] = pattern
    
    return suggestions

def deploy_domain(domain_name, catalog_name, domain_type, analysis_results, pattern_mappings):
    """Execute autonomous domain deployment"""
    
    # Create config
    config = {
        'domain': {
            'name': domain_name,
            'catalog': catalog_name,
            'type': domain_type.lower()
        },
        'bronze': {
            'tables': []
        },
        'silver': {
            'dimensions': [],
            'facts': []
        },
        'gold': {
            'metrics': []
        }
    }
    
    # Build config from analysis
    for filename, analysis in analysis_results.items():
        table_name = analysis['table_name']
        table_type = analysis['table_type']
        pattern = pattern_mappings.get(filename, 'generic')
        
        # Bronze
        config['bronze']['tables'].append({
            'name': table_name,
            'file': filename,
            'rows': analysis['rows']
        })
        
        # Silver
        if table_type == 'dimension':
            config['silver']['dimensions'].append({
                'name': f"{table_name}_enhanced",
                'source_pattern': pattern,
                'pattern': 'SCD2' if 'scd2' in pattern else 'SCD1',
                'source': table_name,
                'description': f'Enhanced {table_name} with SkillAIPath patterns'
            })
        else:
            config['silver']['facts'].append({
                'name': f"{table_name}_enriched",
                'source_pattern': pattern,
                'joins': [],
                'source': table_name,
                'description': f'Enriched {table_name} with multi-table joins'
            })
    
    # Gold metrics
    if any(a['table_type'] == 'dimension' for a in analysis_results.values()):
        config['gold']['metrics'].append({
            'name': f"{domain_name.lower().replace(' ', '_')}_360",
            'source_pattern': 'mart_customer_360',
            'description': f'Comprehensive {domain_name} analytics'
        })
    
    # Initialize generators
    _, skillaipath = scan_skillaipath()
    
    results = {
        'catalog': catalog_name,
        'bronze_count': 0,
        'silver_count': 0,
        'gold_count': 0,
        'errors': [],
        'warnings': []
    }
    
    # Phase 1: Create Unity Catalog structure
    yield "📦 Phase 1: Creating Unity Catalog structure..."
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
        for schema in ['bronze', 'silver', 'gold']:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema}")
        yield f"✅ Created catalog: {catalog_name}"
    except Exception as e:
        yield f"⚠️ UC setup: {str(e)[:100]}"
        results['warnings'].append(f"UC setup: {str(e)}")
    
    # Phase 2: Load Bronze tables
    yield "\n📥 Phase 2: Loading Bronze tables..."
    for filename, analysis in analysis_results.items():
        try:
            table_name = analysis['table_name']
            df = analysis['df']
            
            # Convert pandas to Spark
            spark_df = spark.createDataFrame(df)
            spark_df.write.mode("overwrite").saveAsTable(f"{catalog_name}.bronze.{table_name}")
            
            results['bronze_count'] += 1
            yield f"✅ {table_name}: {len(df):,} rows"
        except Exception as e:
            yield f"⚠️ {table_name}: {str(e)[:80]}"
            results['errors'].append(f"Bronze {table_name}: {str(e)}")
    
    # Phase 3: Generate Silver layer
    yield "\n⚙️ Phase 3: Generating Silver layer from SkillAIPath patterns..."
    try:
        silver_gen = SilverGenerator(skillaipath, config)
        
        # Generate dimensions
        if config['silver']['dimensions']:
            dimensions = silver_gen.generate_all_dimensions()
            yield f"✅ Generated {len(dimensions)} dimension patterns"
            
            # Create tables
            for dim_config in config['silver']['dimensions']:
                try:
                    source_table = dim_config['source']
                    target_table = dim_config['name']
                    
                    df = spark.table(f"{catalog_name}.bronze.{source_table}")
                    df.write.mode("overwrite").saveAsTable(f"{catalog_name}.silver.{target_table}")
                    
                    results['silver_count'] += 1
                    yield f"✅ {target_table}: {df.count():,} rows"
                except Exception as e:
                    yield f"⚠️ {target_table}: {str(e)[:80]}"
        
        # Generate facts
        if config['silver']['facts']:
            facts = silver_gen.generate_all_facts()
            yield f"✅ Generated {len(facts)} fact patterns"
            
            for fact_config in config['silver']['facts']:
                try:
                    source_table = fact_config['source']
                    target_table = fact_config['name']
                    
                    df = spark.table(f"{catalog_name}.bronze.{source_table}")
                    df.write.mode("overwrite").saveAsTable(f"{catalog_name}.silver.{target_table}")
                    
                    results['silver_count'] += 1
                    yield f"✅ {target_table}: {df.count():,} rows"
                except Exception as e:
                    yield f"⚠️ {target_table}: {str(e)[:80]}"
    
    except Exception as e:
        yield f"⚠️ Silver generation: {str(e)[:100]}"
        results['errors'].append(f"Silver: {str(e)}")
    
    # Phase 4: Generate Gold metrics
    yield "\n📊 Phase 4: Generating Gold metrics..."
    try:
        gold_gen = GoldGenerator(skillaipath, config)
        metrics = gold_gen.generate_all_metrics()
        yield f"✅ Generated {len(metrics)} gold metric patterns"
        results['gold_count'] = len(metrics)
    except Exception as e:
        yield f"⚠️ Gold generation: {str(e)[:100]}"
        results['errors'].append(f"Gold: {str(e)}")
    
    # Final summary
    yield "\n" + "="*60
    yield "🎉 DEPLOYMENT COMPLETE!"
    yield "="*60
    yield f"\n📊 Summary:"
    yield f"   • Catalog: {catalog_name}"
    yield f"   • Bronze tables: {results['bronze_count']}"
    yield f"   • Silver tables: {results['silver_count']}"
    yield f"   • Gold metrics: {results['gold_count']}"
    
    if results['errors']:
        yield f"\n⚠️ Errors: {len(results['errors'])}"
    
    yield f"\n🔍 View in Catalog Explorer: Select '{catalog_name}' → browse schemas"
    
    return results

# === MAIN APP ===

render_header()

st.markdown("---")

# Three-column layout
col1, col2, col3 = st.columns([1, 1, 1])

# === COLUMN 1: Input Section ===
with col1:
    st.markdown("### 📋 Domain Configuration")
    
    domain_name = st.text_input(
        "Domain Name",
        placeholder="e.g., Healthcare Analytics",
        help="Descriptive name for your data platform"
    )
    
    catalog_name = st.text_input(
        "Target Catalog (optional)",
        placeholder="Auto-generated if empty",
        help="Unity Catalog name. Leave empty to auto-generate."
    )
    
    # Auto-generate catalog name if empty
    if not catalog_name and domain_name:
        catalog_name = domain_name.lower().replace(' ', '_').replace('-', '_')
        st.info(f"💡 Auto-generated catalog: `{catalog_name}`")
    
    domain_type = st.selectbox(
        "Domain Type",
        ["Education", "Healthcare", "Retail", "Finance", "Manufacturing", "Other"]
    )
    
    st.markdown("---")
    
    # File uploader
    st.markdown("### 📁 Upload Data Files")
    uploaded_files = st.file_uploader(
        "Upload CSV or Excel files",
        type=['csv', 'xlsx', 'xls'],
        accept_multiple_files=True,
        help="Upload your dimension and fact tables"
    )
    
    if uploaded_files:
        st.success(f"✅ {len(uploaded_files)} file(s) uploaded")
        
        # Store files in session state
        for uploaded_file in uploaded_files:
            st.session_state.uploaded_files[uploaded_file.name] = uploaded_file.read()
        
        # Analyze button
        if st.button("🔍 Analyze Files", type="primary", use_container_width=True):
            with st.spinner("Analyzing files..."):
                st.session_state.analysis_results = analyze_uploaded_files(st.session_state.uploaded_files)
                st.session_state.pattern_suggestions = suggest_skillaipath_patterns(st.session_state.analysis_results)
                st.session_state.analyzed = True
                st.rerun()

# === COLUMN 2: Preview Section ===
with col2:
    st.markdown("### 🔍 Schema Preview")
    
    if st.session_state.analyzed and 'analysis_results' in st.session_state:
        analysis_results = st.session_state.analysis_results
        pattern_suggestions = st.session_state.pattern_suggestions
        
        st.success(f"✅ Analyzed {len(analysis_results)} table(s)")
        
        # Show detected tables
        for filename, analysis in analysis_results.items():
            with st.expander(f"📊 {analysis['table_name']} ({analysis['table_type']})", expanded=False):
                st.write(f"**Rows:** {analysis['rows']:,}")
                st.write(f"**Columns:** {analysis['columns']}")
                st.write(f"**Suggested Pattern:** `{pattern_suggestions[filename]}`")
                
                # Schema preview
                st.write("**Schema:**")
                schema_df = pd.DataFrame([
                    {'Column': col, 'Type': str(dtype)}
                    for col, dtype in analysis['schema'].items()
                ])
                st.dataframe(schema_df, hide_index=True)
                
                # Data preview
                st.write("**Sample Data:**")
                st.dataframe(analysis['sample'], hide_index=True)
        
        # Pattern mappings (editable)
        st.markdown("---")
        st.markdown("**Pattern Mappings** (edit if needed):")
        
        if 'pattern_mappings' not in st.session_state:
            st.session_state.pattern_mappings = pattern_suggestions.copy()
        
        for filename in analysis_results.keys():
            st.session_state.pattern_mappings[filename] = st.text_input(
                f"{filename}",
                value=st.session_state.pattern_mappings[filename],
                key=f"pattern_{filename}"
            )
    else:
        st.info("👆 Upload files and click 'Analyze Files' to see schema preview")

# === COLUMN 3: Deployment Section ===
with col3:
    st.markdown("### 🚀 Deployment")
    
    if st.session_state.analyzed:
        # Deploy button
        deploy_disabled = not (domain_name and catalog_name and len(st.session_state.uploaded_files) > 0)
        
        if st.button(
            "🚀 Deploy Domain",
            type="primary",
            use_container_width=True,
            disabled=deploy_disabled or st.session_state.deploying
        ):
            st.session_state.deploying = True
            st.session_state.deployment_complete = False
            st.rerun()
        
        # Show deployment progress
        if st.session_state.deploying:
            st.markdown("---")
            st.markdown("**📊 Deployment Progress:**")
            
            log_container = st.container()
            
            with log_container:
                deployment_logs = st.empty()
                
                # Execute deployment
                log_text = ""
                try:
                    for log_line in deploy_domain(
                        domain_name,
                        catalog_name,
                        domain_type,
                        st.session_state.analysis_results,
                        st.session_state.pattern_mappings
                    ):
                        log_text += log_line + "\n"
                        deployment_logs.text_area(
                            "Deployment Logs",
                            log_text,
                            height=400,
                            key=f"logs_{datetime.now().timestamp()}"
                        )
                        time.sleep(0.1)  # Small delay for visual effect
                    
                    st.session_state.deploying = False
                    st.session_state.deployment_complete = True
                    st.session_state.deployment_result = {
                        'catalog': catalog_name,
                        'timestamp': datetime.now()
                    }
                    
                except Exception as e:
                    st.error(f"❌ Deployment failed: {str(e)}")
                    st.session_state.deploying = False
        
        # Show success message
        if st.session_state.deployment_complete and st.session_state.deployment_result:
            st.success("✅ Deployment Complete!")
            
            result = st.session_state.deployment_result
            st.markdown(f"**Catalog:** `{result['catalog']}`")
            st.markdown(f"**Deployed at:** {result['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Link to catalog
            st.markdown(f"\n🔗 **[Open Catalog Explorer](/#catalog)**")
            
            # Reset button
            if st.button("🔄 Deploy Another Domain"):
                st.session_state.analyzed = False
                st.session_state.deploying = False
                st.session_state.deployment_complete = False
                st.session_state.uploaded_files = {}
                st.rerun()
    else:
        st.info("👆 Analyze your files first to enable deployment")

# Footer
st.markdown("---")
st.markdown(
    """<div style='text-align: center; color: #666;'>
    <small>Powered by Domain Templates V2 • SkillAIPath Patterns • Unity Catalog</small>
    </div>""",
    unsafe_allow_html=True
)