"""
Dashboard Generator - POC
Automatically generates Lakeview dashboards from YAML configuration

Pattern: YAML Config → Python Generator → Databricks API → Live Dashboard

Usage:
    python dashboard_generator.py --config ../configs/config_poc.yaml
"""

import yaml
import sys
from pathlib import Path
from typing import Dict, List, Any


class DashboardGenerator:
    """Generates Lakeview dashboards from YAML configuration"""
    
    def __init__(self, config_path: str):
        self.config_path = Path(config_path)
        self.config = self._load_config()
        
    def _load_config(self) -> Dict:
        """Load and parse YAML configuration"""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_path}")
        
        with open(self.config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        # Validate required sections
        if 'dashboards' not in config:
            raise ValueError("Config must contain 'dashboards' section")
        
        return config
    
    def generate_all(self) -> List[str]:
        """Generate all dashboards defined in config"""
        dashboard_ids = []
        
        for dashboard_config in self.config['dashboards']:
            dashboard_id = self._generate_dashboard(dashboard_config)
            dashboard_ids.append(dashboard_id)
        
        return dashboard_ids
    
    def _generate_dashboard(self, dashboard_config: Dict) -> str:
        """
        Generate a single dashboard from config
        
        Strategy:
        1. Create blank dashboard asset
        2. Hand off to dashboard page agent with continueMessage
        3. Dashboard page agent adds widgets using renderChart or native tools
        """
        dashboard_name = dashboard_config['name']
        description = dashboard_config.get('description', '')
        widgets = dashboard_config.get('widgets', [])
        table = dashboard_config.get('table', None)
        
        print(f"\n[Dashboard Generator] Creating: {dashboard_name}")
        print(f"  Description: {description}")
        print(f"  Widgets: {len(widgets)}")
        print(f"  Data source: {table}")
        
        # Build widget specifications for continueMessage
        widget_specs = self._build_widget_specs(widgets, table)
        
        # Create continue message for dashboard page
        continue_message = self._build_continue_message(
            dashboard_name, description, widget_specs, table
        )
        
        print(f"\n[Dashboard Generator] Instructions prepared")
        print(f"  Continue message length: {len(continue_message)} chars")
        print(f"\n📋 NEXT STEP: Use Genie Code Assistant to:")
        print(f"  1. Create dashboard: '{dashboard_name}'")
        print(f"  2. Add {len(widgets)} widgets using these specs:")
        print(f"\n{continue_message}")
        print(f"\n" + "="*80)
        
        # Return placeholder ID (in real implementation, would call createAsset)
        return f"dashboard_{dashboard_name.lower().replace(' ', '_')}"
    
    def _build_widget_specs(self, widgets: List[Dict], table: str) -> List[Dict]:
        """Build widget specifications for dashboard page"""
        specs = []
        
        for i, widget in enumerate(widgets):
            widget_type = widget['type']
            title = widget['title']
            position = widget.get('position', {})
            
            spec = {
                'type': widget_type,
                'title': title,
                'position': position
            }
            
            if widget_type == 'counter':
                spec['metric'] = widget['metric']
                if 'filter' in widget:
                    spec['filter'] = widget['filter']
            
            elif widget_type == 'bar':
                spec['x_axis'] = widget['x_axis']
                spec['y_axis'] = widget['y_axis']
            
            elif widget_type == 'line':
                spec['x_axis'] = widget['x_axis']
                spec['y_axis'] = widget['y_axis']
                if 'color_by' in widget:
                    spec['color_by'] = widget['color_by']
            
            specs.append(spec)
        
        return specs
    
    def _build_continue_message(self, name: str, description: str, 
                                 widget_specs: List[Dict], table: str) -> str:
        """Build detailed instruction message for dashboard page agent"""
        message = f"""Create a Lakeview dashboard with these specifications:

**Dashboard Name:** {name}
**Description:** {description}
**Data Source:** {table}

**Widgets to Create:**
"""
        
        for i, spec in enumerate(widget_specs, 1):
            message += f"\n{i}. **{spec['title']}** ({spec['type']})"
            
            if spec['type'] == 'counter':
                message += f"\n   - Metric: `{spec['metric']}`"
                if 'filter' in spec:
                    message += f"\n   - Filter: `{spec['filter']}`"
            
            elif spec['type'] == 'bar':
                message += f"\n   - X-axis: `{spec['x_axis']}`"
                message += f"\n   - Y-axis: `{spec['y_axis']}`"
            
            elif spec['type'] == 'line':
                message += f"\n   - X-axis: `{spec['x_axis']}`"
                message += f"\n   - Y-axis: `{spec['y_axis']}`"
                if 'color_by' in spec:
                    message += f"\n   - Color by: `{spec['color_by']}`"
            
            if 'position' in spec and spec['position']:
                pos = spec['position']
                message += f"\n   - Position: x={pos.get('x',0)}, y={pos.get('y',0)}, width={pos.get('width',2)}, height={pos.get('height',2)}"
        
        message += "\n\nPlease create this dashboard and add all widgets as specified."
        
        return message


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Generate Lakeview dashboards from YAML configuration'
    )
    parser.add_argument(
        '--config',
        required=True,
        help='Path to YAML configuration file'
    )
    
    args = parser.parse_args()
    
    try:
        generator = DashboardGenerator(args.config)
        
        print("="*80)
        print("DASHBOARD GENERATOR - POC")
        print("="*80)
        print(f"\nLoading configuration: {args.config}")
        print(f"Domain: {generator.config['domain']['name']}")
        print(f"Catalog: {generator.config['domain']['catalog']}")
        
        dashboard_ids = generator.generate_all()
        
        print("\n" + "="*80)
        print("✅ POC COMPLETE!")
        print("="*80)
        print(f"\nDashboards prepared: {len(dashboard_ids)}")
        print("\nNext steps:")
        print("  1. Copy the continue message above")
        print("  2. Ask Genie Code Assistant to create the dashboard")
        print("  3. Verify widgets render correctly")
        print("\nIf this works, you've validated the pattern!")
        print("Then replicate for 9 other generators.")
        
    except Exception as e:
        print(f"\n❌ Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
