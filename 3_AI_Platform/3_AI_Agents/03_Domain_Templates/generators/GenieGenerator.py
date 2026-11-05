'''Genie space generator'''
import yaml
class GenieGenerator:
    def __init__(self, config_path):
        with open(config_path) as f:
            self.config = yaml.safe_load(f)
    def create_space(self):
        tables = [s['name'] for s in self.config.get('bronze', {}).get('sources', [])]
        return {'tables': tables}
