"""
JOB GENERATOR - Production Grade
Creates orchestration jobs replicating SkillAIPath patterns
Handles dependencies, scheduling, error handling
"""
from pathlib import Path
from typing import Dict, List

class JobGenerator:
    """Generate orchestration jobs"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.catalog = config['domain']['catalog']
        self.domain = config['domain']['name']
    
    def generate_orchestration_job(self) -> Dict:
        """Generate main orchestration job"""
        
        schedule = self.config.get('scheduling', {})
        
        job_config = {
            'name': f"{self.domain} - Data Pipeline",
            'tasks': self._generate_tasks(),
            'schedule': {
                'quartz_cron_expression': self._convert_to_cron(schedule.get('frequency', 'daily')),
                'timezone_id': 'UTC'
            },
            'email_notifications': {
                'on_failure': schedule.get('alert_email', [])
            },
            'max_concurrent_runs': 1,
            'timeout_seconds': 3600
        }
        
        return job_config
    
    def _generate_tasks(self) -> List[Dict]:
        """Generate task dependency graph"""
        tasks = []
        
        # Task 1: Bronze ingestion
        tasks.append({
            'task_key': 'bronze_ingestion',
            'notebook_task': {
                'notebook_path': f'/Domain_Templates/{self.domain}/bronze/ingest_all.py'
            }
        })
        
        # Task 2: Silver dimensions (depends on bronze)
        tasks.append({
            'task_key': 'silver_dimensions',
            'depends_on': [{'task_key': 'bronze_ingestion'}],
            'notebook_task': {
                'notebook_path': f'/Domain_Templates/{self.domain}/silver/dimensions.py'
            }
        })
        
        # Task 3: Silver facts (depends on dimensions)
        tasks.append({
            'task_key': 'silver_facts',
            'depends_on': [{'task_key': 'silver_dimensions'}],
            'notebook_task': {
                'notebook_path': f'/Domain_Templates/{self.domain}/silver/facts.py'
            }
        })
        
        # Task 4: Gold metrics (depends on facts)
        tasks.append({
            'task_key': 'gold_metrics',
            'depends_on': [{'task_key': 'silver_facts'}],
            'notebook_task': {
                'notebook_path': f'/Domain_Templates/{self.domain}/gold/metrics.py'
            }
        })
        
        # Task 5: Data quality checks (runs after all)
        tasks.append({
            'task_key': 'data_quality',
            'depends_on': [{'task_key': 'gold_metrics'}],
            'notebook_task': {
                'notebook_path': f'/Domain_Templates/{self.domain}/monitoring/quality_checks.py'
            }
        })
        
        return tasks
    
    def _convert_to_cron(self, frequency: str) -> str:
        """Convert frequency to cron expression"""
        cron_map = {
            'hourly': '0 0 * * * ?',
            'daily': '0 0 0 * * ?',
            'phase-based': '0 0 0 ? * MON'
        }
        return cron_map.get(frequency, '0 0 0 * * ?')
    
    def save_job_config(self, output_path: Path):
        """Save job configuration"""
        import json
        
        job = self.generate_orchestration_job()
        
        with open(output_path, 'w') as f:
            json.dump(job, f, indent=2)
        
        print(f"   ✅ Saved job config: {output_path.name}")

if __name__ == '__main__':
    print("Job Generator Ready")
