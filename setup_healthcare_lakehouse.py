#!/usr/bin/env python3
"""
Healthcare Data Lakehouse - Complete Setup Script
Deploy the entire healthcare data lakehouse infrastructure and pipelines.
"""

import os
import sys
import argparse
import subprocess
import json
import time
from datetime import datetime
import structlog

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()

class HealthcareLakehouseSetup:
    """Complete setup for healthcare data lakehouse"""
    
    def __init__(self, project_id: str, region: str = "us-central1", data_engineer_email: str = None):
        self.project_id = project_id
        self.region = region
        self.data_engineer_email = data_engineer_email or f"data-engineer@{project_id}.iam.gserviceaccount.com"
        self.logger = structlog.get_logger()
        
        # Setup status tracking
        self.setup_status = {
            "infrastructure": False,
            "dataflow_pipelines": False,
            "dbt_models": False,
            "airflow_dags": False,
            "monitoring": False,
            "compliance": False
        }
    
    def check_prerequisites(self):
        """Check if all prerequisites are met"""
        self.logger.info("Checking prerequisites...")
        
        # Check if gcloud is installed
        try:
            subprocess.run(["gcloud", "--version"], check=True, capture_output=True)
            self.logger.info("✅ gcloud CLI is installed")
        except (subprocess.CalledProcessError, FileNotFoundError):
            self.logger.error("❌ gcloud CLI is not installed")
            return False
        
        # Check if terraform is installed
        try:
            subprocess.run(["terraform", "--version"], check=True, capture_output=True)
            self.logger.info("✅ Terraform is installed")
        except (subprocess.CalledProcessError, FileNotFoundError):
            self.logger.error("❌ Terraform is not installed")
            return False
        
        # Check if dbt is installed
        try:
            subprocess.run(["dbt", "--version"], check=True, capture_output=True)
            self.logger.info("✅ dbt is installed")
        except (subprocess.CalledProcessError, FileNotFoundError):
            self.logger.error("❌ dbt is not installed")
            return False
        
        # Check if authenticated with gcloud
        try:
            result = subprocess.run(["gcloud", "auth", "list", "--filter=status:ACTIVE", "--format=value(account)"], 
                                  check=True, capture_output=True, text=True)
            if result.stdout.strip():
                self.logger.info(f"✅ Authenticated as: {result.stdout.strip()}")
            else:
                self.logger.error("❌ Not authenticated with gcloud")
                return False
        except subprocess.CalledProcessError:
            self.logger.error("❌ Failed to check gcloud authentication")
            return False
        
        # Check if project exists and is accessible
        try:
            subprocess.run(["gcloud", "config", "set", "project", self.project_id], check=True, capture_output=True)
            self.logger.info(f"✅ Project {self.project_id} is accessible")
        except subprocess.CalledProcessError:
            self.logger.error(f"❌ Project {self.project_id} is not accessible")
            return False
        
        self.logger.info("✅ All prerequisites are met")
        return True
    
    def deploy_infrastructure(self):
        """Deploy infrastructure using Terraform"""
        self.logger.info("Deploying infrastructure with Terraform...")
        
        try:
            # Navigate to terraform directory
            os.chdir("terraform")
            
            # Initialize Terraform
            self.logger.info("Initializing Terraform...")
            subprocess.run(["terraform", "init"], check=True)
            
            # Plan Terraform deployment
            self.logger.info("Planning Terraform deployment...")
            plan_cmd = [
                "terraform", "plan",
                f"-var=project_id={self.project_id}",
                f"-var=region={self.region}",
                f"-var=data_engineer_email={self.data_engineer_email}",
                "-out=tfplan"
            ]
            subprocess.run(plan_cmd, check=True)
            
            # Apply Terraform deployment
            self.logger.info("Applying Terraform deployment...")
            subprocess.run(["terraform", "apply", "tfplan"], check=True)
            
            # Get outputs
            self.logger.info("Getting Terraform outputs...")
            result = subprocess.run(["terraform", "output", "-json"], check=True, capture_output=True, text=True)
            outputs = json.loads(result.stdout)
            
            # Save outputs for later use
            with open("../terraform_outputs.json", "w") as f:
                json.dump(outputs, f, indent=2)
            
            self.logger.info("✅ Infrastructure deployed successfully")
            self.setup_status["infrastructure"] = True
            
            # Return to original directory
            os.chdir("..")
            
            return outputs
            
        except subprocess.CalledProcessError as e:
            self.logger.error(f"❌ Infrastructure deployment failed: {e}")
            os.chdir("..")
            return None
    
    def deploy_dataflow_pipelines(self):
        """Deploy Dataflow pipelines"""
        self.logger.info("Deploying Dataflow pipelines...")
        
        try:
            # Navigate to dataflow directory
            os.chdir("dataflow")
            
            # Install dependencies
            self.logger.info("Installing Dataflow dependencies...")
            subprocess.run(["pip", "install", "-r", "requirements.txt"], check=True)
            
            # Deploy pipelines
            self.logger.info("Deploying pipeline templates...")
            deploy_cmd = [
                "python", "deploy_pipelines.py",
                "--project-id", self.project_id,
                "--region", self.region,
                "--action", "deploy"
            ]
            subprocess.run(deploy_cmd, check=True)
            
            self.logger.info("✅ Dataflow pipelines deployed successfully")
            self.setup_status["dataflow_pipelines"] = True
            
            # Return to original directory
            os.chdir("..")
            
        except subprocess.CalledProcessError as e:
            self.logger.error(f"❌ Dataflow pipeline deployment failed: {e}")
            os.chdir("..")
            return False
        
        return True
    
    def setup_dbt_models(self):
        """Set up dbt models"""
        self.logger.info("Setting up dbt models...")
        
        try:
            # Navigate to dbt directory
            os.chdir("dbt")
            
            # Install dbt dependencies
            self.logger.info("Installing dbt dependencies...")
            subprocess.run(["dbt", "deps"], check=True)
            
            # Run dbt models
            self.logger.info("Running dbt models...")
            subprocess.run(["dbt", "run"], check=True)
            
            # Run dbt tests
            self.logger.info("Running dbt tests...")
            subprocess.run(["dbt", "test"], check=True)
            
            # Generate dbt docs
            self.logger.info("Generating dbt documentation...")
            subprocess.run(["dbt", "docs", "generate"], check=True)
            
            self.logger.info("✅ dbt models set up successfully")
            self.setup_status["dbt_models"] = True
            
            # Return to original directory
            os.chdir("..")
            
        except subprocess.CalledProcessError as e:
            self.logger.error(f"❌ dbt setup failed: {e}")
            os.chdir("..")
            return False
        
        return True
    
    def deploy_airflow_dags(self):
        """Deploy Airflow DAGs to Cloud Composer"""
        self.logger.info("Deploying Airflow DAGs...")
        
        try:
            # Get Composer environment details
            result = subprocess.run([
                "gcloud", "composer", "environments", "list",
                f"--project={self.project_id}",
                f"--location={self.region}",
                "--format=value(name)"
            ], check=True, capture_output=True, text=True)
            
            composer_env = result.stdout.strip()
            if not composer_env:
                self.logger.warning("⚠️ No Cloud Composer environment found, skipping DAG deployment")
                return True
            
            # Upload DAGs to Composer
            self.logger.info(f"Uploading DAGs to Composer environment: {composer_env}")
            subprocess.run([
                "gcloud", "composer", "environments", "storage", "dags", "import",
                f"--environment={composer_env}",
                f"--location={self.region}",
                f"--project={self.project_id}",
                "--source=airflow/dags/"
            ], check=True)
            
            self.logger.info("✅ Airflow DAGs deployed successfully")
            self.setup_status["airflow_dags"] = True
            
        except subprocess.CalledProcessError as e:
            self.logger.error(f"❌ Airflow DAG deployment failed: {e}")
            return False
        
        return True
    
    def setup_monitoring(self):
        """Set up monitoring and alerting"""
        self.logger.info("Setting up monitoring and alerting...")
        
        try:
            # Create monitoring dashboard
            self.logger.info("Creating monitoring dashboard...")
            dashboard_file = "monitoring/dashboards/healthcare_monitoring_dashboard.json"
            
            # Replace placeholders in dashboard
            with open(dashboard_file, "r") as f:
                dashboard_content = f.read()
            
            dashboard_content = dashboard_content.replace("${PROJECT_ID}", self.project_id)
            
            with open(dashboard_file, "w") as f:
                f.write(dashboard_content)
            
            # Create dashboard using gcloud
            subprocess.run([
                "gcloud", "monitoring", "dashboards", "create",
                f"--project={self.project_id}",
                f"--config-from-file={dashboard_file}"
            ], check=True)
            
            self.logger.info("✅ Monitoring dashboard created successfully")
            self.setup_status["monitoring"] = True
            
        except subprocess.CalledProcessError as e:
            self.logger.error(f"❌ Monitoring setup failed: {e}")
            return False
        
        return True
    
    def run_compliance_checks(self):
        """Run HIPAA compliance checks"""
        self.logger.info("Running HIPAA compliance checks...")
        
        try:
            # Navigate to security directory
            os.chdir("security/compliance")
            
            # Run compliance checker
            subprocess.run([
                "python", "hipaa_compliance_checker.py",
                "--project-id", self.project_id,
                "--region", self.region,
                "--check", "all"
            ], check=True)
            
            self.logger.info("✅ HIPAA compliance checks completed")
            self.setup_status["compliance"] = True
            
            # Return to original directory
            os.chdir("../..")
            
        except subprocess.CalledProcessError as e:
            self.logger.error(f"❌ Compliance checks failed: {e}")
            os.chdir("../..")
            return False
        
        return True
    
    def start_data_ingestion(self):
        """Start data ingestion for demo"""
        self.logger.info("Starting data ingestion for demo...")
        
        try:
            # Navigate to ingestion directory
            os.chdir("ingestion")
            
            # Install dependencies
            subprocess.run(["pip", "install", "-r", "requirements.txt"], check=True)
            
            # Start ingestion in background
            self.logger.info("Starting data ingestion in demo mode...")
            process = subprocess.Popen([
                "python", "start_ingestion.py",
                "--project-id", self.project_id,
                "--region", self.region,
                "--mode", "demo",
                "--duration", "30"  # Run for 30 minutes
            ])
            
            self.logger.info(f"✅ Data ingestion started (PID: {process.pid})")
            return process
            
        except subprocess.CalledProcessError as e:
            self.logger.error(f"❌ Data ingestion failed to start: {e}")
            os.chdir("..")
            return None
    
    def generate_setup_report(self):
        """Generate setup completion report"""
        self.logger.info("Generating setup report...")
        
        report = {
            "project_id": self.project_id,
            "region": self.region,
            "setup_date": datetime.now().isoformat(),
            "setup_status": self.setup_status,
            "overall_status": "COMPLETE" if all(self.setup_status.values()) else "PARTIAL",
            "next_steps": []
        }
        
        # Add next steps based on what's completed
        if self.setup_status["infrastructure"]:
            report["next_steps"].append("Infrastructure is ready - you can access BigQuery, GCS, and Pub/Sub")
        
        if self.setup_status["dataflow_pipelines"]:
            report["next_steps"].append("Dataflow pipelines are deployed - data processing is active")
        
        if self.setup_status["dbt_models"]:
            report["next_steps"].append("dbt models are set up - analytics-ready tables are available")
        
        if self.setup_status["airflow_dags"]:
            report["next_steps"].append("Airflow DAGs are deployed - orchestration is active")
        
        if self.setup_status["monitoring"]:
            report["next_steps"].append("Monitoring is set up - view dashboards in Cloud Monitoring")
        
        if self.setup_status["compliance"]:
            report["next_steps"].append("Compliance checks passed - HIPAA requirements are met")
        
        # Save report
        with open("setup_report.json", "w") as f:
            json.dump(report, f, indent=2, default=str)
        
        self.logger.info("✅ Setup report generated: setup_report.json")
        return report
    
    def run_complete_setup(self):
        """Run the complete setup process"""
        self.logger.info("Starting complete healthcare data lakehouse setup...")
        
        # Check prerequisites
        if not self.check_prerequisites():
            self.logger.error("❌ Prerequisites not met. Please fix the issues above and try again.")
            return False
        
        # Deploy infrastructure
        outputs = self.deploy_infrastructure()
        if not outputs:
            self.logger.error("❌ Infrastructure deployment failed")
            return False
        
        # Wait for infrastructure to be ready
        self.logger.info("Waiting for infrastructure to be ready...")
        time.sleep(60)
        
        # Deploy Dataflow pipelines
        if not self.deploy_dataflow_pipelines():
            self.logger.error("❌ Dataflow pipeline deployment failed")
            return False
        
        # Set up dbt models
        if not self.setup_dbt_models():
            self.logger.error("❌ dbt setup failed")
            return False
        
        # Deploy Airflow DAGs
        if not self.deploy_airflow_dags():
            self.logger.error("❌ Airflow DAG deployment failed")
            return False
        
        # Set up monitoring
        if not self.setup_monitoring():
            self.logger.error("❌ Monitoring setup failed")
            return False
        
        # Run compliance checks
        if not self.run_compliance_checks():
            self.logger.error("❌ Compliance checks failed")
            return False
        
        # Generate setup report
        report = self.generate_setup_report()
        
        # Start data ingestion for demo
        ingestion_process = self.start_data_ingestion()
        
        # Print final status
        self.logger.info("🎉 Healthcare Data Lakehouse Setup Complete!")
        self.logger.info(f"Project ID: {self.project_id}")
        self.logger.info(f"Region: {self.region}")
        self.logger.info(f"Overall Status: {report['overall_status']}")
        
        if ingestion_process:
            self.logger.info(f"Data ingestion is running (PID: {ingestion_process.pid})")
            self.logger.info("Demo data is being generated and processed")
        
        self.logger.info("\n📋 Next Steps:")
        for step in report["next_steps"]:
            self.logger.info(f"  - {step}")
        
        self.logger.info("\n🔗 Useful Links:")
        self.logger.info(f"  - BigQuery: https://console.cloud.google.com/bigquery?project={self.project_id}")
        self.logger.info(f"  - Cloud Monitoring: https://console.cloud.google.com/monitoring?project={self.project_id}")
        self.logger.info(f"  - Dataflow: https://console.cloud.google.com/dataflow?project={self.project_id}")
        self.logger.info(f"  - Cloud Composer: https://console.cloud.google.com/composer?project={self.project_id}")
        
        return True

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Healthcare Data Lakehouse Setup")
    parser.add_argument("--project-id", required=True, help="GCP Project ID")
    parser.add_argument("--region", default="us-central1", help="GCP Region")
    parser.add_argument("--data-engineer-email", help="Data engineer email for BigQuery access")
    parser.add_argument("--skip-prerequisites", action="store_true", help="Skip prerequisite checks")
    parser.add_argument("--components", nargs="+", 
                       choices=["infrastructure", "dataflow", "dbt", "airflow", "monitoring", "compliance"],
                       help="Specific components to deploy")
    
    args = parser.parse_args()
    
    setup = HealthcareLakehouseSetup(args.project_id, args.region, args.data_engineer_email)
    
    try:
        if args.components:
            # Deploy specific components
            for component in args.components:
                if component == "infrastructure":
                    setup.deploy_infrastructure()
                elif component == "dataflow":
                    setup.deploy_dataflow_pipelines()
                elif component == "dbt":
                    setup.setup_dbt_models()
                elif component == "airflow":
                    setup.deploy_airflow_dags()
                elif component == "monitoring":
                    setup.setup_monitoring()
                elif component == "compliance":
                    setup.run_compliance_checks()
        else:
            # Run complete setup
            setup.run_complete_setup()
        
    except KeyboardInterrupt:
        logger.info("Setup interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error("Setup failed", error=str(e))
        sys.exit(1)

if __name__ == "__main__":
    main() 