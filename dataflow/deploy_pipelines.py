#!/usr/bin/env python3
"""
Healthcare Data Lakehouse - Dataflow Pipeline Deployment
Deploy Apache Beam pipelines to Google Cloud Dataflow.
"""

import os
import sys
import argparse
import subprocess
import json
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

class DataflowPipelineDeployer:
    """Deploy Dataflow pipelines to Google Cloud"""
    
    def __init__(self, project_id: str, region: str = "us-central1"):
        self.project_id = project_id
        self.region = region
        self.logger = structlog.get_logger()
        
        # Pipeline configuration
        self.pipelines = {
            "healthcare-etl": {
                "script": "pipelines/healthcare_etl_pipeline.py",
                "job_name": "healthcare-etl-pipeline",
                "description": "Healthcare ETL pipeline for processing data from Pub/Sub to BigQuery",
                "streaming": True,
                "machine_type": "n1-standard-2",
                "max_workers": 10,
                "worker_disk_size_gb": 50
            }
        }
        
        # GCS paths
        self.gcs_paths = {
            "temp_location": f"gs://{project_id}-healthcare-processed/temp",
            "staging_location": f"gs://{project_id}-healthcare-processed/staging",
            "template_location": f"gs://{project_id}-healthcare-processed/templates"
        }
        
        # Pub/Sub topics
        self.pubsub_topics = {
            "iot_data": f"projects/{project_id}/topics/healthcare-iot-data",
            "claims_data": f"projects/{project_id}/topics/healthcare-claims-data",
            "ehr_data": f"projects/{project_id}/topics/healthcare-ehr-data"
        }
        
        # BigQuery datasets
        self.bigquery_datasets = {
            "raw": f"{project_id}.healthcare_raw",
            "processed": f"{project_id}.healthcare_processed",
            "curated": f"{project_id}.healthcare_curated"
        }
    
    def create_gcs_directories(self):
        """Create necessary GCS directories"""
        self.logger.info("Creating GCS directories")
        
        for path_name, path in self.gcs_paths.items():
            try:
                # Create directory using gsutil
                cmd = ["gsutil", "mb", "-p", self.project_id, "-c", "STANDARD", "-l", self.region, path]
                subprocess.run(cmd, check=True, capture_output=True)
                self.logger.info(f"Created GCS directory: {path}")
            except subprocess.CalledProcessError as e:
                if "already exists" in e.stderr.decode():
                    self.logger.info(f"GCS directory already exists: {path}")
                else:
                    self.logger.error(f"Failed to create GCS directory: {path}", error=e.stderr.decode())
                    raise
    
    def deploy_pipeline_template(self, pipeline_name: str):
        """Deploy a pipeline as a template"""
        if pipeline_name not in self.pipelines:
            raise ValueError(f"Unknown pipeline: {pipeline_name}")
        
        pipeline_config = self.pipelines[pipeline_name]
        template_path = f"{self.gcs_paths['template_location']}/{pipeline_name}-template"
        
        self.logger.info(f"Deploying pipeline template: {pipeline_name}")
        
        # Build command
        cmd = [
            "python", pipeline_config["script"],
            f"--project={self.project_id}",
            f"--region={self.region}",
            f"--temp_location={self.gcs_paths['temp_location']}",
            f"--staging_location={self.gcs_paths['staging_location']}",
            f"--template_location={template_path}",
            f"--runner=DataflowRunner",
            f"--job_name={pipeline_config['job_name']}",
            f"--machine_type={pipeline_config['machine_type']}",
            f"--max_workers={pipeline_config['max_workers']}",
            f"--worker_disk_size_gb={pipeline_config['worker_disk_size_gb']}",
            "--setup_file=./setup.py",
            "--requirements_file=./requirements.txt"
        ]
        
        # Add streaming flag if needed
        if pipeline_config.get("streaming", False):
            cmd.append("--streaming")
        
        # Add pipeline-specific arguments
        if pipeline_name == "healthcare-etl":
            cmd.extend([
                f"--input-topic={self.pubsub_topics['iot_data']}",  # Can be configured per job
                f"--output-dataset={self.bigquery_datasets['processed']}"
            ])
        
        try:
            self.logger.info(f"Running command: {' '.join(cmd)}")
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            self.logger.info(f"Pipeline template deployed successfully: {template_path}")
            return template_path
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Failed to deploy pipeline template: {pipeline_name}", 
                            error=e.stderr, stdout=e.stdout)
            raise
    
    def run_pipeline_job(self, pipeline_name: str, job_name: str = None, **kwargs):
        """Run a pipeline job from template"""
        if pipeline_name not in self.pipelines:
            raise ValueError(f"Unknown pipeline: {pipeline_name}")
        
        pipeline_config = self.pipelines[pipeline_name]
        template_path = f"{self.gcs_paths['template_location']}/{pipeline_name}-template"
        
        if job_name is None:
            job_name = f"{pipeline_config['job_name']}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        
        self.logger.info(f"Running pipeline job: {job_name}")
        
        # Build command
        cmd = [
            "gcloud", "dataflow", "jobs", "run", job_name,
            f"--project={self.project_id}",
            f"--region={self.region}",
            f"--gcs-location={template_path}",
            f"--staging-location={self.gcs_paths['staging_location']}",
            f"--temp-location={self.gcs_paths['temp_location']}",
            f"--machine-type={pipeline_config['machine_type']}",
            f"--max-workers={pipeline_config['max_workers']}",
            f"--worker-disk-size-gb={pipeline_config['worker_disk_size_gb']}"
        ]
        
        # Add pipeline-specific parameters
        if pipeline_name == "healthcare-etl":
            # Add parameters for different data sources
            cmd.extend([
                f"--parameters=input-topic={kwargs.get('input_topic', self.pubsub_topics['iot_data'])}",
                f"--parameters=output-dataset={kwargs.get('output_dataset', self.bigquery_datasets['processed'])}"
            ])
        
        try:
            self.logger.info(f"Running command: {' '.join(cmd)}")
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            self.logger.info(f"Pipeline job started successfully: {job_name}")
            return job_name
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Failed to run pipeline job: {job_name}", 
                            error=e.stderr, stdout=e.stdout)
            raise
    
    def deploy_all_pipelines(self):
        """Deploy all pipeline templates"""
        self.logger.info("Deploying all pipeline templates")
        
        deployed_templates = {}
        
        for pipeline_name in self.pipelines.keys():
            try:
                template_path = self.deploy_pipeline_template(pipeline_name)
                deployed_templates[pipeline_name] = template_path
            except Exception as e:
                self.logger.error(f"Failed to deploy pipeline: {pipeline_name}", error=str(e))
                raise
        
        self.logger.info("All pipeline templates deployed successfully", 
                        templates=deployed_templates)
        return deployed_templates
    
    def start_streaming_jobs(self):
        """Start streaming pipeline jobs for all data sources"""
        self.logger.info("Starting streaming pipeline jobs")
        
        jobs = {}
        
        # Start job for IoT data
        try:
            iot_job = self.run_pipeline_job(
                "healthcare-etl",
                job_name="healthcare-etl-iot",
                input_topic=self.pubsub_topics["iot_data"],
                output_dataset=self.bigquery_datasets["processed"]
            )
            jobs["iot"] = iot_job
        except Exception as e:
            self.logger.error("Failed to start IoT streaming job", error=str(e))
        
        # Start job for claims data
        try:
            claims_job = self.run_pipeline_job(
                "healthcare-etl",
                job_name="healthcare-etl-claims",
                input_topic=self.pubsub_topics["claims_data"],
                output_dataset=self.bigquery_datasets["processed"]
            )
            jobs["claims"] = claims_job
        except Exception as e:
            self.logger.error("Failed to start claims streaming job", error=str(e))
        
        # Start job for EHR data
        try:
            ehr_job = self.run_pipeline_job(
                "healthcare-etl",
                job_name="healthcare-etl-ehr",
                input_topic=self.pubsub_topics["ehr_data"],
                output_dataset=self.bigquery_datasets["processed"]
            )
            jobs["ehr"] = ehr_job
        except Exception as e:
            self.logger.error("Failed to start EHR streaming job", error=str(e))
        
        self.logger.info("Streaming jobs started", jobs=jobs)
        return jobs
    
    def monitor_jobs(self, job_names: list):
        """Monitor running Dataflow jobs"""
        self.logger.info("Monitoring Dataflow jobs", job_names=job_names)
        
        for job_name in job_names:
            try:
                cmd = [
                    "gcloud", "dataflow", "jobs", "describe", job_name,
                    f"--project={self.project_id}",
                    f"--region={self.region}",
                    "--format=json"
                ]
                
                result = subprocess.run(cmd, check=True, capture_output=True, text=True)
                job_info = json.loads(result.stdout)
                
                state = job_info.get("currentState", "UNKNOWN")
                self.logger.info(f"Job status: {job_name}", state=state)
                
                if state == "JOB_STATE_FAILED":
                    self.logger.error(f"Job failed: {job_name}", job_info=job_info)
                
            except Exception as e:
                self.logger.error(f"Failed to monitor job: {job_name}", error=str(e))
    
    def cleanup_jobs(self, job_names: list):
        """Cancel running Dataflow jobs"""
        self.logger.info("Cleaning up Dataflow jobs", job_names=job_names)
        
        for job_name in job_names:
            try:
                cmd = [
                    "gcloud", "dataflow", "jobs", "cancel", job_name,
                    f"--project={self.project_id}",
                    f"--region={self.region}"
                ]
                
                subprocess.run(cmd, check=True, capture_output=True)
                self.logger.info(f"Job cancelled: {job_name}")
                
            except Exception as e:
                self.logger.error(f"Failed to cancel job: {job_name}", error=str(e))

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Healthcare Data Lakehouse - Pipeline Deployment")
    parser.add_argument("--project-id", required=True, help="GCP Project ID")
    parser.add_argument("--region", default="us-central1", help="GCP Region")
    parser.add_argument("--action", choices=["deploy", "run", "monitor", "cleanup"], 
                       default="deploy", help="Action to perform")
    parser.add_argument("--pipeline", help="Specific pipeline to deploy/run")
    parser.add_argument("--job-name", help="Job name for running pipelines")
    parser.add_argument("--input-topic", help="Input Pub/Sub topic")
    parser.add_argument("--output-dataset", help="Output BigQuery dataset")
    
    args = parser.parse_args()
    
    deployer = DataflowPipelineDeployer(args.project_id, args.region)
    
    try:
        if args.action == "deploy":
            # Create GCS directories
            deployer.create_gcs_directories()
            
            # Deploy pipelines
            if args.pipeline:
                deployer.deploy_pipeline_template(args.pipeline)
            else:
                deployer.deploy_all_pipelines()
        
        elif args.action == "run":
            if args.pipeline:
                deployer.run_pipeline_job(
                    args.pipeline,
                    job_name=args.job_name,
                    input_topic=args.input_topic,
                    output_dataset=args.output_dataset
                )
            else:
                deployer.start_streaming_jobs()
        
        elif args.action == "monitor":
            # This would typically monitor jobs from a previous run
            # For demo purposes, we'll just log the action
            logger.info("Monitoring jobs (implement job tracking for production)")
        
        elif args.action == "cleanup":
            # This would typically clean up jobs from a previous run
            # For demo purposes, we'll just log the action
            logger.info("Cleaning up jobs (implement job tracking for production)")
        
        logger.info("Pipeline deployment completed successfully")
        
    except Exception as e:
        logger.error("Pipeline deployment failed", error=str(e))
        sys.exit(1)

if __name__ == "__main__":
    main() 