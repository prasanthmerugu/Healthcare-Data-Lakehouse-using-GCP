"""
Pub/Sub Publisher for Healthcare Data Ingestion
Publishes healthcare data to Google Cloud Pub/Sub topics.
"""

import json
import time
import random
import logging
from typing import Dict, Any, List
from datetime import datetime, timedelta
from google.cloud import pubsub_v1
from google.cloud.exceptions import GoogleCloudError
import structlog

# Import data generator
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'data_generators'))
from healthcare_data_generator import HealthcareDataGenerator

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

class HealthcareDataPublisher:
    """Publishes healthcare data to Pub/Sub topics"""
    
    def __init__(self, project_id: str, region: str = "us-central1"):
        self.project_id = project_id
        self.region = region
        self.publisher = pubsub_v1.PublisherClient()
        self.data_generator = HealthcareDataGenerator()
        
        # Topic paths
        self.topics = {
            "iot_data": f"projects/{project_id}/topics/healthcare-iot-data",
            "claims_data": f"projects/{project_id}/topics/healthcare-claims-data",
            "ehr_data": f"projects/{project_id}/topics/healthcare-ehr-data"
        }
        
        logger.info("HealthcareDataPublisher initialized", 
                   project_id=project_id, 
                   region=region,
                   topics=list(self.topics.keys()))
    
    def publish_message(self, topic_name: str, data: Dict[str, Any]) -> str:
        """Publish a single message to a topic"""
        try:
            topic_path = self.topics[topic_name]
            message_json = json.dumps(data, default=str)
            message_bytes = message_json.encode("utf-8")
            
            # Add metadata to the message
            future = self.publisher.publish(
                topic_path,
                data=message_bytes,
                data_type=data.get("data_type", "unknown"),
                timestamp=data.get("timestamp", datetime.now().isoformat()),
                source="healthcare-data-publisher"
            )
            
            message_id = future.result()
            logger.info("Message published successfully",
                       topic=topic_name,
                       message_id=message_id,
                       data_type=data.get("data_type", "unknown"))
            
            return message_id
            
        except GoogleCloudError as e:
            logger.error("Failed to publish message",
                        topic=topic_name,
                        error=str(e),
                        data=data)
            raise
    
    def publish_vitals_data(self, count: int = 10, interval_seconds: float = 1.0):
        """Publish patient vitals data to IoT topic"""
        logger.info("Starting vitals data publishing",
                   count=count,
                   interval=interval_seconds)
        
        published_count = 0
        for i in range(count):
            try:
                vitals = self.data_generator.generate_patient_vitals()
                vitals_dict = {
                    "data_type": "patient_vitals",
                    "timestamp": vitals.timestamp,
                    "patient_id": vitals.patient_id,
                    "heart_rate": vitals.heart_rate,
                    "blood_pressure_systolic": vitals.blood_pressure_systolic,
                    "blood_pressure_diastolic": vitals.blood_pressure_diastolic,
                    "temperature": vitals.temperature,
                    "oxygen_saturation": vitals.oxygen_saturation,
                    "respiratory_rate": vitals.respiratory_rate,
                    "device_id": vitals.device_id,
                    "location": vitals.location
                }
                
                self.publish_message("iot_data", vitals_dict)
                published_count += 1
                
                # Random interval between messages
                time.sleep(random.uniform(interval_seconds * 0.5, interval_seconds * 1.5))
                
            except Exception as e:
                logger.error("Failed to publish vitals data",
                           error=str(e),
                           attempt=i + 1)
        
        logger.info("Vitals data publishing completed",
                   published_count=published_count,
                   total_attempts=count)
    
    def publish_claims_data(self, count: int = 5, interval_seconds: float = 2.0):
        """Publish insurance claims data to claims topic"""
        logger.info("Starting claims data publishing",
                   count=count,
                   interval=interval_seconds)
        
        published_count = 0
        for i in range(count):
            try:
                claim = self.data_generator.generate_insurance_claim()
                claim_dict = {
                    "data_type": "insurance_claim",
                    "timestamp": datetime.now().isoformat(),
                    "claim_id": claim.claim_id,
                    "patient_id": claim.patient_id,
                    "provider_id": claim.provider_id,
                    "service_date": claim.service_date,
                    "diagnosis_codes": claim.diagnosis_codes,
                    "procedure_codes": claim.procedure_codes,
                    "total_amount": claim.total_amount,
                    "insurance_type": claim.insurance_type,
                    "claim_status": claim.claim_status,
                    "submission_date": claim.submission_date
                }
                
                self.publish_message("claims_data", claim_dict)
                published_count += 1
                
                time.sleep(random.uniform(interval_seconds * 0.5, interval_seconds * 1.5))
                
            except Exception as e:
                logger.error("Failed to publish claims data",
                           error=str(e),
                           attempt=i + 1)
        
        logger.info("Claims data publishing completed",
                   published_count=published_count,
                   total_attempts=count)
    
    def publish_ehr_data(self, count: int = 3, interval_seconds: float = 3.0):
        """Publish EHR data to EHR topic"""
        logger.info("Starting EHR data publishing",
                   count=count,
                   interval=interval_seconds)
        
        published_count = 0
        for i in range(count):
            try:
                ehr = self.data_generator.generate_ehr_record()
                ehr_dict = {
                    "data_type": "ehr_record",
                    "timestamp": datetime.now().isoformat(),
                    "record_id": ehr.record_id,
                    "patient_id": ehr.patient_id,
                    "visit_date": ehr.visit_date,
                    "provider_id": ehr.provider_id,
                    "diagnosis": ehr.diagnosis,
                    "treatment": ehr.treatment,
                    "medications": ehr.medications,
                    "lab_results": ehr.lab_results,
                    "notes": ehr.notes
                }
                
                self.publish_message("ehr_data", ehr_dict)
                published_count += 1
                
                time.sleep(random.uniform(interval_seconds * 0.5, interval_seconds * 1.5))
                
            except Exception as e:
                logger.error("Failed to publish EHR data",
                           error=str(e),
                           attempt=i + 1)
        
        logger.info("EHR data publishing completed",
                   published_count=published_count,
                   total_attempts=count)
    
    def publish_all_data_types(self, duration_minutes: int = 10):
        """Publish all data types for a specified duration"""
        logger.info("Starting comprehensive data publishing",
                   duration_minutes=duration_minutes)
        
        end_time = datetime.now() + timedelta(minutes=duration_minutes)
        published_counts = {"vitals": 0, "claims": 0, "ehr": 0}
        
        while datetime.now() < end_time:
            try:
                # Publish vitals (most frequent)
                if random.random() < 0.6:  # 60% chance
                    vitals = self.data_generator.generate_patient_vitals()
                    vitals_dict = {
                        "data_type": "patient_vitals",
                        "timestamp": vitals.timestamp,
                        "patient_id": vitals.patient_id,
                        "heart_rate": vitals.heart_rate,
                        "blood_pressure_systolic": vitals.blood_pressure_systolic,
                        "blood_pressure_diastolic": vitals.blood_pressure_diastolic,
                        "temperature": vitals.temperature,
                        "oxygen_saturation": vitals.oxygen_saturation,
                        "respiratory_rate": vitals.respiratory_rate,
                        "device_id": vitals.device_id,
                        "location": vitals.location
                    }
                    self.publish_message("iot_data", vitals_dict)
                    published_counts["vitals"] += 1
                
                # Publish claims (less frequent)
                if random.random() < 0.2:  # 20% chance
                    claim = self.data_generator.generate_insurance_claim()
                    claim_dict = {
                        "data_type": "insurance_claim",
                        "timestamp": datetime.now().isoformat(),
                        "claim_id": claim.claim_id,
                        "patient_id": claim.patient_id,
                        "provider_id": claim.provider_id,
                        "service_date": claim.service_date,
                        "diagnosis_codes": claim.diagnosis_codes,
                        "procedure_codes": claim.procedure_codes,
                        "total_amount": claim.total_amount,
                        "insurance_type": claim.insurance_type,
                        "claim_status": claim.claim_status,
                        "submission_date": claim.submission_date
                    }
                    self.publish_message("claims_data", claim_dict)
                    published_counts["claims"] += 1
                
                # Publish EHR (least frequent)
                if random.random() < 0.1:  # 10% chance
                    ehr = self.data_generator.generate_ehr_record()
                    ehr_dict = {
                        "data_type": "ehr_record",
                        "timestamp": datetime.now().isoformat(),
                        "record_id": ehr.record_id,
                        "patient_id": ehr.patient_id,
                        "visit_date": ehr.visit_date,
                        "provider_id": ehr.provider_id,
                        "diagnosis": ehr.diagnosis,
                        "treatment": ehr.treatment,
                        "medications": ehr.medications,
                        "lab_results": ehr.lab_results,
                        "notes": ehr.notes
                    }
                    self.publish_message("ehr_data", ehr_dict)
                    published_counts["ehr"] += 1
                
                # Random interval between batches
                time.sleep(random.uniform(0.5, 2.0))
                
            except Exception as e:
                logger.error("Failed to publish data batch",
                           error=str(e))
        
        logger.info("Comprehensive data publishing completed",
                   published_counts=published_counts,
                   duration_minutes=duration_minutes)

def main():
    """Main function for running the publisher"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Healthcare Data Publisher")
    parser.add_argument("--project-id", required=True, help="GCP Project ID")
    parser.add_argument("--region", default="us-central1", help="GCP Region")
    parser.add_argument("--data-type", choices=["vitals", "claims", "ehr", "all"], 
                       default="all", help="Type of data to publish")
    parser.add_argument("--count", type=int, default=100, help="Number of messages to publish")
    parser.add_argument("--duration", type=int, default=10, help="Duration in minutes for continuous publishing")
    parser.add_argument("--interval", type=float, default=1.0, help="Interval between messages in seconds")
    
    args = parser.parse_args()
    
    publisher = HealthcareDataPublisher(args.project_id, args.region)
    
    try:
        if args.data_type == "vitals":
            publisher.publish_vitals_data(args.count, args.interval)
        elif args.data_type == "claims":
            publisher.publish_claims_data(args.count, args.interval)
        elif args.data_type == "ehr":
            publisher.publish_ehr_data(args.count, args.interval)
        elif args.data_type == "all":
            publisher.publish_all_data_types(args.duration)
        
        logger.info("Data publishing completed successfully")
        
    except KeyboardInterrupt:
        logger.info("Data publishing interrupted by user")
    except Exception as e:
        logger.error("Data publishing failed", error=str(e))
        raise

if __name__ == "__main__":
    main() 