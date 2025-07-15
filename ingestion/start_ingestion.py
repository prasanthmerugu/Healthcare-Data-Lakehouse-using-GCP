#!/usr/bin/env python3
"""
Healthcare Data Lakehouse - Data Ingestion Starter
Main script to start data ingestion for the healthcare data lakehouse.
"""

import os
import sys
import time
import argparse
import signal
from datetime import datetime
import structlog

# Add the ingestion directory to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from publishers.pubsub_publisher import HealthcareDataPublisher

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

class DataIngestionManager:
    """Manages the data ingestion process"""
    
    def __init__(self, project_id: str, region: str = "us-central1"):
        self.project_id = project_id
        self.region = region
        self.publisher = HealthcareDataPublisher(project_id, region)
        self.running = False
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info("DataIngestionManager initialized",
                   project_id=project_id,
                   region=region)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info("Received shutdown signal", signal=signum)
        self.running = False
    
    def start_continuous_ingestion(self, duration_minutes: int = None):
        """Start continuous data ingestion"""
        self.running = True
        start_time = datetime.now()
        
        logger.info("Starting continuous data ingestion",
                   start_time=start_time.isoformat(),
                   duration_minutes=duration_minutes)
        
        try:
            while self.running:
                # Check if we've reached the duration limit
                if duration_minutes and (datetime.now() - start_time).total_seconds() / 60 >= duration_minutes:
                    logger.info("Reached duration limit, stopping ingestion",
                               duration_minutes=duration_minutes)
                    break
                
                # Publish a batch of data
                self.publisher.publish_all_data_types(duration_minutes=1)
                
                # Small delay between batches
                time.sleep(2)
                
        except KeyboardInterrupt:
            logger.info("Ingestion interrupted by user")
        except Exception as e:
            logger.error("Ingestion failed", error=str(e))
            raise
        finally:
            self.running = False
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds() / 60
            
            logger.info("Data ingestion completed",
                       start_time=start_time.isoformat(),
                       end_time=end_time.isoformat(),
                       duration_minutes=duration)
    
    def start_batch_ingestion(self, data_type: str, count: int):
        """Start batch data ingestion"""
        logger.info("Starting batch data ingestion",
                   data_type=data_type,
                   count=count)
        
        try:
            if data_type == "vitals":
                self.publisher.publish_vitals_data(count)
            elif data_type == "claims":
                self.publisher.publish_claims_data(count)
            elif data_type == "ehr":
                self.publisher.publish_ehr_data(count)
            elif data_type == "all":
                # Publish a mix of all data types
                self.publisher.publish_vitals_data(count // 3)
                self.publisher.publish_claims_data(count // 6)
                self.publisher.publish_ehr_data(count // 6)
            else:
                raise ValueError(f"Unknown data type: {data_type}")
            
            logger.info("Batch ingestion completed successfully",
                       data_type=data_type,
                       count=count)
            
        except Exception as e:
            logger.error("Batch ingestion failed",
                        data_type=data_type,
                        count=count,
                        error=str(e))
            raise
    
    def start_demo_mode(self):
        """Start demo mode with realistic data patterns"""
        logger.info("Starting demo mode - realistic healthcare data patterns")
        
        self.running = True
        start_time = datetime.now()
        
        try:
            while self.running:
                current_hour = datetime.now().hour
                
                # Simulate different data patterns throughout the day
                if 6 <= current_hour <= 18:  # Day shift - more activity
                    vitals_interval = 0.5
                    claims_interval = 1.5
                    ehr_interval = 2.0
                else:  # Night shift - less activity
                    vitals_interval = 1.0
                    claims_interval = 3.0
                    ehr_interval = 5.0
                
                # Publish vitals (most frequent)
                self.publisher.publish_vitals_data(5, vitals_interval)
                
                # Publish claims (moderate frequency)
                if datetime.now().minute % 5 == 0:  # Every 5 minutes
                    self.publisher.publish_claims_data(2, claims_interval)
                
                # Publish EHR (least frequent)
                if datetime.now().minute % 15 == 0:  # Every 15 minutes
                    self.publisher.publish_ehr_data(1, ehr_interval)
                
                time.sleep(30)  # Wait 30 seconds before next cycle
                
        except KeyboardInterrupt:
            logger.info("Demo mode interrupted by user")
        except Exception as e:
            logger.error("Demo mode failed", error=str(e))
            raise
        finally:
            self.running = False
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds() / 60
            
            logger.info("Demo mode completed",
                       start_time=start_time.isoformat(),
                       end_time=end_time.isoformat(),
                       duration_minutes=duration)

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Healthcare Data Lakehouse - Data Ingestion")
    parser.add_argument("--project-id", required=True, help="GCP Project ID")
    parser.add_argument("--region", default="us-central1", help="GCP Region")
    parser.add_argument("--mode", choices=["continuous", "batch", "demo"], 
                       default="demo", help="Ingestion mode")
    parser.add_argument("--data-type", choices=["vitals", "claims", "ehr", "all"], 
                       default="all", help="Type of data to ingest (for batch mode)")
    parser.add_argument("--count", type=int, default=100, help="Number of records (for batch mode)")
    parser.add_argument("--duration", type=int, default=60, help="Duration in minutes (for continuous mode)")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], 
                       default="INFO", help="Log level")
    
    args = parser.parse_args()
    
    # Set log level
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    # Create ingestion manager
    manager = DataIngestionManager(args.project_id, args.region)
    
    try:
        if args.mode == "continuous":
            manager.start_continuous_ingestion(args.duration)
        elif args.mode == "batch":
            manager.start_batch_ingestion(args.data_type, args.count)
        elif args.mode == "demo":
            manager.start_demo_mode()
        
        logger.info("Data ingestion completed successfully")
        
    except KeyboardInterrupt:
        logger.info("Data ingestion interrupted by user")
    except Exception as e:
        logger.error("Data ingestion failed", error=str(e))
        sys.exit(1)

if __name__ == "__main__":
    main() 