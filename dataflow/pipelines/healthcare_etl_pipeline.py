"""
Healthcare Data Lakehouse - Dataflow ETL Pipeline
Main Apache Beam pipeline for processing healthcare data from Pub/Sub to BigQuery.
"""

import json
import logging
import argparse
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.io import ReadFromPubSub, WriteToBigQuery, WriteToText
from apache_beam.io.gcp.bigquery import BigQueryDisposition
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.transforms import window
from apache_beam.transforms.trigger import AfterProcessingTime, AfterCount
from apache_beam.transforms.combiners import Count, Mean
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

class HealthcareDataProcessor:
    """Processes healthcare data with data quality checks and transformations"""
    
    def __init__(self):
        self.logger = structlog.get_logger()
    
    def parse_message(self, message: bytes) -> Dict[str, Any]:
        """Parse Pub/Sub message and validate data"""
        try:
            data = json.loads(message.decode('utf-8'))
            
            # Add processing metadata
            data['processed_at'] = datetime.now().isoformat()
            data['pipeline_version'] = '1.0.0'
            
            # Validate required fields based on data type
            data_type = data.get('data_type', 'unknown')
            
            if data_type == 'patient_vitals':
                self._validate_vitals_data(data)
            elif data_type == 'insurance_claim':
                self._validate_claims_data(data)
            elif data_type == 'ehr_record':
                self._validate_ehr_data(data)
            
            return data
            
        except json.JSONDecodeError as e:
            self.logger.error("Failed to parse JSON message", error=str(e))
            return {'error': 'invalid_json', 'raw_message': message.decode('utf-8', errors='ignore')}
        except Exception as e:
            self.logger.error("Failed to process message", error=str(e))
            return {'error': 'processing_error', 'raw_message': message.decode('utf-8', errors='ignore')}
    
    def _validate_vitals_data(self, data: Dict[str, Any]):
        """Validate patient vitals data"""
        required_fields = ['patient_id', 'heart_rate', 'temperature', 'timestamp']
        for field in required_fields:
            if field not in data:
                raise ValueError(f"Missing required field: {field}")
        
        # Validate ranges
        if not (40 <= data.get('heart_rate', 0) <= 200):
            data['heart_rate_anomaly'] = True
        
        if not (35.0 <= data.get('temperature', 0) <= 42.0):
            data['temperature_anomaly'] = True
    
    def _validate_claims_data(self, data: Dict[str, Any]):
        """Validate insurance claims data"""
        required_fields = ['claim_id', 'patient_id', 'total_amount', 'service_date']
        for field in required_fields:
            if field not in data:
                raise ValueError(f"Missing required field: {field}")
        
        # Validate amount
        if data.get('total_amount', 0) <= 0:
            data['amount_anomaly'] = True
    
    def _validate_ehr_data(self, data: Dict[str, Any]):
        """Validate EHR data"""
        required_fields = ['record_id', 'patient_id', 'visit_date', 'diagnosis']
        for field in required_fields:
            if field not in data:
                raise ValueError(f"Missing required field: {field}")

class DataQualityFilter(beam.DoFn):
    """Filter out low-quality data"""
    
    def process(self, element: Dict[str, Any]) -> List[Dict[str, Any]]:
        # Skip records with errors
        if 'error' in element:
            return []
        
        # Skip records with anomalies
        if any(key.endswith('_anomaly') and element[key] for key in element.keys()):
            return []
        
        return [element]

class DataEnricher(beam.DoFn):
    """Enrich data with additional fields"""
    
    def process(self, element: Dict[str, Any]) -> List[Dict[str, Any]]:
        data_type = element.get('data_type', 'unknown')
        
        # Add common enrichment fields
        element['enriched_at'] = datetime.now().isoformat()
        element['data_quality_score'] = self._calculate_quality_score(element)
        
        # Type-specific enrichment
        if data_type == 'patient_vitals':
            element = self._enrich_vitals(element)
        elif data_type == 'insurance_claim':
            element = self._enrich_claims(element)
        elif data_type == 'ehr_record':
            element = self._enrich_ehr(element)
        
        return [element]
    
    def _calculate_quality_score(self, data: Dict[str, Any]) -> float:
        """Calculate data quality score (0-1)"""
        score = 1.0
        
        # Deduct points for missing fields
        required_fields = ['patient_id', 'timestamp']
        for field in required_fields:
            if not data.get(field):
                score -= 0.2
        
        # Deduct points for anomalies
        anomaly_fields = [key for key in data.keys() if key.endswith('_anomaly')]
        for field in anomaly_fields:
            if data[field]:
                score -= 0.3
        
        return max(0.0, score)
    
    def _enrich_vitals(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich vitals data"""
        # Calculate derived metrics
        heart_rate = data.get('heart_rate', 0)
        if heart_rate > 100:
            data['heart_rate_category'] = 'elevated'
        elif heart_rate < 60:
            data['heart_rate_category'] = 'low'
        else:
            data['heart_rate_category'] = 'normal'
        
        # Add time-based fields
        timestamp = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
        data['hour_of_day'] = timestamp.hour
        data['day_of_week'] = timestamp.strftime('%A')
        
        return data
    
    def _enrich_claims(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich claims data"""
        # Calculate claim processing time
        service_date = datetime.strptime(data['service_date'], '%Y-%m-%d')
        submission_date = datetime.strptime(data['submission_date'], '%Y-%m-%d')
        processing_days = (submission_date - service_date).days
        data['processing_days'] = processing_days
        
        # Categorize claim amount
        amount = data.get('total_amount', 0)
        if amount < 100:
            data['amount_category'] = 'low'
        elif amount < 1000:
            data['amount_category'] = 'medium'
        else:
            data['amount_category'] = 'high'
        
        return data
    
    def _enrich_ehr(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich EHR data"""
        # Count medications
        medications = data.get('medications', [])
        data['medication_count'] = len(medications)
        
        # Count lab tests
        lab_results = data.get('lab_results', {})
        data['lab_test_count'] = len(lab_results)
        
        return data

class DataPartitioner(beam.DoFn):
    """Partition data by type for different processing paths"""
    
    def process(self, element: Dict[str, Any]) -> List[pvalue.TaggedOutput]:
        data_type = element.get('data_type', 'unknown')
        
        if data_type == 'patient_vitals':
            yield pvalue.TaggedOutput('vitals', element)
        elif data_type == 'insurance_claim':
            yield pvalue.TaggedOutput('claims', element)
        elif data_type == 'ehr_record':
            yield pvalue.TaggedOutput('ehr', element)
        else:
            yield pvalue.TaggedOutput('unknown', element)

def run_pipeline(argv=None):
    """Run the healthcare ETL pipeline"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', required=True, help='GCP Project ID')
    parser.add_argument('--region', default='us-central1', help='GCP Region')
    parser.add_argument('--input-topic', required=True, help='Input Pub/Sub topic')
    parser.add_argument('--output-dataset', required=True, help='Output BigQuery dataset')
    parser.add_argument('--temp-location', required=True, help='GCS temp location')
    parser.add_argument('--staging-location', required=True, help='GCS staging location')
    parser.add_argument('--runner', default='DataflowRunner', help='Pipeline runner')
    parser.add_argument('--streaming', action='store_true', help='Run in streaming mode')
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    # Set up pipeline options
    pipeline_options = PipelineOptions(pipeline_args)
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = known_args.project
    google_cloud_options.region = known_args.region
    google_cloud_options.temp_location = known_args.temp_location
    google_cloud_options.staging_location = known_args.staging_location
    
    # Set streaming options
    if known_args.streaming:
        pipeline_options.view_as(StandardOptions).streaming = True
    
    # Create pipeline
    with beam.Pipeline(options=pipeline_options) as pipeline:
        
        # Read from Pub/Sub
        if known_args.streaming:
            messages = (
                pipeline
                | 'Read from Pub/Sub' >> ReadFromPubSub(topic=known_args.input_topic)
                | 'Window' >> beam.WindowInto(
                    window.FixedWindows(60),  # 1-minute windows
                    trigger=AfterProcessingTime(30),  # Trigger after 30 seconds
                    accumulation_mode=window.AccumulationMode.ACCUMULATING
                )
            )
        else:
            messages = (
                pipeline
                | 'Read from Pub/Sub' >> ReadFromPubSub(topic=known_args.input_topic)
            )
        
        # Parse and validate messages
        parsed_data = (
            messages
            | 'Parse Messages' >> beam.Map(HealthcareDataProcessor().parse_message)
        )
        
        # Filter out low-quality data
        quality_data = (
            parsed_data
            | 'Filter Quality' >> beam.ParDo(DataQualityFilter())
        )
        
        # Enrich data
        enriched_data = (
            quality_data
            | 'Enrich Data' >> beam.ParDo(DataEnricher())
        )
        
        # Partition data by type
        partitioned_data = (
            enriched_data
            | 'Partition Data' >> beam.ParDo(DataPartitioner()).with_outputs('vitals', 'claims', 'ehr', 'unknown')
        )
        
        # Write vitals data to BigQuery
        (
            partitioned_data.vitals
            | 'Write Vitals to BigQuery' >> WriteToBigQuery(
                table=f'{known_args.project}:{known_args.output_dataset}.patient_vitals',
                schema='patient_id:STRING,timestamp:TIMESTAMP,heart_rate:INTEGER,'
                       'blood_pressure_systolic:INTEGER,blood_pressure_diastolic:INTEGER,'
                       'temperature:FLOAT,oxygen_saturation:INTEGER,respiratory_rate:INTEGER,'
                       'device_id:STRING,location:STRING,processed_at:TIMESTAMP,'
                       'heart_rate_category:STRING,hour_of_day:INTEGER,day_of_week:STRING,'
                       'data_quality_score:FLOAT',
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED
            )
        )
        
        # Write claims data to BigQuery
        (
            partitioned_data.claims
            | 'Write Claims to BigQuery' >> WriteToBigQuery(
                table=f'{known_args.project}:{known_args.output_dataset}.insurance_claims',
                schema='claim_id:STRING,patient_id:STRING,provider_id:STRING,'
                       'service_date:DATE,diagnosis_codes:STRING,procedure_codes:STRING,'
                       'total_amount:FLOAT,insurance_type:STRING,claim_status:STRING,'
                       'submission_date:DATE,processed_at:TIMESTAMP,processing_days:INTEGER,'
                       'amount_category:STRING,data_quality_score:FLOAT',
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED
            )
        )
        
        # Write EHR data to BigQuery
        (
            partitioned_data.ehr
            | 'Write EHR to BigQuery' >> WriteToBigQuery(
                table=f'{known_args.project}:{known_args.output_dataset}.ehr_records',
                schema='record_id:STRING,patient_id:STRING,visit_date:DATE,'
                       'provider_id:STRING,diagnosis:STRING,treatment:STRING,'
                       'medications:STRING,lab_results:STRING,notes:STRING,'
                       'processed_at:TIMESTAMP,medication_count:INTEGER,'
                       'lab_test_count:INTEGER,data_quality_score:FLOAT',
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED
            )
        )
        
        # Write unknown data to error log
        (
            partitioned_data.unknown
            | 'Write Unknown Data' >> WriteToText(
                file_path_prefix=f'{known_args.staging_location}/errors/unknown_data',
                file_name_suffix='.json'
            )
        )
        
        # Calculate metrics
        metrics = (
            enriched_data
            | 'Count Records' >> Count.Globally()
            | 'Log Metrics' >> beam.Map(lambda count: logger.info("Processed records", count=count))
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run_pipeline() 