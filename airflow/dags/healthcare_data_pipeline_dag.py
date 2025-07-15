"""
Healthcare Data Lakehouse - Airflow DAG
Main orchestration DAG for the healthcare data pipeline.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator, BigQueryValueCheckOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator
from airflow.utils.trigger_rule import TriggerRule
import os

# Default arguments
default_args = {
    'owner': 'healthcare-data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'project_id': os.getenv('GCP_PROJECT_ID', 'your-project-id'),
    'region': os.getenv('GCP_REGION', 'us-central1'),
}

# DAG configuration
dag = DAG(
    'healthcare_data_pipeline',
    default_args=default_args,
    description='Healthcare Data Lakehouse Pipeline',
    schedule_interval='*/15 * * * *',  # Every 15 minutes
    catchup=False,
    max_active_runs=1,
    tags=['healthcare', 'data-pipeline', 'etl'],
)

# Task IDs
START_TASK = 'start_pipeline'
CHECK_DATA_QUALITY = 'check_data_quality'
PROCESS_IOT_DATA = 'process_iot_data'
PROCESS_CLAIMS_DATA = 'process_claims_data'
PROCESS_EHR_DATA = 'process_ehr_data'
RUN_DBT_MODELS = 'run_dbt_models'
GENERATE_REPORTS = 'generate_reports'
MONITOR_PIPELINE = 'monitor_pipeline'
END_TASK = 'end_pipeline'

# Start task
start = DummyOperator(
    task_id=START_TASK,
    dag=dag,
)

# Data quality checks
check_data_quality = PythonOperator(
    task_id=CHECK_DATA_QUALITY,
    python_callable=check_data_quality_metrics,
    dag=dag,
)

# Process IoT data (vitals)
process_iot_data = DataflowTemplatedJobStartOperator(
    task_id=PROCESS_IOT_DATA,
    template='gs://{{ var.value.project_id }}-healthcare-processed/templates/healthcare-etl-template',
    job_name='healthcare-etl-iot-{{ ts_nodash }}',
    location=default_args['region'],
    parameters={
        'input-topic': f"projects/{default_args['project_id']}/topics/healthcare-iot-data",
        'output-dataset': f"{default_args['project_id']}.healthcare_processed",
    },
    dag=dag,
)

# Process claims data
process_claims_data = DataflowTemplatedJobStartOperator(
    task_id=PROCESS_CLAIMS_DATA,
    template='gs://{{ var.value.project_id }}-healthcare-processed/templates/healthcare-etl-template',
    job_name='healthcare-etl-claims-{{ ts_nodash }}',
    location=default_args['region'],
    parameters={
        'input-topic': f"projects/{default_args['project_id']}/topics/healthcare-claims-data",
        'output-dataset': f"{default_args['project_id']}.healthcare_processed",
    },
    dag=dag,
)

# Process EHR data
process_ehr_data = DataflowTemplatedJobStartOperator(
    task_id=PROCESS_EHR_DATA,
    template='gs://{{ var.value.project_id }}-healthcare-processed/templates/healthcare-etl-template',
    job_name='healthcare-etl-ehr-{{ ts_nodash }}',
    location=default_args['region'],
    parameters={
        'input-topic': f"projects/{default_args['project_id']}/topics/healthcare-ehr-data",
        'output-dataset': f"{default_args['project_id']}.healthcare_processed",
    },
    dag=dag,
)

# Run dbt models
run_dbt_models = BashOperator(
    task_id=RUN_DBT_MODELS,
    bash_command="""
    cd /opt/airflow/dbt/healthcare_data_lakehouse
    dbt run --profiles-dir /opt/airflow/dbt/profiles
    dbt test --profiles-dir /opt/airflow/dbt/profiles
    """,
    dag=dag,
)

# Generate reports
generate_reports = PythonOperator(
    task_id=GENERATE_REPORTS,
    python_callable=generate_healthcare_reports,
    dag=dag,
)

# Monitor pipeline health
monitor_pipeline = PythonOperator(
    task_id=MONITOR_PIPELINE,
    python_callable=monitor_pipeline_health,
    dag=dag,
)

# End task
end = DummyOperator(
    task_id=END_TASK,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

# Task dependencies
start >> check_data_quality

check_data_quality >> [process_iot_data, process_claims_data, process_ehr_data]

[process_iot_data, process_claims_data, process_ehr_data] >> run_dbt_models

run_dbt_models >> generate_reports

generate_reports >> monitor_pipeline

monitor_pipeline >> end

# Python functions for custom tasks
def check_data_quality_metrics(**context):
    """Check data quality metrics for the pipeline"""
    import logging
    from google.cloud import bigquery
    
    logging.info("Checking data quality metrics...")
    
    # Initialize BigQuery client
    client = bigquery.Client(project=default_args['project_id'])
    
    # Check for data freshness
    freshness_query = """
    SELECT 
        COUNT(*) as recent_records,
        MAX(processed_at) as latest_processing_time
    FROM `{project_id}.healthcare_processed.patient_vitals`
    WHERE processed_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
    """.format(project_id=default_args['project_id'])
    
    query_job = client.query(freshness_query)
    results = query_job.result()
    
    for row in results:
        logging.info(f"Recent records: {row.recent_records}")
        logging.info(f"Latest processing time: {row.latest_processing_time}")
        
        if row.recent_records == 0:
            raise ValueError("No recent data found - pipeline may be failing")
    
    # Check for data quality issues
    quality_query = """
    SELECT 
        COUNT(*) as total_records,
        COUNT(CASE WHEN data_quality_score < 0.7 THEN 1 END) as low_quality_records,
        AVG(data_quality_score) as avg_quality_score
    FROM `{project_id}.healthcare_processed.patient_vitals`
    WHERE processed_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
    """.format(project_id=default_args['project_id'])
    
    query_job = client.query(quality_query)
    results = query_job.result()
    
    for row in results:
        logging.info(f"Total records: {row.total_records}")
        logging.info(f"Low quality records: {row.low_quality_records}")
        logging.info(f"Average quality score: {row.avg_quality_score}")
        
        if row.avg_quality_score < 0.8:
            logging.warning("Data quality score is below threshold")
    
    logging.info("Data quality check completed successfully")

def generate_healthcare_reports(**context):
    """Generate healthcare reports and metrics"""
    import logging
    from google.cloud import bigquery
    
    logging.info("Generating healthcare reports...")
    
    # Initialize BigQuery client
    client = bigquery.Client(project=default_args['project_id'])
    
    # Generate patient monitoring report
    monitoring_query = """
    SELECT 
        DATE(measurement_timestamp) as date,
        COUNT(*) as total_measurements,
        COUNT(CASE WHEN elevated_heart_rate_alert THEN 1 END) as elevated_heart_rate_count,
        COUNT(CASE WHEN low_oxygen_alert THEN 1 END) as low_oxygen_count,
        COUNT(CASE WHEN fever_alert THEN 1 END) as fever_count,
        AVG(heart_rate) as avg_heart_rate,
        AVG(temperature) as avg_temperature
    FROM `{project_id}.healthcare_curated.fact_patient_encounters`
    WHERE measurement_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
    GROUP BY DATE(measurement_timestamp)
    ORDER BY date DESC
    """.format(project_id=default_args['project_id'])
    
    query_job = client.query(monitoring_query)
    results = query_job.result()
    
    for row in results:
        logging.info(f"Date: {row.date}")
        logging.info(f"Total measurements: {row.total_measurements}")
        logging.info(f"Elevated heart rate alerts: {row.elevated_heart_rate_count}")
        logging.info(f"Low oxygen alerts: {row.low_oxygen_count}")
        logging.info(f"Fever alerts: {row.fever_count}")
        logging.info(f"Average heart rate: {row.avg_heart_rate}")
        logging.info(f"Average temperature: {row.avg_temperature}")
    
    # Generate claims processing report
    claims_query = """
    SELECT 
        DATE(service_date) as date,
        COUNT(*) as total_claims,
        SUM(total_amount) as total_value,
        COUNT(CASE WHEN claim_status = 'Paid' THEN 1 END) as paid_claims,
        COUNT(CASE WHEN claim_status = 'Denied' THEN 1 END) as denied_claims,
        AVG(processing_days) as avg_processing_days
    FROM `{project_id}.healthcare_curated.fact_patient_encounters`
    WHERE service_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    GROUP BY DATE(service_date)
    ORDER BY date DESC
    """.format(project_id=default_args['project_id'])
    
    query_job = client.query(claims_query)
    results = query_job.result()
    
    for row in results:
        logging.info(f"Claims Date: {row.date}")
        logging.info(f"Total claims: {row.total_claims}")
        logging.info(f"Total value: ${row.total_value:,.2f}")
        logging.info(f"Paid claims: {row.paid_claims}")
        logging.info(f"Denied claims: {row.denied_claims}")
        logging.info(f"Average processing days: {row.avg_processing_days}")
    
    logging.info("Healthcare reports generated successfully")

def monitor_pipeline_health(**context):
    """Monitor overall pipeline health and send alerts if needed"""
    import logging
    from google.cloud import bigquery
    from google.cloud import monitoring_v3
    
    logging.info("Monitoring pipeline health...")
    
    # Initialize clients
    client = bigquery.Client(project=default_args['project_id'])
    monitoring_client = monitoring_v3.MetricServiceClient()
    
    # Check pipeline metrics
    pipeline_query = """
    SELECT 
        COUNT(*) as total_encounters,
        COUNT(CASE WHEN risk_level = 'high_risk' THEN 1 END) as high_risk_encounters,
        COUNT(CASE WHEN encounter_type = 'comprehensive' THEN 1 END) as comprehensive_encounters,
        AVG(encounter_complexity_score) as avg_complexity
    FROM `{project_id}.healthcare_curated.fact_patient_encounters`
    WHERE measurement_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
    """.format(project_id=default_args['project_id'])
    
    query_job = client.query(pipeline_query)
    results = query_job.result()
    
    for row in results:
        logging.info(f"Total encounters: {row.total_encounters}")
        logging.info(f"High risk encounters: {row.high_risk_encounters}")
        logging.info(f"Comprehensive encounters: {row.comprehensive_encounters}")
        logging.info(f"Average complexity: {row.avg_complexity}")
        
        # Alert if high risk encounters exceed threshold
        if row.high_risk_encounters > 10:
            logging.warning(f"High number of high-risk encounters: {row.high_risk_encounters}")
        
        # Alert if no data is being processed
        if row.total_encounters == 0:
            logging.error("No encounters processed in the last hour - pipeline may be failing")
            raise ValueError("Pipeline health check failed - no data processed")
    
    # Check data freshness
    freshness_query = """
    SELECT 
        MAX(measurement_timestamp) as latest_timestamp,
        TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(measurement_timestamp), MINUTE) as minutes_since_last_update
    FROM `{project_id}.healthcare_curated.fact_patient_encounters`
    """.format(project_id=default_args['project_id'])
    
    query_job = client.query(freshness_query)
    results = query_job.result()
    
    for row in results:
        logging.info(f"Latest timestamp: {row.latest_timestamp}")
        logging.info(f"Minutes since last update: {row.minutes_since_last_update}")
        
        # Alert if data is stale
        if row.minutes_since_last_update > 30:
            logging.warning(f"Data is stale - last update was {row.minutes_since_last_update} minutes ago")
    
    logging.info("Pipeline health monitoring completed successfully")

# Add custom sensors for monitoring
class HealthcareDataSensor(PubSubPullSensor):
    """Custom sensor for monitoring healthcare data flow"""
    
    def __init__(self, **kwargs):
        super().__init__(
            project_id=default_args['project_id'],
            subscription=f"projects/{default_args['project_id']}/subscriptions/healthcare-iot-data-sub",
            **kwargs
        )
    
    def poke(self, context):
        """Check if there are messages in the subscription"""
        messages = self.pull_messages()
        if messages:
            self.log.info(f"Found {len(messages)} messages in subscription")
            return True
        return False

# Add the sensor to the DAG
data_sensor = HealthcareDataSensor(
    task_id='monitor_data_flow',
    poke_interval=60,  # Check every minute
    timeout=300,  # 5 minute timeout
    dag=dag,
)

# Add sensor to the workflow
start >> data_sensor >> check_data_quality 