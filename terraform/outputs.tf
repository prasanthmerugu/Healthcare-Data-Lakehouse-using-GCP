output "project_id" {
  description = "The GCP project ID"
  value       = var.project_id
}

output "region" {
  description = "The GCP region"
  value       = var.region
}

output "vpc_network" {
  description = "The VPC network name"
  value       = google_compute_network.healthcare_vpc.name
}

output "vpc_subnet" {
  description = "The VPC subnet name"
  value       = google_compute_subnetwork.private_subnet.name
}

output "gcs_buckets" {
  description = "GCS bucket names for data lake zones"
  value = {
    raw_zone      = google_storage_bucket.raw_zone.name
    processed_zone = google_storage_bucket.processed_zone.name
    curated_zone   = google_storage_bucket.curated_zone.name
  }
}

output "bigquery_datasets" {
  description = "BigQuery dataset IDs"
  value = {
    raw_dataset      = google_bigquery_dataset.raw_dataset.dataset_id
    processed_dataset = google_bigquery_dataset.processed_dataset.dataset_id
    curated_dataset   = google_bigquery_dataset.curated_dataset.dataset_id
  }
}

output "pubsub_topics" {
  description = "Pub/Sub topic names"
  value = {
    iot_data_topic    = google_pubsub_topic.iot_data_topic.name
    claims_data_topic = google_pubsub_topic.claims_data_topic.name
    ehr_data_topic    = google_pubsub_topic.ehr_data_topic.name
  }
}

output "pubsub_subscriptions" {
  description = "Pub/Sub subscription names"
  value = {
    iot_data_subscription    = google_pubsub_subscription.iot_data_subscription.name
    claims_data_subscription = google_pubsub_subscription.claims_data_subscription.name
  }
}

output "service_accounts" {
  description = "Service account emails"
  value = {
    dataflow_sa  = google_service_account.dataflow_sa.email
    composer_sa  = google_service_account.composer_sa.email
    ingestion_sa = google_service_account.ingestion_sa.email
  }
}

output "composer_environment" {
  description = "Cloud Composer environment details"
  value = {
    name   = google_composer_environment.healthcare_composer.name
    region = google_composer_environment.healthcare_composer.region
    config = google_composer_environment.healthcare_composer.config
  }
}

output "kms_key" {
  description = "Cloud KMS encryption key"
  value = {
    key_ring = google_kms_key_ring.healthcare_keyring.name
    key_name = google_kms_crypto_key.healthcare_key.name
    key_id   = google_kms_crypto_key.healthcare_key.id
  }
}

output "monitoring_workspace" {
  description = "Cloud Monitoring workspace"
  value = {
    name    = google_monitoring_workspace.healthcare_workspace.name
    project = google_monitoring_workspace.healthcare_workspace.project
  }
}

output "audit_logs_sink" {
  description = "Audit logs sink destination"
  value = {
    name        = google_logging_project_sink.audit_logs.name
    destination = google_logging_project_sink.audit_logs.destination
  }
}

output "setup_instructions" {
  description = "Instructions for setting up the data lakehouse"
  value = <<-EOT
    Healthcare Data Lakehouse Infrastructure Deployed Successfully!
    
    Next Steps:
    1. Deploy Dataflow pipelines: cd ../dataflow && python deploy_pipelines.py
    2. Set up dbt models: cd ../dbt && dbt deps && dbt run
    3. Deploy Airflow DAGs: Upload DAGs to Cloud Composer
    4. Start data ingestion: cd ../ingestion && python start_ingestion.py
    5. Configure Looker: Connect to BigQuery datasets
    
    Important Resources:
    - GCS Buckets: ${google_storage_bucket.raw_zone.name}, ${google_storage_bucket.processed_zone.name}, ${google_storage_bucket.curated_zone.name}
    - BigQuery Datasets: ${google_bigquery_dataset.raw_dataset.dataset_id}, ${google_bigquery_dataset.processed_dataset.dataset_id}, ${google_bigquery_dataset.curated_dataset.dataset_id}
    - Pub/Sub Topics: ${google_pubsub_topic.iot_data_topic.name}, ${google_pubsub_topic.claims_data_topic.name}
    - Composer Environment: ${google_composer_environment.healthcare_composer.name}
    
    Security Features Enabled:
    - VPC Service Controls: ${var.enable_vpc_service_controls}
    - Encryption: ${var.enable_encryption}
    - Audit Logging: ${var.enable_audit_logging}
  EOT
} 