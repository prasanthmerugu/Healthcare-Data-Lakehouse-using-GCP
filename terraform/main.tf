terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

# Enable required APIs
resource "google_project_service" "required_apis" {
  for_each = toset([
    "compute.googleapis.com",
    "storage.googleapis.com",
    "bigquery.googleapis.com",
    "pubsub.googleapis.com",
    "dataflow.googleapis.com",
    "composer.googleapis.com",
    "cloudkms.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "servicenetworking.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "iam.googleapis.com"
  ])
  
  project = var.project_id
  service = each.value
  
  disable_dependent_services = true
  disable_on_destroy         = false
}

# VPC Network
resource "google_compute_network" "healthcare_vpc" {
  name                    = "healthcare-vpc"
  auto_create_subnetworks = false
  depends_on              = [google_project_service.required_apis]
}

# Subnet for private services
resource "google_compute_subnetwork" "private_subnet" {
  name          = "healthcare-private-subnet"
  ip_cidr_range = "10.0.0.0/24"
  network       = google_compute_network.healthcare_vpc.id
  region        = var.region
  
  # Enable private Google access for BigQuery and Dataflow
  private_ip_google_access = true
}

# Cloud Router for NAT
resource "google_compute_router" "router" {
  name    = "healthcare-router"
  region  = var.region
  network = google_compute_network.healthcare_vpc.id
}

# Cloud NAT for private instances to access internet
resource "google_compute_router_nat" "nat" {
  name                               = "healthcare-nat"
  router                            = google_compute_router.router.name
  region                            = var.region
  nat_ip_allocate_option            = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"
}

# VPC Service Controls
resource "google_access_context_manager_access_policy" "healthcare_policy" {
  parent = "organizations/${var.organization_id}"
  title  = "Healthcare Data Protection Policy"
}

resource "google_access_context_manager_service_perimeter" "healthcare_perimeter" {
  parent = "accessPolicies/${google_access_context_manager_access_policy.healthcare_policy.name}"
  name   = "accessPolicies/${google_access_context_manager_access_policy.healthcare_policy.name}/servicePerimeters/healthcare-perimeter"
  title  = "Healthcare Data Perimeter"
  
  status {
    restricted_services = [
      "storage.googleapis.com",
      "bigquery.googleapis.com",
      "pubsub.googleapis.com",
      "dataflow.googleapis.com"
    ]
    
    resources = [
      "projects/${var.project_id}"
    ]
  }
}

# Cloud KMS Key Ring
resource "google_kms_key_ring" "healthcare_keyring" {
  name     = "healthcare-keyring"
  location = var.region
  depends_on = [google_project_service.required_apis]
}

# Cloud KMS Crypto Key for encryption
resource "google_kms_crypto_key" "healthcare_key" {
  name     = "healthcare-encryption-key"
  key_ring = google_kms_key_ring.healthcare_keyring.id
  
  lifecycle {
    prevent_destroy = true
  }
}

# GCS Buckets for Data Lake
resource "google_storage_bucket" "raw_zone" {
  name          = "${var.project_id}-healthcare-raw"
  location      = var.region
  force_destroy = true
  
  versioning {
    enabled = true
  }
  
  lifecycle_rule {
    condition {
      age = 365  # Keep raw data for 1 year
    }
    action {
      type = "Delete"
    }
  }
  
  uniform_bucket_level_access = true
  
  # Enable encryption with Cloud KMS
  encryption {
    default_kms_key_name = google_kms_crypto_key.healthcare_key.id
  }
}

resource "google_storage_bucket" "processed_zone" {
  name          = "${var.project_id}-healthcare-processed"
  location      = var.region
  force_destroy = true
  
  versioning {
    enabled = true
  }
  
  lifecycle_rule {
    condition {
      age = 90  # Keep processed data for 90 days
    }
    action {
      type = "Delete"
    }
  }
  
  uniform_bucket_level_access = true
  
  encryption {
    default_kms_key_name = google_kms_crypto_key.healthcare_key.id
  }
}

resource "google_storage_bucket" "curated_zone" {
  name          = "${var.project_id}-healthcare-curated"
  location      = var.region
  force_destroy = true
  
  versioning {
    enabled = true
  }
  
  lifecycle_rule {
    condition {
      age = 2555  # Keep curated data for 7 years (HIPAA requirement)
    }
    action {
      type = "Delete"
    }
  }
  
  uniform_bucket_level_access = true
  
  encryption {
    default_kms_key_name = google_kms_crypto_key.healthcare_key.id
  }
}

# BigQuery Datasets
resource "google_bigquery_dataset" "raw_dataset" {
  dataset_id  = "healthcare_raw"
  description = "Raw healthcare data from various sources"
  location    = var.region
  
  # Enable encryption with Cloud KMS
  encryption_configuration {
    kms_key_name = google_kms_crypto_key.healthcare_key.id
  }
  
  # Access control
  access {
    role          = "OWNER"
    user_by_email = var.data_engineer_email
  }
  
  access {
    role          = "READER"
    special_group = "projectReaders"
  }
}

resource "google_bigquery_dataset" "processed_dataset" {
  dataset_id  = "healthcare_processed"
  description = "Processed and cleaned healthcare data"
  location    = var.region
  
  encryption_configuration {
    kms_key_name = google_kms_crypto_key.healthcare_key.id
  }
  
  access {
    role          = "OWNER"
    user_by_email = var.data_engineer_email
  }
}

resource "google_bigquery_dataset" "curated_dataset" {
  dataset_id  = "healthcare_curated"
  description = "Analytics-ready healthcare data"
  location    = var.region
  
  encryption_configuration {
    kms_key_name = google_kms_crypto_key.healthcare_key.id
  }
  
  access {
    role          = "OWNER"
    user_by_email = var.data_engineer_email
  }
}

# Pub/Sub Topics
resource "google_pubsub_topic" "iot_data_topic" {
  name = "healthcare-iot-data"
  
  # Enable encryption
  kms_key_name = google_kms_crypto_key.healthcare_key.id
}

resource "google_pubsub_topic" "claims_data_topic" {
  name = "healthcare-claims-data"
  
  kms_key_name = google_kms_crypto_key.healthcare_key.id
}

resource "google_pubsub_topic" "ehr_data_topic" {
  name = "healthcare-ehr-data"
  
  kms_key_name = google_kms_crypto_key.healthcare_key.id
}

# Pub/Sub Subscriptions
resource "google_pubsub_subscription" "iot_data_subscription" {
  name  = "healthcare-iot-data-sub"
  topic = google_pubsub_topic.iot_data_topic.name
  
  # Configure for real-time processing
  ack_deadline_seconds = 20
  
  expiration_policy {
    ttl = "2678400s"  # 31 days
  }
}

resource "google_pubsub_subscription" "claims_data_subscription" {
  name  = "healthcare-claims-data-sub"
  topic = google_pubsub_topic.claims_data_topic.name
  
  ack_deadline_seconds = 20
  
  expiration_policy {
    ttl = "2678400s"
  }
}

# Service Accounts
resource "google_service_account" "dataflow_sa" {
  account_id   = "healthcare-dataflow-sa"
  display_name = "Healthcare Dataflow Service Account"
  description  = "Service account for Dataflow jobs"
}

resource "google_service_account" "composer_sa" {
  account_id   = "healthcare-composer-sa"
  display_name = "Healthcare Composer Service Account"
  description  = "Service account for Cloud Composer"
}

resource "google_service_account" "ingestion_sa" {
  account_id   = "healthcare-ingestion-sa"
  display_name = "Healthcare Data Ingestion Service Account"
  description  = "Service account for data ingestion"
}

# IAM Roles for Service Accounts
resource "google_project_iam_member" "dataflow_worker" {
  project = var.project_id
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

resource "google_project_iam_member" "dataflow_admin" {
  project = var.project_id
  role    = "roles/dataflow.admin"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

resource "google_project_iam_member" "storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

resource "google_project_iam_member" "bigquery_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

resource "google_project_iam_member" "pubsub_admin" {
  project = var.project_id
  role    = "roles/pubsub.admin"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

resource "google_project_iam_member" "composer_worker" {
  project = var.project_id
  role    = "roles/composer.worker"
  member  = "serviceAccount:${google_service_account.composer_sa.email}"
}

resource "google_project_iam_member" "composer_admin" {
  project = var.project_id
  role    = "roles/composer.admin"
  member  = "serviceAccount:${google_service_account.composer_sa.email}"
}

# Cloud Composer Environment
resource "google_composer_environment" "healthcare_composer" {
  name   = "healthcare-composer"
  region = var.region
  
  config {
    software_config {
      image_version = "composer-2.0.31-airflow-2.2.5"
      
      # Airflow configuration overrides
      airflow_config_overrides = {
        core-dags_are_paused_at_creation = "True"
        core-max_active_runs_per_dag     = "16"
        core-parallelism                 = "32"
        core-dag_concurrency            = "16"
      }
      
      # Environment variables
      env_variables = {
        ENVIRONMENT = "production"
        PROJECT_ID  = var.project_id
        REGION      = var.region
      }
    }
    
    node_config {
      network    = google_compute_network.healthcare_vpc.id
      subnetwork = google_compute_subnetwork.private_subnet.id
      
      # Use private IP for security
      ip_allocation_policy {
        use_ip_aliases = true
        cluster_ipv4_cidr_block = "/16"
        services_ipv4_cidr_block = "/22"
      }
      
      service_account = google_service_account.composer_sa.email
    }
    
    private_environment_config {
      enable_private_endpoint = true
      cloud_sql_ipv4_cidr     = "10.0.0.0/24"
    }
  }
  
  depends_on = [google_project_service.required_apis]
}

# Logging Sink for Audit Logs
resource "google_logging_project_sink" "audit_logs" {
  name        = "healthcare-audit-logs"
  destination = "storage.googleapis.com/${google_storage_bucket.raw_zone.name}/audit-logs"
  
  filter = "resource.type=\"gce_instance\" OR resource.type=\"gcs_bucket\" OR resource.type=\"bigquery_dataset\" OR resource.type=\"pubsub_topic\""
  
  unique_writer_identity = true
}

# Monitoring Workspace
resource "google_monitoring_workspace" "healthcare_workspace" {
  display_name = "Healthcare Data Lakehouse Monitoring"
  project      = var.project_id
} 