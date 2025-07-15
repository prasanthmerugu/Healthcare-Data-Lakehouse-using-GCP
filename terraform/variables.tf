variable "project_id" {
  description = "The GCP project ID"
  type        = string
}

variable "region" {
  description = "The GCP region for resources"
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "The GCP zone for zonal resources"
  type        = string
  default     = "us-central1-a"
}

variable "organization_id" {
  description = "The GCP organization ID for VPC Service Controls"
  type        = string
  default     = ""
}

variable "data_engineer_email" {
  description = "Email of the data engineer for BigQuery access"
  type        = string
  default     = ""
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
  
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod."
  }
}

variable "composer_environment_size" {
  description = "Size of the Cloud Composer environment"
  type        = string
  default     = "ENVIRONMENT_SIZE_SMALL"
  
  validation {
    condition = contains([
      "ENVIRONMENT_SIZE_SMALL",
      "ENVIRONMENT_SIZE_MEDIUM",
      "ENVIRONMENT_SIZE_LARGE"
    ], var.composer_environment_size)
    error_message = "Composer environment size must be one of: ENVIRONMENT_SIZE_SMALL, ENVIRONMENT_SIZE_MEDIUM, ENVIRONMENT_SIZE_LARGE."
  }
}

variable "data_retention_days" {
  description = "Data retention period in days"
  type        = number
  default     = 365
  
  validation {
    condition     = var.data_retention_days >= 30 && var.data_retention_days <= 2555
    error_message = "Data retention must be between 30 and 2555 days (7 years for HIPAA)."
  }
}

variable "enable_audit_logging" {
  description = "Enable comprehensive audit logging"
  type        = bool
  default     = true
}

variable "enable_vpc_service_controls" {
  description = "Enable VPC Service Controls for data protection"
  type        = bool
  default     = true
}

variable "enable_encryption" {
  description = "Enable encryption at rest and in transit"
  type        = bool
  default     = true
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default = {
    project     = "healthcare-data-lakehouse"
    environment = "dev"
    managed_by  = "terraform"
    compliance  = "hipaa"
  }
} 