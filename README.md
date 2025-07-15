# Healthcare Data Lakehouse using GCP

A comprehensive end-to-end data engineering project that unifies and processes multi-source healthcare datasets (EHR, claims, IoT device data) in a HIPAA-compliant architecture using GCP-native tools.

## 🏗️ Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │    │   Real-time     │    │   Batch Sources │
│                 │    │   IoT Devices   │    │                 │
│ • EHR Systems   │    │ • Heart Rate    │    │ • Claims Data   │
│ • Claims Data   │    │ • Temperature   │    │ • Patient Info  │
│ • IoT Devices   │    │ • Blood Pressure│    │ • Lab Results   │
└─────────┬───────┘    └─────────┬───────┘    └─────────┬───────┘
          │                      │                      │
          └──────────────────────┼──────────────────────┘
                                 │
                    ┌─────────────▼─────────────┐
                    │    Cloud Pub/Sub         │
                    │   (Message Queue)        │
                    └─────────────┬─────────────┘
                                  │
                    ┌─────────────▼─────────────┐
                    │    Cloud Dataflow        │
                    │   (Streaming/Batch ETL)  │
                    └─────────────┬─────────────┘
                                  │
                    ┌─────────────▼─────────────┐
                    │   Google Cloud Storage   │
                    │   (Data Lake)            │
                    │                          │
                    │ ┌─────────┬─────────────┐│
                    │ │  Raw    │  Processed ││
                    │ │  Zone   │   Zone     ││
                    │ └─────────┴─────────────┘│
                    └─────────────┬─────────────┘
                                  │
                    ┌─────────────▼─────────────┐
                    │      BigQuery            │
                    │   (Data Warehouse)       │
                    │                          │
                    │ ┌───────────────────────┐│
                    │ │   Curated Zone        ││
                    │ │ • Fact Tables         ││
                    │ │ • Dimension Tables    ││
                    │ │ • Analytics Views     ││
                    │ └───────────────────────┘│
                    └─────────────┬─────────────┘
                                  │
                    ┌─────────────▼─────────────┐
                    │      Looker              │
                    │   (BI & Analytics)       │
                    └───────────────────────────┘
```

## 🚀 Quick Start

### Prerequisites
- Google Cloud Platform account with billing enabled
- Terraform installed (v1.0+)
- Python 3.8+
- Google Cloud SDK
- Docker (for local development)

### Setup Instructions

1. **Clone and Navigate**
   ```bash
   cd "Healthcare Data Lakehouse using GCP"
   ```

2. **Initialize GCP Project**
   ```bash
   # Set your GCP project ID
   export PROJECT_ID="your-project-id"
   gcloud config set project $PROJECT_ID
   
   # Enable required APIs
   gcloud services enable \
     compute.googleapis.com \
     storage.googleapis.com \
     bigquery.googleapis.com \
     pubsub.googleapis.com \
     dataflow.googleapis.com \
     composer.googleapis.com \
     cloudkms.googleapis.com \
     logging.googleapis.com \
     monitoring.googleapis.com
   ```

3. **Deploy Infrastructure**
   ```bash
   cd terraform
   terraform init
   terraform plan -var="project_id=$PROJECT_ID"
   terraform apply -var="project_id=$PROJECT_ID"
   ```

4. **Deploy Data Pipeline**
   ```bash
   cd ../dataflow
   python deploy_pipelines.py
   ```

5. **Run dbt Models**
   ```bash
   cd ../dbt
   dbt deps
   dbt run
   ```

6. **Start Data Ingestion**
   ```bash
   cd ../ingestion
   python start_ingestion.py
   ```

## 📁 Project Structure

```
Healthcare Data Lakehouse using GCP/
├── README.md                           # This file
├── terraform/                          # Infrastructure as Code
│   ├── main.tf                        # Main Terraform configuration
│   ├── variables.tf                   # Variable definitions
│   ├── outputs.tf                     # Output values
│   └── modules/                       # Reusable Terraform modules
├── ingestion/                         # Data ingestion scripts
│   ├── publishers/                    # Pub/Sub publishers
│   ├── subscribers/                   # Pub/Sub subscribers
│   └── data_generators/               # Synthetic data generators
├── dataflow/                          # Apache Beam pipelines
│   ├── pipelines/                     # Dataflow job definitions
│   ├── templates/                     # Job templates
│   └── utils/                         # Pipeline utilities
├── dbt/                               # Data transformation models
│   ├── models/                        # dbt models
│   ├── macros/                        # Reusable macros
│   └── dbt_project.yml                # dbt configuration
├── airflow/                           # Orchestration DAGs
│   ├── dags/                          # Airflow DAG definitions
│   └── requirements.txt               # Python dependencies
├── security/                          # Security configurations
│   ├── iam/                           # IAM policies and roles
│   └── compliance/                    # HIPAA compliance checks
├── monitoring/                        # Monitoring and alerting
│   ├── dashboards/                    # Cloud Monitoring dashboards
│   └── alerts/                        # Alert policies
└── docs/                              # Documentation
    ├── architecture/                  # Architecture diagrams
    └── api/                           # API documentation
```

## 🔒 Security & Compliance

This project implements HIPAA-compliant security measures:

- **Encryption**: All data encrypted at rest and in transit
- **Access Control**: IAM with least privilege principles
- **Audit Logging**: Comprehensive logging for compliance
- **Network Security**: VPC with private service access
- **Data Governance**: Data lineage and cataloging

## 📊 Data Flow

1. **Ingestion**: Real-time IoT data and batch claims data ingested via Pub/Sub
2. **Processing**: Dataflow pipelines clean and transform data
3. **Storage**: Raw data stored in GCS, processed data in BigQuery
4. **Analytics**: dbt models create analytics-ready tables
5. **Visualization**: Looker dashboards provide insights

## 🛠️ Technologies Used

- **Infrastructure**: Terraform, Google Cloud Platform
- **Storage**: Google Cloud Storage, BigQuery
- **Processing**: Apache Beam (Dataflow)
- **Orchestration**: Apache Airflow (Cloud Composer)
- **Transformation**: dbt (data build tool)
- **Visualization**: Looker
- **Security**: Cloud KMS, IAM, VPC Service Controls

## 📈 Monitoring & Alerting

- Real-time pipeline monitoring via Cloud Monitoring
- Custom dashboards for operational metrics
- Alert policies for data quality and pipeline health
- Audit logs for compliance reporting

## 🚀 Demo Scenarios

1. **Real-time Patient Monitoring**: Stream IoT vitals data
2. **Claims Processing**: Batch process insurance claims
3. **Operational Analytics**: Hospital administration KPIs
4. **Compliance Reporting**: HIPAA audit trail generation

## 📝 License

This project is for educational and portfolio purposes.

## 🤝 Contributing

This is a demonstration project. For production use, ensure proper security reviews and compliance validation. 