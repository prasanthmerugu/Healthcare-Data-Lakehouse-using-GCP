{{
  config(
    materialized='view',
    tags=['staging', 'claims']
  )
}}

WITH source AS (
  SELECT * FROM {{ source('healthcare_processed', 'insurance_claims') }}
),

cleaned AS (
  SELECT
    -- Primary identifiers
    claim_id,
    patient_id,
    provider_id,
    
    -- Dates
    DATE(service_date) as service_date,
    DATE(submission_date) as submission_date,
    TIMESTAMP(processed_at) as processed_timestamp,
    
    -- Financial data with validation
    CASE 
      WHEN total_amount BETWEEN {{ var('min_claim_amount') }} AND {{ var('max_claim_amount') }}
      THEN total_amount
      ELSE NULL
    END as total_amount,
    
    -- Categorical data
    insurance_type,
    claim_status,
    amount_category,
    
    -- Arrays (stored as strings in BigQuery)
    diagnosis_codes,
    procedure_codes,
    
    -- Derived fields
    processing_days,
    data_quality_score,
    
    -- Metadata
    pipeline_version,
    
    -- Data quality flags
    CASE 
      WHEN total_amount NOT BETWEEN {{ var('min_claim_amount') }} AND {{ var('max_claim_amount') }}
      THEN TRUE
      ELSE FALSE
    END as amount_anomaly,
    
    CASE 
      WHEN processing_days < 0
      THEN TRUE
      ELSE FALSE
    END as negative_processing_days,
    
    CASE 
      WHEN processing_days > 365
      THEN TRUE
      ELSE FALSE
    END as excessive_processing_days,
    
    CASE 
      WHEN total_amount > {{ var('high_claim_amount_threshold') }}
      THEN TRUE
      ELSE FALSE
    END as high_value_claim,
    
    -- Business logic flags
    CASE 
      WHEN claim_status = 'Denied'
      THEN TRUE
      ELSE FALSE
    END as denied_claim,
    
    CASE 
      WHEN claim_status = 'Paid'
      THEN TRUE
      ELSE FALSE
    END as paid_claim,
    
    CASE 
      WHEN claim_status IN ('Submitted', 'Under Review')
      THEN TRUE
      ELSE FALSE
    END as pending_claim
    
  FROM source
  WHERE claim_id IS NOT NULL
    AND patient_id IS NOT NULL
    AND service_date IS NOT NULL
),

final AS (
  SELECT
    *,
    -- Additional derived metrics
    CASE 
      WHEN total_amount IS NOT NULL AND processing_days > 0
      THEN total_amount / processing_days
      ELSE NULL
    END as daily_claim_value,
    
    -- Claim complexity indicators
    CASE 
      WHEN diagnosis_codes IS NOT NULL 
      THEN ARRAY_LENGTH(SPLIT(diagnosis_codes, ','))
      ELSE 0
    END as diagnosis_count,
    
    CASE 
      WHEN procedure_codes IS NOT NULL 
      THEN ARRAY_LENGTH(SPLIT(procedure_codes, ','))
      ELSE 0
    END as procedure_count,
    
    -- Processing efficiency
    CASE 
      WHEN processing_days <= 7 THEN 'excellent'
      WHEN processing_days <= 30 THEN 'good'
      WHEN processing_days <= 90 THEN 'fair'
      ELSE 'poor'
    END as processing_efficiency,
    
    -- Financial categorization
    CASE 
      WHEN total_amount < 100 THEN 'low_value'
      WHEN total_amount < 1000 THEN 'medium_value'
      WHEN total_amount < 10000 THEN 'high_value'
      ELSE 'very_high_value'
    END as value_category,
    
    -- Insurance type grouping
    CASE 
      WHEN insurance_type IN ('Medicare', 'Medicaid') THEN 'government'
      WHEN insurance_type = 'Private' THEN 'private'
      ELSE 'self_pay'
    END as insurance_category,
    
    -- Claim lifecycle stage
    CASE 
      WHEN claim_status = 'Submitted' THEN 'submitted'
      WHEN claim_status = 'Under Review' THEN 'reviewing'
      WHEN claim_status = 'Approved' THEN 'approved'
      WHEN claim_status = 'Denied' THEN 'denied'
      WHEN claim_status = 'Paid' THEN 'paid'
      ELSE 'unknown'
    END as lifecycle_stage,
    
    -- Data quality categorization
    CASE 
      WHEN data_quality_score >= 0.9 THEN 'excellent'
      WHEN data_quality_score >= 0.7 THEN 'good'
      WHEN data_quality_score >= 0.5 THEN 'fair'
      ELSE 'poor'
    END as quality_category
    
  FROM cleaned
)

SELECT * FROM final 