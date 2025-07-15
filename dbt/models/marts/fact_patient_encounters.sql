{{
  config(
    materialized='table',
    tags=['marts', 'fact', 'encounters']
  )
}}

WITH vitals AS (
  SELECT 
    patient_id,
    measurement_timestamp,
    device_id,
    location,
    heart_rate,
    blood_pressure_systolic,
    blood_pressure_diastolic,
    temperature,
    oxygen_saturation,
    respiratory_rate,
    heart_rate_category,
    pulse_pressure,
    rate_pressure_product,
    shift_category,
    day_category,
    quality_category,
    -- Alert flags
    low_oxygen_alert,
    elevated_heart_rate_alert,
    low_heart_rate_alert,
    fever_alert
  FROM {{ ref('stg_patient_vitals') }}
  WHERE measurement_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ var('lookback_days') }} DAY)
),

claims AS (
  SELECT 
    patient_id,
    claim_id,
    service_date,
    submission_date,
    provider_id,
    total_amount,
    insurance_type,
    insurance_category,
    claim_status,
    lifecycle_stage,
    amount_category,
    value_category,
    processing_days,
    processing_efficiency,
    diagnosis_count,
    procedure_count,
    daily_claim_value,
    -- Business flags
    denied_claim,
    paid_claim,
    pending_claim,
    high_value_claim
  FROM {{ ref('stg_insurance_claims') }}
  WHERE service_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('lookback_days') }} DAY)
),

ehr AS (
  SELECT 
    patient_id,
    record_id,
    visit_date,
    provider_id,
    diagnosis,
    treatment,
    medications,
    lab_results,
    medication_count,
    lab_test_count,
    notes
  FROM {{ ref('stg_ehr_records') }}
  WHERE visit_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('lookback_days') }} DAY)
),

-- Combine vitals with claims based on patient and date proximity
vitals_with_claims AS (
  SELECT 
    v.*,
    c.claim_id,
    c.service_date,
    c.submission_date,
    c.provider_id as claim_provider_id,
    c.total_amount,
    c.insurance_type,
    c.insurance_category,
    c.claim_status,
    c.lifecycle_stage,
    c.amount_category,
    c.value_category,
    c.processing_days,
    c.processing_efficiency,
    c.diagnosis_count,
    c.procedure_count,
    c.daily_claim_value,
    c.denied_claim,
    c.paid_claim,
    c.pending_claim,
    c.high_value_claim,
    -- Calculate days between vitals measurement and claim service
    DATE_DIFF(DATE(v.measurement_timestamp), c.service_date, DAY) as days_from_service,
    -- Rank claims by proximity to vitals measurement
    ROW_NUMBER() OVER (
      PARTITION BY v.patient_id, v.measurement_timestamp 
      ORDER BY ABS(DATE_DIFF(DATE(v.measurement_timestamp), c.service_date, DAY))
    ) as claim_proximity_rank
  FROM vitals v
  LEFT JOIN claims c 
    ON v.patient_id = c.patient_id
    AND ABS(DATE_DIFF(DATE(v.measurement_timestamp), c.service_date, DAY)) <= 7  -- Within 7 days
),

-- Combine with EHR data
encounters AS (
  SELECT 
    vc.*,
    e.record_id,
    e.visit_date,
    e.provider_id as ehr_provider_id,
    e.diagnosis,
    e.treatment,
    e.medications,
    e.lab_results,
    e.medication_count,
    e.lab_test_count,
    e.notes,
    -- Calculate days between vitals measurement and EHR visit
    DATE_DIFF(DATE(vc.measurement_timestamp), e.visit_date, DAY) as days_from_visit,
    -- Rank EHR records by proximity to vitals measurement
    ROW_NUMBER() OVER (
      PARTITION BY vc.patient_id, vc.measurement_timestamp 
      ORDER BY ABS(DATE_DIFF(DATE(vc.measurement_timestamp), e.visit_date, DAY))
    ) as ehr_proximity_rank
  FROM vitals_with_claims vc
  LEFT JOIN ehr e 
    ON vc.patient_id = e.patient_id
    AND ABS(DATE_DIFF(DATE(vc.measurement_timestamp), e.visit_date, DAY)) <= 7  -- Within 7 days
),

-- Create encounter-level aggregations
encounter_metrics AS (
  SELECT 
    patient_id,
    measurement_timestamp,
    device_id,
    location,
    
    -- Vitals metrics
    heart_rate,
    blood_pressure_systolic,
    blood_pressure_diastolic,
    temperature,
    oxygen_saturation,
    respiratory_rate,
    heart_rate_category,
    pulse_pressure,
    rate_pressure_product,
    shift_category,
    day_category,
    quality_category,
    
    -- Alert flags
    low_oxygen_alert,
    elevated_heart_rate_alert,
    low_heart_rate_alert,
    fever_alert,
    
    -- Claims data (closest match)
    CASE WHEN claim_proximity_rank = 1 THEN claim_id END as primary_claim_id,
    CASE WHEN claim_proximity_rank = 1 THEN service_date END as primary_service_date,
    CASE WHEN claim_proximity_rank = 1 THEN total_amount END as primary_claim_amount,
    CASE WHEN claim_proximity_rank = 1 THEN insurance_type END as primary_insurance_type,
    CASE WHEN claim_proximity_rank = 1 THEN claim_status END as primary_claim_status,
    CASE WHEN claim_proximity_rank = 1 THEN processing_days END as primary_processing_days,
    
    -- EHR data (closest match)
    CASE WHEN ehr_proximity_rank = 1 THEN record_id END as primary_ehr_record_id,
    CASE WHEN ehr_proximity_rank = 1 THEN visit_date END as primary_visit_date,
    CASE WHEN ehr_proximity_rank = 1 THEN diagnosis END as primary_diagnosis,
    CASE WHEN ehr_proximity_rank = 1 THEN treatment END as primary_treatment,
    CASE WHEN ehr_proximity_rank = 1 THEN medication_count END as primary_medication_count,
    CASE WHEN ehr_proximity_rank = 1 THEN lab_test_count END as primary_lab_test_count,
    
    -- Encounter-level aggregations
    COUNT(DISTINCT claim_id) as total_claims,
    COUNT(DISTINCT record_id) as total_ehr_records,
    SUM(total_amount) as total_claim_value,
    AVG(processing_days) as avg_processing_days,
    SUM(CASE WHEN denied_claim THEN 1 ELSE 0 END) as denied_claims_count,
    SUM(CASE WHEN paid_claim THEN 1 ELSE 0 END) as paid_claims_count,
    SUM(CASE WHEN pending_claim THEN 1 ELSE 0 END) as pending_claims_count,
    SUM(CASE WHEN high_value_claim THEN 1 ELSE 0 END) as high_value_claims_count,
    
    -- Encounter complexity score
    (
      COALESCE(COUNT(DISTINCT claim_id), 0) * 0.3 +
      COALESCE(COUNT(DISTINCT record_id), 0) * 0.2 +
      COALESCE(SUM(diagnosis_count), 0) * 0.2 +
      COALESCE(SUM(procedure_count), 0) * 0.2 +
      COALESCE(SUM(medication_count), 0) * 0.1
    ) as encounter_complexity_score,
    
    -- Risk indicators
    CASE 
      WHEN low_oxygen_alert OR elevated_heart_rate_alert OR low_heart_rate_alert OR fever_alert
      THEN 'high_risk'
      WHEN heart_rate_category != 'normal' OR temperature > 37.5
      THEN 'medium_risk'
      ELSE 'low_risk'
    END as risk_level,
    
    -- Encounter type
    CASE 
      WHEN total_claims > 0 AND total_ehr_records > 0 THEN 'comprehensive'
      WHEN total_claims > 0 THEN 'claims_only'
      WHEN total_ehr_records > 0 THEN 'ehr_only'
      ELSE 'vitals_only'
    END as encounter_type,
    
    -- Timestamps
    CURRENT_TIMESTAMP() as created_at,
    CURRENT_TIMESTAMP() as updated_at
    
  FROM encounters
  GROUP BY 
    patient_id, measurement_timestamp, device_id, location,
    heart_rate, blood_pressure_systolic, blood_pressure_diastolic,
    temperature, oxygen_saturation, respiratory_rate, heart_rate_category,
    pulse_pressure, rate_pressure_product, shift_category, day_category,
    quality_category, low_oxygen_alert, elevated_heart_rate_alert,
    low_heart_rate_alert, fever_alert, claim_proximity_rank, ehr_proximity_rank,
    claim_id, service_date, total_amount, insurance_type, claim_status,
    processing_days, record_id, visit_date, diagnosis, treatment,
    medication_count, lab_test_count
)

SELECT * FROM encounter_metrics 