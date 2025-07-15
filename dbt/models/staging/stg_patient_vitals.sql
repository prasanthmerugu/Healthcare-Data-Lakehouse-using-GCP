{{
  config(
    materialized='view',
    tags=['staging', 'vitals']
  )
}}

WITH source AS (
  SELECT * FROM {{ source('healthcare_processed', 'patient_vitals') }}
),

cleaned AS (
  SELECT
    -- Primary identifiers
    patient_id,
    device_id,
    location,
    
    -- Timestamps
    TIMESTAMP(timestamp) as measurement_timestamp,
    TIMESTAMP(processed_at) as processed_timestamp,
    
    -- Vital signs with validation
    CASE 
      WHEN heart_rate BETWEEN {{ var('min_heart_rate') }} AND {{ var('max_heart_rate') }}
      THEN heart_rate
      ELSE NULL
    END as heart_rate,
    
    CASE 
      WHEN blood_pressure_systolic BETWEEN 70 AND 250
      THEN blood_pressure_systolic
      ELSE NULL
    END as blood_pressure_systolic,
    
    CASE 
      WHEN blood_pressure_diastolic BETWEEN 40 AND 150
      THEN blood_pressure_diastolic
      ELSE NULL
    END as blood_pressure_diastolic,
    
    CASE 
      WHEN temperature BETWEEN {{ var('min_temperature') }} AND {{ var('max_temperature') }}
      THEN temperature
      ELSE NULL
    END as temperature,
    
    CASE 
      WHEN oxygen_saturation BETWEEN 70 AND 100
      THEN oxygen_saturation
      ELSE NULL
    END as oxygen_saturation,
    
    CASE 
      WHEN respiratory_rate BETWEEN 8 AND 40
      THEN respiratory_rate
      ELSE NULL
    END as respiratory_rate,
    
    -- Derived fields
    heart_rate_category,
    hour_of_day,
    day_of_week,
    data_quality_score,
    
    -- Metadata
    pipeline_version,
    
    -- Data quality flags
    CASE 
      WHEN heart_rate NOT BETWEEN {{ var('min_heart_rate') }} AND {{ var('max_heart_rate') }}
      THEN TRUE
      ELSE FALSE
    END as heart_rate_anomaly,
    
    CASE 
      WHEN temperature NOT BETWEEN {{ var('min_temperature') }} AND {{ var('max_temperature') }}
      THEN TRUE
      ELSE FALSE
    END as temperature_anomaly,
    
    CASE 
      WHEN oxygen_saturation < 95
      THEN TRUE
      ELSE FALSE
    END as low_oxygen_alert,
    
    CASE 
      WHEN heart_rate > {{ var('elevated_heart_rate_threshold') }}
      THEN TRUE
      ELSE FALSE
    END as elevated_heart_rate_alert,
    
    CASE 
      WHEN heart_rate < {{ var('low_heart_rate_threshold') }}
      THEN TRUE
      ELSE FALSE
    END as low_heart_rate_alert,
    
    CASE 
      WHEN temperature > {{ var('fever_temperature_threshold') }}
      THEN TRUE
      ELSE FALSE
    END as fever_alert
    
  FROM source
  WHERE patient_id IS NOT NULL
    AND timestamp IS NOT NULL
),

final AS (
  SELECT
    *,
    -- Additional derived metrics
    CASE 
      WHEN blood_pressure_systolic IS NOT NULL AND blood_pressure_diastolic IS NOT NULL
      THEN blood_pressure_systolic - blood_pressure_diastolic
      ELSE NULL
    END as pulse_pressure,
    
    CASE 
      WHEN heart_rate IS NOT NULL AND blood_pressure_systolic IS NOT NULL
      THEN (heart_rate * blood_pressure_systolic) / 100
      ELSE NULL
    END as rate_pressure_product,
    
    -- Time-based categorizations
    CASE 
      WHEN hour_of_day BETWEEN 6 AND 18 THEN 'day_shift'
      ELSE 'night_shift'
    END as shift_category,
    
    CASE 
      WHEN day_of_week IN ('Saturday', 'Sunday') THEN 'weekend'
      ELSE 'weekday'
    END as day_category,
    
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