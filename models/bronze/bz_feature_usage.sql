{{
  config(
    materialized='table',
    pre_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, status) VALUES ('bz_feature_usage', CURRENT_TIMESTAMP(), 'DBT_BRONZE_PIPELINE', 'STARTED'){% endif %}",
    post_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, status) VALUES ('bz_feature_usage', CURRENT_TIMESTAMP(), 'DBT_BRONZE_PIPELINE', 'SUCCESS'){% endif %}"
  )
}}

/*
=============================================================================
BRONZE LAYER FEATURE USAGE MODEL
=============================================================================
Purpose: Transform raw feature usage data to bronze layer with data quality checks
Source: ZOOM_RAW_SCHEMA.FEATURE_USAGE
Target: ZOOM_BRONZE_SCHEMA.bz_feature_usage
Transformation: 1:1 mapping with audit columns
=============================================================================
*/

WITH source_data AS (
    -- Extract raw feature usage data with basic validation
    SELECT 
        usage_id,
        feature_name,
        usage_date,
        usage_count,
        meeting_id,
        source_system,
        load_timestamp,
        update_timestamp
    FROM {{ source('zoom_raw_schema', 'feature_usage') }}
    WHERE usage_id IS NOT NULL  -- Basic data quality check
),

data_quality_checks AS (
    -- Apply data quality transformations
    SELECT 
        usage_id,
        TRIM(UPPER(feature_name)) AS feature_name,
        usage_date,
        CASE 
            WHEN usage_count < 0 THEN 0 
            ELSE COALESCE(usage_count, 0) 
        END AS usage_count,  -- Ensure non-negative usage count
        meeting_id,
        COALESCE(source_system, 'ZOOM_RAW_SCHEMA') AS source_system,
        COALESCE(load_timestamp, CURRENT_TIMESTAMP()) AS load_timestamp,
        COALESCE(update_timestamp, CURRENT_TIMESTAMP()) AS update_timestamp
    FROM source_data
)

-- Final select with bronze layer structure
SELECT 
    usage_id,
    feature_name,
    usage_date,
    usage_count,
    meeting_id,
    source_system,
    load_timestamp,
    update_timestamp
FROM data_quality_checks
