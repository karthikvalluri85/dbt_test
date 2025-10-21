{{
  config(
    materialized='table',
    pre_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, status) VALUES ('bz_licenses', CURRENT_TIMESTAMP(), 'DBT_BRONZE_PIPELINE', 'STARTED'){% endif %}",
    post_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, status) VALUES ('bz_licenses', CURRENT_TIMESTAMP(), 'DBT_BRONZE_PIPELINE', 'SUCCESS'){% endif %}"
  )
}}

/*
=============================================================================
BRONZE LAYER LICENSES MODEL
=============================================================================
Purpose: Transform raw licenses data to bronze layer with data quality checks
Source: ZOOM_RAW_SCHEMA.LICENSES
Target: ZOOM_BRONZE_SCHEMA.bz_licenses
Transformation: 1:1 mapping with audit columns
=============================================================================
*/

WITH source_data AS (
    -- Extract raw licenses data with basic validation
    SELECT 
        license_id,
        license_type,
        start_date,
        end_date,
        assigned_to_user_id,
        source_system,
        load_timestamp,
        update_timestamp
    FROM {{ source('zoom_raw', 'licenses') }}
    WHERE license_id IS NOT NULL  -- Basic data quality check
),

data_quality_checks AS (
    -- Apply data quality transformations
    SELECT 
        license_id,
        TRIM(UPPER(license_type)) AS license_type,
        start_date,
        end_date,
        assigned_to_user_id,
        COALESCE(source_system, 'ZOOM_RAW_SCHEMA') AS source_system,
        COALESCE(load_timestamp, CURRENT_TIMESTAMP()) AS load_timestamp,
        COALESCE(update_timestamp, CURRENT_TIMESTAMP()) AS update_timestamp
    FROM source_data
    WHERE start_date <= COALESCE(end_date, '9999-12-31')  -- Validate date logic
)

-- Final select with bronze layer structure
SELECT 
    license_id,
    license_type,
    start_date,
    end_date,
    assigned_to_user_id,
    source_system,
    load_timestamp,
    update_timestamp
FROM data_quality_checks
