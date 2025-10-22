{{ config(
    materialized='table',
    pre_hook="INSERT INTO {{ this.schema }}.go_process_audit (audit_id, pipeline_run_id, pipeline_name, execution_start_time, execution_status, source_system, target_table, load_date) SELECT UUID_STRING(), '{{ invocation_id }}', 'go_license_dimension', CURRENT_TIMESTAMP(), 'STARTED', 'ZOOM_SILVER', 'go_license_dimension', CURRENT_DATE()",
    post_hook="UPDATE {{ this.schema }}.go_process_audit SET execution_end_time = CURRENT_TIMESTAMP(), execution_status = 'SUCCESS', execution_duration_seconds = DATEDIFF('second', execution_start_time, CURRENT_TIMESTAMP()), records_processed = (SELECT COUNT(*) FROM {{ this }}), update_date = CURRENT_DATE() WHERE pipeline_run_id = '{{ invocation_id }}' AND target_table = 'go_license_dimension'"
) }}

-- License Dimension with SCD Type 2 Implementation
WITH source_data AS (
    SELECT 
        license_id,
        license_type,
        start_date,
        end_date,
        load_date,
        update_date,
        source_system
    FROM {{ source('zoom_silver', 'si_licenses') }}
    WHERE license_id IS NOT NULL
),

transformed_data AS (
    SELECT 
        COALESCE(license_id, UUID_STRING()) AS license_id,
        UPPER(TRIM(COALESCE(license_type, 'UNKNOWN'))) AS license_type_code,
        CASE 
            WHEN UPPER(COALESCE(license_type, '')) = 'BASIC' THEN 'Basic Plan'
            WHEN UPPER(COALESCE(license_type, '')) = 'PRO' THEN 'Professional Plan'
            WHEN UPPER(COALESCE(license_type, '')) = 'BUSINESS' THEN 'Business Plan'
            WHEN UPPER(COALESCE(license_type, '')) = 'ENTERPRISE' THEN 'Enterprise Plan'
            ELSE 'Other Plan'
        END AS license_type_name,
        COALESCE(start_date, '1900-01-01'::DATE) AS license_start_date,
        COALESCE(end_date, '2099-12-31'::DATE) AS license_end_date,
        CASE 
            WHEN CURRENT_DATE() < COALESCE(start_date, '1900-01-01'::DATE) THEN 'Future'
            WHEN CURRENT_DATE() > COALESCE(end_date, '2099-12-31'::DATE) THEN 'Expired'
            WHEN DATEDIFF('day', CURRENT_DATE(), COALESCE(end_date, '2099-12-31'::DATE)) <= 30 THEN 'Expiring Soon'
            ELSE 'Active'
        END AS license_status,
        DATEDIFF('day', COALESCE(start_date, '1900-01-01'::DATE), COALESCE(end_date, '2099-12-31'::DATE)) AS license_duration_days,
        CASE 
            WHEN CURRENT_DATE() BETWEEN COALESCE(start_date, '1900-01-01'::DATE) AND COALESCE(end_date, '2099-12-31'::DATE) THEN TRUE 
            ELSE FALSE 
        END AS is_active,
        COALESCE(load_date, CURRENT_DATE()) AS effective_start_date,
        NULL AS effective_end_date,
        TRUE AS is_current,
        CURRENT_TIMESTAMP() AS created_timestamp,
        COALESCE(update_date, CURRENT_TIMESTAMP()) AS updated_timestamp,
        COALESCE(source_system, 'ZOOM_SILVER') AS source_system
    FROM source_data
)

SELECT 
    UUID_STRING() AS license_dimension_id,
    license_id,
    license_type_code,
    license_type_name,
    license_start_date,
    license_end_date,
    license_status,
    license_duration_days,
    is_active,
    effective_start_date,
    effective_end_date,
    is_current,
    created_timestamp,
    updated_timestamp,
    source_system,
    CURRENT_DATE() AS load_date,
    CURRENT_DATE() AS update_date
FROM transformed_data
