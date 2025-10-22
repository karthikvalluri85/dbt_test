{{ config(
    materialized='table',
    pre_hook="INSERT INTO {{ this.schema }}.go_process_audit (audit_id, pipeline_run_id, pipeline_name, execution_start_time, execution_status, source_system, target_table, load_date) SELECT UUID_STRING(), '{{ invocation_id }}', 'go_user_dimension', CURRENT_TIMESTAMP(), 'STARTED', 'ZOOM_SILVER', 'go_user_dimension', CURRENT_DATE()",
    post_hook="UPDATE {{ this.schema }}.go_process_audit SET execution_end_time = CURRENT_TIMESTAMP(), execution_status = 'SUCCESS', execution_duration_seconds = DATEDIFF('second', execution_start_time, CURRENT_TIMESTAMP()), records_processed = (SELECT COUNT(*) FROM {{ this }}), update_date = CURRENT_DATE() WHERE pipeline_run_id = '{{ invocation_id }}' AND target_table = 'go_user_dimension'"
) }}

-- User Dimension with SCD Type 2 Implementation
WITH source_data AS (
    SELECT 
        user_id,
        email,
        user_name,
        plan_type,
        company,
        load_date,
        update_date,
        source_system
    FROM {{ source('zoom_silver', 'si_users') }}
    WHERE user_id IS NOT NULL
),

transformed_data AS (
    SELECT 
        user_id,
        LOWER(TRIM(COALESCE(email, 'unknown@example.com'))) AS email_address,
        INITCAP(TRIM(COALESCE(user_name, 'Unknown User'))) AS user_full_name,
        CASE 
            WHEN UPPER(COALESCE(plan_type, '')) IN ('BASIC', 'FREE') THEN 'BASIC'
            WHEN UPPER(COALESCE(plan_type, '')) IN ('PRO', 'PROFESSIONAL') THEN 'PRO'
            WHEN UPPER(COALESCE(plan_type, '')) IN ('BUSINESS', 'BIZ') THEN 'BUSINESS'
            WHEN UPPER(COALESCE(plan_type, '')) IN ('ENTERPRISE', 'ENT') THEN 'ENTERPRISE'
            ELSE 'OTHER'
        END AS plan_type_code,
        INITCAP(TRIM(COALESCE(company, 'Not Specified'))) AS company_name,
        CASE 
            WHEN UPPER(COALESCE(plan_type, '')) IN ('BASIC', 'FREE') THEN 'Individual'
            WHEN UPPER(COALESCE(plan_type, '')) = 'PRO' THEN 'Professional'
            ELSE 'Enterprise'
        END AS user_segment,
        UPPER(SUBSTRING(COALESCE(email, 'unknown@example.com'), POSITION('@' IN COALESCE(email, 'unknown@example.com')) + 1)) AS email_domain,
        CASE 
            WHEN company IS NOT NULL AND TRIM(company) != '' THEN TRUE 
            ELSE FALSE 
        END AS is_business_user,
        COALESCE(load_date, CURRENT_DATE()) AS effective_start_date,
        NULL AS effective_end_date,
        TRUE AS is_current,
        CURRENT_TIMESTAMP() AS created_timestamp,
        COALESCE(update_date, CURRENT_TIMESTAMP()) AS updated_timestamp,
        COALESCE(source_system, 'ZOOM_SILVER') AS source_system
    FROM source_data
)

SELECT 
    UUID_STRING() AS user_dimension_id,
    user_id,
    email_address,
    user_full_name,
    plan_type_code,
    company_name,
    user_segment,
    email_domain,
    is_business_user,
    effective_start_date,
    effective_end_date,
    is_current,
    created_timestamp,
    updated_timestamp,
    source_system,
    CURRENT_DATE() AS load_date,
    CURRENT_DATE() AS update_date
FROM transformed_data
