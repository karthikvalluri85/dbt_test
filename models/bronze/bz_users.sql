{{
  config(
    materialized='table',
    pre_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, status) VALUES ('bz_users', CURRENT_TIMESTAMP(), 'DBT_BRONZE_PIPELINE', 'STARTED'){% endif %}",
    post_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, status) VALUES ('bz_users', CURRENT_TIMESTAMP(), 'DBT_BRONZE_PIPELINE', 'SUCCESS'){% endif %}"
  )
}}

/*
=============================================================================
BRONZE LAYER USERS MODEL
=============================================================================
Purpose: Transform raw users data to bronze layer with data quality checks
Source: ZOOM_RAW_SCHEMA.USERS
Target: ZOOM_BRONZE_SCHEMA.bz_users
Transformation: 1:1 mapping with audit columns
=============================================================================
*/

WITH source_data AS (
    -- Extract raw users data with basic validation
    SELECT 
        user_id,
        email,
        user_name,
        plan_type,
        company,
        source_system,
        load_timestamp,
        update_timestamp
    FROM {{ source('zoom_raw_schema', 'users') }}
    WHERE user_id IS NOT NULL  -- Basic data quality check
),

data_quality_checks AS (
    -- Apply data quality transformations
    SELECT 
        user_id,
        TRIM(UPPER(email)) AS email,  -- Standardize email format
        TRIM(user_name) AS user_name,
        TRIM(UPPER(plan_type)) AS plan_type,
        TRIM(company) AS company,
        COALESCE(source_system, 'ZOOM_RAW_SCHEMA') AS source_system,
        COALESCE(load_timestamp, CURRENT_TIMESTAMP()) AS load_timestamp,
        COALESCE(update_timestamp, CURRENT_TIMESTAMP()) AS update_timestamp
    FROM source_data
)

-- Final select with bronze layer structure
SELECT 
    user_id,
    email,
    user_name,
    plan_type,
    company,
    source_system,
    load_timestamp,
    update_timestamp
FROM data_quality_checks
