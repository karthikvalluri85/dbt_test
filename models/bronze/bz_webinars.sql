{{
  config(
    materialized='table',
    pre_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, status) VALUES ('bz_webinars', CURRENT_TIMESTAMP(), 'DBT_BRONZE_PIPELINE', 'STARTED'){% endif %}",
    post_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, status) VALUES ('bz_webinars', CURRENT_TIMESTAMP(), 'DBT_BRONZE_PIPELINE', 'SUCCESS'){% endif %}"
  )
}}

/*
=============================================================================
BRONZE LAYER WEBINARS MODEL
=============================================================================
Purpose: Transform raw webinars data to bronze layer with data quality checks
Source: ZOOM_RAW_SCHEMA.WEBINARS
Target: ZOOM_BRONZE_SCHEMA.bz_webinars
Transformation: 1:1 mapping with audit columns
=============================================================================
*/

WITH source_data AS (
    -- Extract raw webinars data with basic validation
    SELECT 
        webinar_id,
        webinar_topic,
        start_time,
        end_time,
        registrants,
        host_id,
        source_system,
        load_timestamp,
        update_timestamp
    FROM {{ source('zoom_raw', 'webinars') }}
    WHERE webinar_id IS NOT NULL  -- Basic data quality check
),

data_quality_checks AS (
    -- Apply data quality transformations
    SELECT 
        webinar_id,
        TRIM(webinar_topic) AS webinar_topic,
        start_time,
        end_time,
        COALESCE(registrants, 0) AS registrants,  -- Handle null registrants
        host_id,
        COALESCE(source_system, 'ZOOM_RAW_SCHEMA') AS source_system,
        COALESCE(load_timestamp, CURRENT_TIMESTAMP()) AS load_timestamp,
        COALESCE(update_timestamp, CURRENT_TIMESTAMP()) AS update_timestamp
    FROM source_data
)

-- Final select with bronze layer structure
SELECT 
    webinar_id,
    webinar_topic,
    start_time,
    end_time,
    registrants,
    host_id,
    source_system,
    load_timestamp,
    update_timestamp
FROM data_quality_checks
