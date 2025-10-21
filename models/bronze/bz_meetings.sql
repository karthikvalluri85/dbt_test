{{
  config(
    materialized='table',
    pre_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, status) VALUES ('bz_meetings', CURRENT_TIMESTAMP(), 'DBT_BRONZE_PIPELINE', 'STARTED'){% endif %}",
    post_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, status) VALUES ('bz_meetings', CURRENT_TIMESTAMP(), 'DBT_BRONZE_PIPELINE', 'SUCCESS'){% endif %}"
  )
}}

/*
=============================================================================
BRONZE LAYER MEETINGS MODEL
=============================================================================
Purpose: Transform raw meetings data to bronze layer with data quality checks
Source: ZOOM_RAW_SCHEMA.MEETINGS
Target: ZOOM_BRONZE_SCHEMA.bz_meetings
Transformation: 1:1 mapping with audit columns
=============================================================================
*/

WITH source_data AS (
    -- Extract raw meetings data with basic validation
    SELECT 
        meeting_id,
        meeting_topic,
        duration_minutes,
        start_time,
        end_time,
        host_id,
        source_system,
        load_timestamp,
        update_timestamp
    FROM {{ source('zoom_raw_schema', 'meetings') }}
    WHERE meeting_id IS NOT NULL  -- Basic data quality check
),

data_quality_checks AS (
    -- Apply data quality transformations
    SELECT 
        meeting_id,
        TRIM(meeting_topic) AS meeting_topic,
        CASE 
            WHEN duration_minutes < 0 THEN 0 
            ELSE duration_minutes 
        END AS duration_minutes,  -- Ensure non-negative duration
        start_time,
        end_time,
        host_id,
        COALESCE(source_system, 'ZOOM_RAW_SCHEMA') AS source_system,
        COALESCE(load_timestamp, CURRENT_TIMESTAMP()) AS load_timestamp,
        COALESCE(update_timestamp, CURRENT_TIMESTAMP()) AS update_timestamp
    FROM source_data
)

-- Final select with bronze layer structure
SELECT 
    meeting_id,
    meeting_topic,
    duration_minutes,
    start_time,
    end_time,
    host_id,
    source_system,
    load_timestamp,
    update_timestamp
FROM data_quality_checks
