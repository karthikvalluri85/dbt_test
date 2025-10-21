{{
  config(
    materialized='table',
    pre_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, status) VALUES ('bz_participants', CURRENT_TIMESTAMP(), 'DBT_BRONZE_PIPELINE', 'STARTED'){% endif %}",
    post_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, status) VALUES ('bz_participants', CURRENT_TIMESTAMP(), 'DBT_BRONZE_PIPELINE', 'SUCCESS'){% endif %}"
  )
}}

/*
=============================================================================
BRONZE LAYER PARTICIPANTS MODEL
=============================================================================
Purpose: Transform raw participants data to bronze layer with data quality checks
Source: ZOOM_RAW_SCHEMA.PARTICIPANTS
Target: ZOOM_BRONZE_SCHEMA.bz_participants
Transformation: 1:1 mapping with audit columns
=============================================================================
*/

WITH source_data AS (
    -- Extract raw participants data with basic validation
    SELECT 
        participant_id,
        meeting_id,
        user_id,
        join_time,
        leave_time,
        source_system,
        load_timestamp,
        update_timestamp
    FROM {{ source('zoom_raw_schema', 'participants') }}
    WHERE participant_id IS NOT NULL  -- Basic data quality check
),

data_quality_checks AS (
    -- Apply data quality transformations
    SELECT 
        participant_id,
        meeting_id,
        user_id,
        join_time,
        leave_time,
        COALESCE(source_system, 'ZOOM_RAW_SCHEMA') AS source_system,
        COALESCE(load_timestamp, CURRENT_TIMESTAMP()) AS load_timestamp,
        COALESCE(update_timestamp, CURRENT_TIMESTAMP()) AS update_timestamp
    FROM source_data
    WHERE join_time <= COALESCE(leave_time, CURRENT_TIMESTAMP())  -- Validate time logic
)

-- Final select with bronze layer structure
SELECT 
    participant_id,
    meeting_id,
    user_id,
    join_time,
    leave_time,
    source_system,
    load_timestamp,
    update_timestamp
FROM data_quality_checks
