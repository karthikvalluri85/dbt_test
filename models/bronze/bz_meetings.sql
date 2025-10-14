-- Bronze Meetings Model
-- Transforms raw meetings data to bronze layer with audit logging

{{ config(
    materialized='table',
    pre_hook="INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status) SELECT 'bz_meetings', CURRENT_TIMESTAMP(), 'DBT_SYSTEM', 0, 'STARTED' WHERE '{{ this.name }}' != 'bz_audit_log'",
    post_hook="INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status) SELECT 'bz_meetings', CURRENT_TIMESTAMP(), 'DBT_SYSTEM', DATEDIFF('second', (SELECT MAX(load_timestamp) FROM {{ ref('bz_audit_log') }} WHERE source_table = 'bz_meetings' AND status = 'STARTED'), CURRENT_TIMESTAMP()), 'COMPLETED' WHERE '{{ this.name }}' != 'bz_audit_log'"
) }}

-- CTE for data transformation and validation
WITH source_data AS (
    SELECT 
        meeting_id,
        host_id,
        meeting_topic,
        start_time,
        end_time,
        duration_minutes,
        load_timestamp as source_load_timestamp,
        update_timestamp as source_update_timestamp,
        source_system as source_system_original
    FROM {{ source('zoom_raw', 'meetings') }}
    WHERE meeting_id IS NOT NULL -- Basic data quality check
),

-- Add audit and metadata columns
transformed_data AS (
    SELECT 
        meeting_id,
        host_id,
        meeting_topic,
        start_time,
        end_time,
        duration_minutes,
        CURRENT_TIMESTAMP() as load_timestamp,
        CURRENT_TIMESTAMP() as update_timestamp,
        'ZOOM_PLATFORM' as source_system
    FROM source_data
)

SELECT * FROM transformed_data
