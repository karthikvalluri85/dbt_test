-- Bronze Participants Model
-- Transforms raw participants data into bronze layer with data quality checks and audit logging
-- Source: ZOOM_RAW_SCHEMA.participants
-- Target: bz_participants

{{ config(
    materialized='table',
    pre_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status) VALUES ('bz_participants', CURRENT_TIMESTAMP(), 'dbt_transformation', 0, 'STARTED'){% endif %}",
    post_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status) VALUES ('bz_participants', CURRENT_TIMESTAMP(), 'dbt_transformation', 1, 'COMPLETED'){% endif %}"
) }}

-- CTE for raw data extraction and basic validation
with raw_participants as (
    select
        participant_id,
        meeting_id,
        user_id,
        join_time,
        leave_time,
        load_timestamp as raw_load_timestamp,
        update_timestamp as raw_update_timestamp,
        source_system as raw_source_system
    from {{ source('zoom_raw_schema', 'participants') }}
    where participant_id is not null  -- Basic data quality check
),

-- CTE for data transformation and standardization
transformed_participants as (
    select
        -- Direct 1:1 mapping from raw to bronze as per mapping specification
        participant_id,
        meeting_id,
        user_id,
        join_time,
        leave_time,
        
        -- Metadata columns with current timestamp for bronze layer
        current_timestamp() as load_timestamp,
        current_timestamp() as update_timestamp,
        'ZOOM_PLATFORM' as source_system
        
    from raw_participants
)

-- Final select with error handling
select 
    participant_id,
    meeting_id,
    user_id,
    join_time,
    leave_time,
    load_timestamp,
    update_timestamp,
    source_system
from transformed_participants
