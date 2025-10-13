-- Bronze Webinars Model
-- Transforms raw webinars data into bronze layer with data quality checks and audit logging
-- Source: ZOOM_RAW_SCHEMA.webinars
-- Target: bz_webinars

{{ config(
    materialized='table',
    pre_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status) VALUES ('bz_webinars', CURRENT_TIMESTAMP(), 'dbt_transformation', 0, 'STARTED'){% endif %}",
    post_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status) VALUES ('bz_webinars', CURRENT_TIMESTAMP(), 'dbt_transformation', DATEDIFF('second', (SELECT MAX(load_timestamp) FROM {{ ref('bz_audit_log') }} WHERE source_table = 'bz_webinars' AND status = 'STARTED'), CURRENT_TIMESTAMP()), 'COMPLETED'){% endif %}"
) }}

-- CTE for raw data extraction and basic validation
with raw_webinars as (
    select
        webinar_id,
        host_id,
        webinar_topic,
        start_time,
        end_time,
        registrants,
        load_timestamp as raw_load_timestamp,
        update_timestamp as raw_update_timestamp,
        source_system as raw_source_system
    from {{ source('zoom_raw', 'webinars') }}
    where webinar_id is not null  -- Basic data quality check
),

-- CTE for data transformation and standardization
transformed_webinars as (
    select
        -- Direct 1:1 mapping from raw to bronze as per mapping specification
        webinar_id,
        host_id,
        webinar_topic,
        start_time,
        end_time,
        registrants,
        
        -- Metadata columns with current timestamp for bronze layer
        current_timestamp() as load_timestamp,
        current_timestamp() as update_timestamp,
        'ZOOM_PLATFORM' as source_system
        
    from raw_webinars
)

-- Final select with error handling
select 
    webinar_id,
    host_id,
    webinar_topic,
    start_time,
    end_time,
    registrants,
    load_timestamp,
    update_timestamp,
    source_system
from transformed_webinars
