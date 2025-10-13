-- Bronze Feature Usage Model
-- Transforms raw feature usage data into bronze layer with data quality checks and audit logging
-- Source: ZOOM_RAW_SCHEMA.feature_usage
-- Target: bz_feature_usage

{{ config(
    materialized='table',
    pre_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status) VALUES ('bz_feature_usage', CURRENT_TIMESTAMP(), 'dbt_transformation', 0, 'STARTED'){% endif %}",
    post_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status) VALUES ('bz_feature_usage', CURRENT_TIMESTAMP(), 'dbt_transformation', DATEDIFF('second', (SELECT MAX(load_timestamp) FROM {{ ref('bz_audit_log') }} WHERE source_table = 'bz_feature_usage' AND status = 'STARTED'), CURRENT_TIMESTAMP()), 'COMPLETED'){% endif %}"
) }}

-- CTE for raw data extraction and basic validation
with raw_feature_usage as (
    select
        usage_id,
        meeting_id,
        feature_name,
        usage_count,
        usage_date,
        load_timestamp as raw_load_timestamp,
        update_timestamp as raw_update_timestamp,
        source_system as raw_source_system
    from {{ source('zoom_raw_schema', 'feature_usage') }}
    where usage_id is not null  -- Basic data quality check
),

-- CTE for data transformation and standardization
transformed_feature_usage as (
    select
        -- Direct 1:1 mapping from raw to bronze as per mapping specification
        usage_id,
        meeting_id,
        feature_name,
        usage_count,
        usage_date,
        
        -- Metadata columns with current timestamp for bronze layer
        current_timestamp() as load_timestamp,
        current_timestamp() as update_timestamp,
        'ZOOM_PLATFORM' as source_system
        
    from raw_feature_usage
)

-- Final select with error handling
select 
    usage_id,
    meeting_id,
    feature_name,
    usage_count,
    usage_date,
    load_timestamp,
    update_timestamp,
    source_system
from transformed_feature_usage
