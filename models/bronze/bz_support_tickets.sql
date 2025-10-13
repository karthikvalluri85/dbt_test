-- Bronze Support Tickets Model
-- Transforms raw support tickets data into bronze layer with data quality checks and audit logging
-- Source: ZOOM_RAW_SCHEMA.support_tickets
-- Target: bz_support_tickets

{{ config(
    materialized='table',
    pre_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status) VALUES ('bz_support_tickets', CURRENT_TIMESTAMP(), 'dbt_transformation', 0, 'STARTED'){% endif %}",
    post_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status) VALUES ('bz_support_tickets', CURRENT_TIMESTAMP(), 'dbt_transformation', 1, 'COMPLETED'){% endif %}"
) }}

-- CTE for raw data extraction and basic validation
with raw_support_tickets as (
    select
        ticket_id,
        user_id,
        ticket_type,
        resolution_status,
        open_date,
        load_timestamp as raw_load_timestamp,
        update_timestamp as raw_update_timestamp,
        source_system as raw_source_system
    from {{ source('zoom_raw_schema', 'support_tickets') }}
    where ticket_id is not null  -- Basic data quality check
),

-- CTE for data transformation and standardization
transformed_support_tickets as (
    select
        -- Direct 1:1 mapping from raw to bronze as per mapping specification
        ticket_id,
        user_id,
        ticket_type,
        resolution_status,
        open_date,
        
        -- Metadata columns with current timestamp for bronze layer
        current_timestamp() as load_timestamp,
        current_timestamp() as update_timestamp,
        'ZOOM_PLATFORM' as source_system
        
    from raw_support_tickets
)

-- Final select with error handling
select 
    ticket_id,
    user_id,
    ticket_type,
    resolution_status,
    open_date,
    load_timestamp,
    update_timestamp,
    source_system
from transformed_support_tickets
