-- Bronze Billing Events Model
-- Transforms raw billing events data to bronze layer with audit logging

{{ config(
    materialized='table',
    pre_hook="INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status) SELECT CAST('bz_billing_events' AS VARCHAR(255)), CURRENT_TIMESTAMP(), CAST('DBT_SYSTEM' AS VARCHAR(255)), 0, CAST('STARTED' AS VARCHAR(50)) WHERE '{{ this.name }}' != 'bz_audit_log'",
    post_hook="INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status) SELECT CAST('bz_billing_events' AS VARCHAR(255)), CURRENT_TIMESTAMP(), CAST('DBT_SYSTEM' AS VARCHAR(255)), DATEDIFF('second', (SELECT MAX(load_timestamp) FROM {{ ref('bz_audit_log') }} WHERE source_table = 'bz_billing_events' AND status = 'STARTED'), CURRENT_TIMESTAMP()), CAST('COMPLETED' AS VARCHAR(50)) WHERE '{{ this.name }}' != 'bz_audit_log'"
) }}

-- CTE for data transformation and validation
WITH source_data AS (
    SELECT 
        event_id,
        user_id,
        event_type,
        amount,
        event_date,
        load_timestamp as source_load_timestamp,
        update_timestamp as source_update_timestamp,
        source_system as source_system_original
    FROM {{ source('zoom_raw_schema', 'billing_events') }}
    WHERE event_id IS NOT NULL -- Basic data quality check
),

-- Add audit and metadata columns
transformed_data AS (
    SELECT 
        event_id,
        user_id,
        event_type,
        amount,
        event_date,
        CURRENT_TIMESTAMP() as load_timestamp,
        CURRENT_TIMESTAMP() as update_timestamp,
        'ZOOM_PLATFORM' as source_system
    FROM source_data
)

SELECT * FROM transformed_data
