{{
  config(
    materialized='table',
    pre_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, status) VALUES ('bz_billing_events', CURRENT_TIMESTAMP(), 'DBT_BRONZE_PIPELINE', 'STARTED'){% endif %}",
    post_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, status) VALUES ('bz_billing_events', CURRENT_TIMESTAMP(), 'DBT_BRONZE_PIPELINE', 'SUCCESS'){% endif %}"
  )
}}

/*
=============================================================================
BRONZE LAYER BILLING EVENTS MODEL
=============================================================================
Purpose: Transform raw billing events data to bronze layer with data quality checks
Source: ZOOM_RAW_SCHEMA.BILLING_EVENTS
Target: ZOOM_BRONZE_SCHEMA.bz_billing_events
Transformation: 1:1 mapping with audit columns
=============================================================================
*/

WITH source_data AS (
    -- Extract raw billing events data with basic validation
    SELECT 
        event_id,
        event_type,
        event_date,
        amount,
        user_id,
        source_system,
        load_timestamp,
        update_timestamp
    FROM {{ source('zoom_raw', 'billing_events') }}
    WHERE event_id IS NOT NULL  -- Basic data quality check
),

data_quality_checks AS (
    -- Apply data quality transformations
    SELECT 
        event_id,
        TRIM(UPPER(event_type)) AS event_type,
        event_date,
        COALESCE(amount, 0) AS amount,  -- Handle null amounts
        user_id,
        COALESCE(source_system, 'ZOOM_RAW_SCHEMA') AS source_system,
        COALESCE(load_timestamp, CURRENT_TIMESTAMP()) AS load_timestamp,
        COALESCE(update_timestamp, CURRENT_TIMESTAMP()) AS update_timestamp
    FROM source_data
)

-- Final select with bronze layer structure
SELECT 
    event_id,
    event_type,
    event_date,
    amount,
    user_id,
    source_system,
    load_timestamp,
    update_timestamp
FROM data_quality_checks
