{{
  config(
    materialized='table',
    pre_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, status) VALUES ('bz_support_tickets', CURRENT_TIMESTAMP(), 'DBT_BRONZE_PIPELINE', 'STARTED'){% endif %}",
    post_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, status) VALUES ('bz_support_tickets', CURRENT_TIMESTAMP(), 'DBT_BRONZE_PIPELINE', 'SUCCESS'){% endif %}"
  )
}}

/*
=============================================================================
BRONZE LAYER SUPPORT TICKETS MODEL
=============================================================================
Purpose: Transform raw support tickets data to bronze layer with data quality checks
Source: ZOOM_RAW_SCHEMA.SUPPORT_TICKETS
Target: ZOOM_BRONZE_SCHEMA.bz_support_tickets
Transformation: 1:1 mapping with audit columns
=============================================================================
*/

WITH source_data AS (
    -- Extract raw support tickets data with basic validation
    SELECT 
        ticket_id,
        ticket_type,
        resolution_status,
        open_date,
        user_id,
        source_system,
        load_timestamp,
        update_timestamp
    FROM {{ source('zoom_raw', 'support_tickets') }}
    WHERE ticket_id IS NOT NULL  -- Basic data quality check
),

data_quality_checks AS (
    -- Apply data quality transformations
    SELECT 
        ticket_id,
        TRIM(UPPER(ticket_type)) AS ticket_type,
        TRIM(UPPER(resolution_status)) AS resolution_status,
        open_date,
        user_id,
        COALESCE(source_system, 'ZOOM_RAW_SCHEMA') AS source_system,
        COALESCE(load_timestamp, CURRENT_TIMESTAMP()) AS load_timestamp,
        COALESCE(update_timestamp, CURRENT_TIMESTAMP()) AS update_timestamp
    FROM source_data
)

-- Final select with bronze layer structure
SELECT 
    ticket_id,
    ticket_type,
    resolution_status,
    open_date,
    user_id,
    source_system,
    load_timestamp,
    update_timestamp
FROM data_quality_checks
