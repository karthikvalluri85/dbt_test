{{ config(
    materialized='table',
    pre_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (SOURCE_TABLE, LOAD_TIMESTAMP, PROCESSED_BY, PROCESSING_TIME, STATUS) VALUES ('BZ_BILLING', CURRENT_TIMESTAMP(), 'DBT', 0, 'START'){% endif %}",
    post_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (SOURCE_TABLE, LOAD_TIMESTAMP, PROCESSED_BY, PROCESSING_TIME, STATUS) VALUES ('BZ_BILLING', CURRENT_TIMESTAMP(), 'DBT', 1, 'DONE'){% endif %}"
) }}

-- Bronze Layer Billing Events Table
-- Raw to Bronze 1:1 mapping for Zoom Billing Events data
-- Author: DBT Data Engineering Team
-- Created: {{ run_started_at }}

WITH source_data AS (
    SELECT 
        -- Direct 1:1 mapping from raw to bronze
        EVENT_ID,
        USER_ID,
        EVENT_TYPE,
        EVENT_DATE,
        AMOUNT,
        LOAD_TIMESTAMP,
        UPDATE_TIMESTAMP,
        SOURCE_SYSTEM
    FROM {{ source('zoom_raw', 'billing_events') }}
),

data_quality_checks AS (
    SELECT 
        *,
        -- Add data quality indicators
        CASE 
            WHEN EVENT_ID IS NULL THEN 'MISSING_ID'
            WHEN AMOUNT < 0 THEN 'NEGATIVE_AMOUNT'
            ELSE 'VALID'
        END AS data_quality_status
    FROM source_data
)

SELECT 
    EVENT_ID,
    USER_ID,
    EVENT_TYPE,
    EVENT_DATE,
    AMOUNT,
    LOAD_TIMESTAMP,
    UPDATE_TIMESTAMP,
    SOURCE_SYSTEM
FROM data_quality_checks
-- Include all records, even those with quality issues for bronze layer
