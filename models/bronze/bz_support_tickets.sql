{{ config(
    materialized='table',
    pre_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (SOURCE_TABLE, LOAD_TIMESTAMP, PROCESSED_BY, PROCESSING_TIME, STATUS) VALUES ('BZ_TICKETS', CURRENT_TIMESTAMP(), 'DBT', 0, 'START'){% endif %}",
    post_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (SOURCE_TABLE, LOAD_TIMESTAMP, PROCESSED_BY, PROCESSING_TIME, STATUS) VALUES ('BZ_TICKETS', CURRENT_TIMESTAMP(), 'DBT', 1, 'DONE'){% endif %}"
) }}

-- Bronze Layer Support Tickets Table
-- Raw to Bronze 1:1 mapping for Zoom Support Tickets data
-- Author: DBT Data Engineering Team
-- Created: {{ run_started_at }}

WITH source_data AS (
    SELECT 
        -- Direct 1:1 mapping from raw to bronze
        TICKET_ID,
        USER_ID,
        TICKET_TYPE,
        RESOLUTION_STATUS,
        OPEN_DATE,
        LOAD_TIMESTAMP,
        UPDATE_TIMESTAMP,
        SOURCE_SYSTEM
    FROM {{ source('zoom_raw', 'support_tickets') }}
),

data_quality_checks AS (
    SELECT 
        *,
        -- Add data quality indicators
        CASE 
            WHEN TICKET_ID IS NULL THEN 'MISSING_ID'
            WHEN USER_ID IS NULL THEN 'MISSING_USER_ID'
            ELSE 'VALID'
        END AS data_quality_status
    FROM source_data
)

SELECT 
    TICKET_ID,
    USER_ID,
    TICKET_TYPE,
    RESOLUTION_STATUS,
    OPEN_DATE,
    LOAD_TIMESTAMP,
    UPDATE_TIMESTAMP,
    SOURCE_SYSTEM
FROM data_quality_checks
-- Include all records, even those with quality issues for bronze layer
