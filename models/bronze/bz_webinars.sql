{{ config(
    materialized='table',
    pre_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (SOURCE_TABLE, LOAD_TIMESTAMP, PROCESSED_BY, PROCESSING_TIME, STATUS) VALUES ('BZ_WEBINARS', CURRENT_TIMESTAMP(), 'DBT_PROCESS', 0, 'STARTED'){% endif %}",
    post_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (SOURCE_TABLE, LOAD_TIMESTAMP, PROCESSED_BY, PROCESSING_TIME, STATUS) VALUES ('BZ_WEBINARS', CURRENT_TIMESTAMP(), 'DBT_PROCESS', DATEDIFF('second', (SELECT MAX(LOAD_TIMESTAMP) FROM {{ ref('bz_audit_log') }} WHERE SOURCE_TABLE = 'BZ_WEBINARS' AND STATUS = 'STARTED'), CURRENT_TIMESTAMP()), 'COMPLETED'){% endif %}"
) }}

-- Bronze Layer Webinars Table
-- Raw to Bronze 1:1 mapping for Zoom Webinars data
-- Author: DBT Data Engineering Team
-- Created: {{ run_started_at }}

WITH source_data AS (
    SELECT 
        -- Direct 1:1 mapping from raw to bronze
        WEBINAR_ID,
        WEBINAR_TOPIC,
        HOST_ID,
        START_TIME,
        END_TIME,
        REGISTRANTS,
        LOAD_TIMESTAMP,
        UPDATE_TIMESTAMP,
        SOURCE_SYSTEM
    FROM {{ source('zoom_raw', 'webinars') }}
),

data_quality_checks AS (
    SELECT 
        *,
        -- Add data quality indicators
        CASE 
            WHEN WEBINAR_ID IS NULL THEN 'MISSING_ID'
            WHEN START_TIME > END_TIME THEN 'INVALID_TIME_RANGE'
            ELSE 'VALID'
        END AS data_quality_status
    FROM source_data
)

SELECT 
    WEBINAR_ID,
    WEBINAR_TOPIC,
    HOST_ID,
    START_TIME,
    END_TIME,
    REGISTRANTS,
    LOAD_TIMESTAMP,
    UPDATE_TIMESTAMP,
    SOURCE_SYSTEM
FROM data_quality_checks
-- Include all records, even those with quality issues for bronze layer
