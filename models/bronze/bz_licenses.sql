{{ config(
    materialized='table',
    pre_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (SOURCE_TABLE, LOAD_TIMESTAMP, PROCESSED_BY, PROCESSING_TIME, STATUS) VALUES ('BZ_LICENSES', CURRENT_TIMESTAMP(), 'DBT', 0, 'START'){% endif %}",
    post_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (SOURCE_TABLE, LOAD_TIMESTAMP, PROCESSED_BY, PROCESSING_TIME, STATUS) VALUES ('BZ_LICENSES', CURRENT_TIMESTAMP(), 'DBT', 1, 'DONE'){% endif %}"
) }}

-- Bronze Layer Licenses Table
-- Raw to Bronze 1:1 mapping for Zoom Licenses data
-- Author: DBT Data Engineering Team
-- Created: {{ run_started_at }}

WITH source_data AS (
    SELECT 
        -- Direct 1:1 mapping from raw to bronze
        LICENSE_ID,
        LICENSE_TYPE,
        ASSIGNED_TO_USER_ID,
        START_DATE,
        END_DATE,
        LOAD_TIMESTAMP,
        UPDATE_TIMESTAMP,
        SOURCE_SYSTEM
    FROM {{ source('zoom_raw', 'licenses') }}
),

data_quality_checks AS (
    SELECT 
        *,
        -- Add data quality indicators
        CASE 
            WHEN LICENSE_ID IS NULL THEN 'MISSING_ID'
            WHEN START_DATE > END_DATE THEN 'INVALID_DATE_RANGE'
            ELSE 'VALID'
        END AS data_quality_status
    FROM source_data
)

SELECT 
    LICENSE_ID,
    LICENSE_TYPE,
    ASSIGNED_TO_USER_ID,
    START_DATE,
    END_DATE,
    LOAD_TIMESTAMP,
    UPDATE_TIMESTAMP,
    SOURCE_SYSTEM
FROM data_quality_checks
-- Include all records, even those with quality issues for bronze layer
