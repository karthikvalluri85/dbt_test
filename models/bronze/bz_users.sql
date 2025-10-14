{{ config(
    materialized='table',
    pre_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (SOURCE_TABLE, LOAD_TIMESTAMP, PROCESSED_BY, PROCESSING_TIME, STATUS) VALUES ('BZ_USERS', CURRENT_TIMESTAMP(), 'DBT', 0, 'START'){% endif %}",
    post_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (SOURCE_TABLE, LOAD_TIMESTAMP, PROCESSED_BY, PROCESSING_TIME, STATUS) VALUES ('BZ_USERS', CURRENT_TIMESTAMP(), 'DBT', 1, 'DONE'){% endif %}"
) }}

-- Bronze Layer Users Table
-- Raw to Bronze 1:1 mapping for Zoom Users data
-- Author: DBT Data Engineering Team
-- Created: {{ run_started_at }}

WITH source_data AS (
    SELECT 
        -- Direct 1:1 mapping from raw to bronze
        USER_ID,
        USER_NAME,
        EMAIL,
        COMPANY,
        PLAN_TYPE,
        LOAD_TIMESTAMP,
        UPDATE_TIMESTAMP,
        SOURCE_SYSTEM
    FROM {{ source('zoom_raw', 'users') }}
),

data_quality_checks AS (
    SELECT 
        *,
        -- Add data quality indicators
        CASE 
            WHEN USER_ID IS NULL THEN 'MISSING_ID'
            WHEN EMAIL IS NULL OR EMAIL NOT LIKE '%@%' THEN 'INVALID_EMAIL'
            ELSE 'VALID'
        END AS data_quality_status
    FROM source_data
)

SELECT 
    USER_ID,
    USER_NAME,
    EMAIL,
    COMPANY,
    PLAN_TYPE,
    LOAD_TIMESTAMP,
    UPDATE_TIMESTAMP,
    SOURCE_SYSTEM
FROM data_quality_checks
-- Include all records, even those with quality issues for bronze layer
