-- Bronze Audit Log Model
-- This model creates the audit log table for tracking data processing

{{ config(
    materialized='table',
    pre_hook="",
    post_hook=""
) }}

SELECT 
    1 as record_id,
    CAST('INITIALIZATION' AS VARCHAR(255)) as source_table,
    CURRENT_TIMESTAMP() as load_timestamp,
    CAST('DBT_SYSTEM' AS VARCHAR(255)) as processed_by,
    0 as processing_time,
    CAST('INITIALIZED' AS VARCHAR(50)) as status
WHERE FALSE -- This ensures no actual data is inserted during model creation
