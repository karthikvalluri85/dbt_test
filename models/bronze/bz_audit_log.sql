-- Bronze Audit Log Model
-- This model creates the audit log table for tracking data processing

{{ config(
    materialized='table',
    pre_hook="",
    post_hook=""
) }}

SELECT 
    1 as record_id,
    'INITIALIZATION' as source_table,
    CURRENT_TIMESTAMP() as load_timestamp,
    'DBT_SYSTEM' as processed_by,
    0 as processing_time,
    'INITIALIZED' as status
WHERE FALSE -- This ensures no actual data is inserted during model creation
