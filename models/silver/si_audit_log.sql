{{ config(
    materialized='table',
    tags=['audit', 'silver']
) }}

-- Audit log table for tracking all silver layer processing
SELECT 
    'INIT' AS audit_id,
    'SILVER_PROCESSING' AS process_name,
    CURRENT_TIMESTAMP() AS process_start_time,
    CURRENT_TIMESTAMP() AS process_end_time,
    'SUCCESS' AS process_status,
    'SYSTEM' AS source_table,
    0 AS records_processed,
    0 AS records_inserted,
    0 AS records_updated,
    0 AS records_rejected,
    0 AS error_count,
    CURRENT_TIMESTAMP() AS load_timestamp,
    CURRENT_TIMESTAMP() AS update_timestamp,
    'DBT_SILVER_PIPELINE' AS source_system
WHERE FALSE -- This ensures no records are inserted during initial creation
