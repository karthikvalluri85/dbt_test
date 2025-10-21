{{ config(
    materialized='incremental',
    unique_key='audit_id',
    on_schema_change='sync_all_columns',
    tags=['audit', 'silver']
) }}

-- Audit log table for tracking all silver layer processing
WITH audit_records AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['source_table', 'load_timestamp']) }} AS audit_id,
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
)

SELECT 
    audit_id,
    process_name,
    process_start_time,
    process_end_time,
    process_status,
    source_table,
    records_processed,
    records_inserted,
    records_updated,
    records_rejected,
    error_count,
    load_timestamp,
    update_timestamp,
    source_system
FROM audit_records

{% if is_incremental() %}
    WHERE load_timestamp > (SELECT COALESCE(MAX(load_timestamp), '1900-01-01') FROM {{ this }})
{% endif %}
