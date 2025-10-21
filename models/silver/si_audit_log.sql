{{
  config(
    materialized='incremental',
    unique_key='audit_id',
    on_schema_change='sync_all_columns',
    pre_hook="INSERT INTO {{ this.schema }}.si_audit_log (audit_id, pipeline_name, execution_start_time, execution_status, source_table, target_table, executed_by, load_date) SELECT '{{ invocation_id }}' || '_START', 'SILVER_PIPELINE', CURRENT_TIMESTAMP(), 'STARTED', 'BRONZE_LAYER', 'SILVER_LAYER', 'DBT_SYSTEM', CURRENT_DATE() WHERE '{{ this.name }}' != 'si_audit_log'",
    post_hook="INSERT INTO {{ this.schema }}.si_audit_log (audit_id, pipeline_name, execution_end_time, execution_status, source_table, target_table, executed_by, load_date) SELECT '{{ invocation_id }}' || '_END', 'SILVER_PIPELINE', CURRENT_TIMESTAMP(), 'COMPLETED', 'BRONZE_LAYER', 'SILVER_LAYER', 'DBT_SYSTEM', CURRENT_DATE() WHERE '{{ this.name }}' != 'si_audit_log'"
  )
}}

-- Audit log table for tracking all silver layer transformations
WITH audit_base AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['invocation_id', 'current_timestamp()']) }} AS audit_id,
    'INITIAL_LOAD' AS pipeline_name,
    CURRENT_TIMESTAMP() AS execution_start_time,
    CURRENT_TIMESTAMP() AS execution_end_time,
    0 AS execution_duration_seconds,
    'SUCCESS' AS execution_status,
    0 AS records_processed,
    0 AS records_inserted,
    0 AS records_updated,
    0 AS records_rejected,
    'ZOOM_BRONZE_SCHEMA' AS source_system,
    'SI_AUDIT_LOG' AS target_table,
    0 AS error_count,
    0 AS warning_count,
    0.0 AS data_volume_mb,
    '1.0.0' AS pipeline_version,
    'DBT_SYSTEM' AS executed_by,
    'INITIAL' AS configuration_hash,
    CURRENT_DATE() AS load_date,
    CURRENT_DATE() AS update_date
  FROM (SELECT 1 AS dummy) -- Dummy table for initial load
)

SELECT * FROM audit_base

{% if is_incremental() %}
  WHERE execution_start_time > (SELECT MAX(execution_start_time) FROM {{ this }})
{% endif %}
