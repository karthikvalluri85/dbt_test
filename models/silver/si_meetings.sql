{{
  config(
    materialized='incremental',
    unique_key='meeting_id',
    on_schema_change='sync_all_columns',
    pre_hook="INSERT INTO {{ this.schema }}.si_audit_log (audit_id, pipeline_name, execution_start_time, execution_status, source_table, target_table, executed_by, load_date) SELECT '{{ invocation_id }}' || '_si_meetings_START', 'SI_MEETINGS_TRANSFORM', CURRENT_TIMESTAMP(), 'STARTED', 'bz_meetings', 'si_meetings', 'DBT_SYSTEM', CURRENT_DATE()",
    post_hook="INSERT INTO {{ this.schema }}.si_audit_log (audit_id, pipeline_name, execution_end_time, execution_status, source_table, target_table, executed_by, load_date) SELECT '{{ invocation_id }}' || '_si_meetings_END', 'SI_MEETINGS_TRANSFORM', CURRENT_TIMESTAMP(), 'COMPLETED', 'bz_meetings', 'si_meetings', 'DBT_SYSTEM', CURRENT_DATE()"
  )
}}

-- Silver layer meetings table with data quality checks and transformations
WITH bronze_meetings AS (
  SELECT 
    meeting_topic,
    duration_minutes,
    end_time,
    start_time,
    load_timestamp,
    update_timestamp,
    source_system,
    -- Generate unique meeting_id from meeting details
    {{ dbt_utils.generate_surrogate_key(['meeting_topic', 'start_time', 'end_time']) }} AS meeting_id
  FROM {{ source('zoom_bronze', 'bz_meetings') }}
  WHERE start_time IS NOT NULL AND end_time IS NOT NULL -- Data quality check
),

-- Data quality checks and transformations
cleansed_meetings AS (
  SELECT 
    meeting_id,
    TRIM(meeting_topic) AS meeting_topic,
    CASE 
      WHEN duration_minutes IS NULL THEN DATEDIFF('MINUTE', start_time, end_time)
      ELSE duration_minutes
    END AS duration_minutes,
    end_time,
    start_time,
    load_timestamp,
    update_timestamp,
    source_system,
    CURRENT_DATE() AS load_date,
    CURRENT_DATE() AS update_date
  FROM bronze_meetings
  WHERE 
    -- Data quality validations
    end_time > start_time -- End time must be after start time
    AND DATEDIFF('MINUTE', start_time, end_time) BETWEEN 0 AND 2880 -- Duration between 0 and 48 hours
    AND LENGTH(TRIM(meeting_topic)) > 0 -- Non-empty meeting topic
),

-- Deduplication logic - keep latest record per meeting
deduped_meetings AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY meeting_id 
      ORDER BY update_timestamp DESC, load_timestamp DESC
    ) AS row_num
  FROM cleansed_meetings
)

SELECT 
  meeting_id,
  meeting_topic,
  duration_minutes,
  end_time,
  start_time,
  load_date,
  update_date,
  source_system
FROM deduped_meetings
WHERE row_num = 1

{% if is_incremental() %}
  AND (
    update_timestamp > (SELECT MAX(update_date) FROM {{ this }})
    OR load_timestamp > (SELECT MAX(load_date) FROM {{ this }})
  )
{% endif %}
