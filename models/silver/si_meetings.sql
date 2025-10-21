{{
  config(
    materialized='incremental',
    unique_key='meeting_id',
    on_schema_change='sync_all_columns'
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
    {{ dbt_utils.generate_surrogate_key(['meeting_topic', 'start_time']) }} AS meeting_id
  FROM {{ source('zoom_bronze', 'bz_meetings') }}
  WHERE start_time IS NOT NULL -- Data quality check
),

-- Data quality checks and transformations
cleansed_meetings AS (
  SELECT 
    meeting_id,
    TRIM(COALESCE(meeting_topic, 'Unknown Meeting')) AS meeting_topic,
    CASE 
      WHEN duration_minutes IS NULL AND end_time IS NOT NULL THEN DATEDIFF('MINUTE', start_time, end_time)
      WHEN duration_minutes IS NULL THEN 0
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
    (end_time IS NULL OR end_time >= start_time) -- End time must be after start time if provided
    AND LENGTH(TRIM(COALESCE(meeting_topic, ''))) > 0 -- Non-empty meeting topic
),

-- Deduplication logic - keep latest record per meeting
deduped_meetings AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY meeting_id 
      ORDER BY COALESCE(update_timestamp, load_timestamp) DESC, load_timestamp DESC
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
    COALESCE(update_timestamp, load_timestamp) > (SELECT COALESCE(MAX(update_date), '1900-01-01') FROM {{ this }})
    OR load_timestamp > (SELECT COALESCE(MAX(load_date), '1900-01-01') FROM {{ this }})
  )
{% endif %}
