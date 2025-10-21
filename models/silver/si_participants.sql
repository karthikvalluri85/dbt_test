{{
  config(
    materialized='incremental',
    unique_key='participant_id',
    on_schema_change='sync_all_columns'
  )
}}

-- Silver layer participants table with data quality checks and transformations
WITH bronze_participants AS (
  SELECT 
    leave_time,
    join_time,
    load_timestamp,
    update_timestamp,
    source_system,
    -- Generate unique participant_id from participant details
    {{ dbt_utils.generate_surrogate_key(['join_time', 'coalesce(leave_time, join_time)']) }} AS participant_id
  FROM {{ source('zoom_bronze', 'bz_participants') }}
  WHERE join_time IS NOT NULL -- Data quality check
),

-- Data quality checks and transformations
cleansed_participants AS (
  SELECT 
    participant_id,
    leave_time,
    join_time,
    load_timestamp,
    update_timestamp,
    source_system,
    CURRENT_DATE() AS load_date,
    CURRENT_DATE() AS update_date
  FROM bronze_participants
  WHERE 
    -- Data quality validations
    (leave_time IS NULL OR leave_time >= join_time) -- Leave time must be after join time if provided
    AND join_time <= CURRENT_TIMESTAMP() -- Join time cannot be in the future
),

-- Deduplication logic - keep latest record per participant
deduped_participants AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY participant_id 
      ORDER BY COALESCE(update_timestamp, load_timestamp) DESC, load_timestamp DESC
    ) AS row_num
  FROM cleansed_participants
)

SELECT 
  participant_id,
  leave_time,
  join_time,
  load_date,
  update_date,
  source_system
FROM deduped_participants
WHERE row_num = 1

{% if is_incremental() %}
  AND (
    COALESCE(update_timestamp, load_timestamp) > (SELECT COALESCE(MAX(update_date), '1900-01-01') FROM {{ this }})
    OR load_timestamp > (SELECT COALESCE(MAX(load_date), '1900-01-01') FROM {{ this }})
  )
{% endif %}
