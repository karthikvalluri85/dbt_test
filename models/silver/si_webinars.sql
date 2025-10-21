{{
  config(
    materialized='incremental',
    unique_key='webinar_id',
    on_schema_change='sync_all_columns'
  )
}}

-- Silver layer webinars table with data quality checks and transformations
WITH bronze_webinars AS (
  SELECT 
    end_time,
    webinar_topic,
    start_time,
    registrants,
    load_timestamp,
    update_timestamp,
    source_system,
    -- Generate unique webinar_id from webinar details
    {{ dbt_utils.generate_surrogate_key(['webinar_topic', 'start_time']) }} AS webinar_id
  FROM {{ source('zoom_bronze', 'bz_webinars') }}
  WHERE start_time IS NOT NULL AND webinar_topic IS NOT NULL -- Data quality check
),

-- Data quality checks and transformations
cleansed_webinars AS (
  SELECT 
    webinar_id,
    end_time,
    TRIM(webinar_topic) AS webinar_topic,
    start_time,
    COALESCE(registrants, 0) AS registrants,
    load_timestamp,
    update_timestamp,
    source_system,
    CURRENT_DATE() AS load_date,
    CURRENT_DATE() AS update_date
  FROM bronze_webinars
  WHERE 
    -- Data quality validations
    (end_time IS NULL OR end_time > start_time) -- End time must be after start time if provided
    AND COALESCE(registrants, 0) >= 0 -- Registrants must be non-negative
    AND LENGTH(TRIM(webinar_topic)) > 0 -- Non-empty webinar topic
),

-- Deduplication logic - keep latest record per webinar
deduped_webinars AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY webinar_id 
      ORDER BY COALESCE(update_timestamp, load_timestamp) DESC, load_timestamp DESC
    ) AS row_num
  FROM cleansed_webinars
)

SELECT 
  webinar_id,
  end_time,
  webinar_topic,
  start_time,
  registrants,
  load_date,
  update_date,
  source_system
FROM deduped_webinars
WHERE row_num = 1

{% if is_incremental() %}
  AND (
    COALESCE(update_timestamp, load_timestamp) > (SELECT COALESCE(MAX(update_date), '1900-01-01') FROM {{ this }})
    OR load_timestamp > (SELECT COALESCE(MAX(load_date), '1900-01-01') FROM {{ this }})
  )
{% endif %}
