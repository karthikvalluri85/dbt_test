{{ config(
    materialized='incremental',
    unique_key='meeting_id',
    on_schema_change='sync_all_columns',
    tags=['meetings', 'silver']
) }}

-- Silver layer meetings table with data quality checks and transformations
WITH bronze_meetings AS (
    SELECT 
        meeting_topic,
        duration_minutes,
        end_time,
        start_time,
        load_timestamp,
        update_timestamp,
        source_system
    FROM {{ source('bronze', 'bz_meetings') }}
    WHERE meeting_topic IS NOT NULL
      AND start_time IS NOT NULL
      AND end_time IS NOT NULL
      AND end_time > start_time
      AND COALESCE(duration_minutes, 0) >= 0
      AND COALESCE(duration_minutes, 0) <= 2880 -- Max 48 hours
),

deduped_meetings AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY meeting_topic, start_time, end_time 
            ORDER BY COALESCE(update_timestamp, load_timestamp) DESC
        ) AS row_num
    FROM bronze_meetings
),

transformed_meetings AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['meeting_topic', 'start_time', 'end_time']) }} AS meeting_id,
        TRIM(meeting_topic) AS meeting_topic,
        CASE 
            WHEN duration_minutes IS NULL THEN 
                DATEDIFF('MINUTE', start_time, end_time)
            ELSE duration_minutes 
        END AS duration_minutes,
        end_time,
        start_time,
        CURRENT_DATE() AS load_date,
        CURRENT_DATE() AS update_date,
        COALESCE(source_system, 'ZOOM_BRONZE_SCHEMA') AS source_system
    FROM deduped_meetings
    WHERE row_num = 1
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
FROM transformed_meetings

{% if is_incremental() %}
    WHERE update_date > (SELECT COALESCE(MAX(update_date), '1900-01-01') FROM {{ this }})
{% endif %}
