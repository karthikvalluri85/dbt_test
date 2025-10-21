{{ config(
    materialized='incremental',
    unique_key='webinar_id',
    on_schema_change='sync_all_columns',
    tags=['webinars', 'silver']
) }}

-- Silver layer webinars table with data quality checks and transformations
WITH bronze_webinars AS (
    SELECT 
        end_time,
        webinar_topic,
        start_time,
        registrants,
        load_timestamp,
        update_timestamp,
        source_system
    FROM {{ source('bronze', 'bz_webinars') }}
    WHERE webinar_topic IS NOT NULL
      AND start_time IS NOT NULL
      AND end_time IS NOT NULL
      AND end_time > start_time
      AND COALESCE(registrants, 0) >= 0
),

deduped_webinars AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY webinar_topic, start_time, end_time
            ORDER BY COALESCE(update_timestamp, load_timestamp) DESC
        ) AS row_num
    FROM bronze_webinars
),

transformed_webinars AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['webinar_topic', 'start_time', 'end_time']) }} AS webinar_id,
        end_time,
        TRIM(webinar_topic) AS webinar_topic,
        start_time,
        COALESCE(registrants, 0) AS registrants,
        CURRENT_DATE() AS load_date,
        CURRENT_DATE() AS update_date,
        COALESCE(source_system, 'ZOOM_BRONZE_SCHEMA') AS source_system
    FROM deduped_webinars
    WHERE row_num = 1
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
FROM transformed_webinars

{% if is_incremental() %}
    WHERE update_date > (SELECT COALESCE(MAX(update_date), '1900-01-01') FROM {{ this }})
{% endif %}
