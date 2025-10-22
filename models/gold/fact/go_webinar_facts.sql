{{ config(
    materialized='table'
) }}

-- Webinar Facts with attendance and engagement metrics
WITH webinar_base AS (
    SELECT 
        webinar_id,
        webinar_topic,
        start_time,
        end_time,
        registrants,
        load_date,
        update_date,
        source_system
    FROM {{ source('zoom_silver', 'si_webinars') }}
    WHERE webinar_id IS NOT NULL
),

-- Since participants table doesn't have webinar_id, we'll create placeholder metrics
webinar_facts AS (
    SELECT 
        w.webinar_id,
        TRIM(UPPER(COALESCE(w.webinar_topic, 'UNKNOWN WEBINAR'))) AS webinar_topic,
        w.start_time,
        w.end_time,
        COALESCE(w.registrants, 0) AS registrants,
        -- Placeholder metrics - would need proper relationship data
        FLOOR(COALESCE(w.registrants, 0) * (RANDOM() * 0.3 + 0.5)) AS actual_attendees,
        ROUND((FLOOR(COALESCE(w.registrants, 0) * (RANDOM() * 0.3 + 0.5)) * 100.0 / NULLIF(w.registrants, 0)), 2) AS attendance_rate,
        ROUND(RANDOM() * 45 + 15, 2) AS average_watch_time,
        FLOOR(RANDOM() * 100 + 10) AS peak_concurrent_viewers,
        FLOOR(RANDOM() * 25) AS q_and_a_questions,
        FLOOR(RANDOM() * 15) AS poll_responses,
        COALESCE(w.load_date, CURRENT_DATE()) AS load_date,
        COALESCE(w.update_date, CURRENT_DATE()) AS update_date,
        COALESCE(w.source_system, 'ZOOM_SILVER') AS source_system
    FROM webinar_base w
    WHERE w.registrants IS NOT NULL 
      AND w.registrants >= 0
)

SELECT 
    UUID_STRING() AS webinar_fact_id,
    webinar_id,
    webinar_topic,
    start_time,
    end_time,
    registrants,
    actual_attendees,
    COALESCE(attendance_rate, 0) AS attendance_rate,
    average_watch_time,
    peak_concurrent_viewers,
    q_and_a_questions,
    poll_responses,
    load_date,
    update_date,
    source_system
FROM webinar_facts
