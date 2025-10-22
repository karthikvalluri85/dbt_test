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

webinar_attendance AS (
    SELECT 
        webinar_id,
        COUNT(DISTINCT participant_id) AS actual_attendees,
        AVG(DATEDIFF('minute', join_time, leave_time)) AS avg_watch_time,
        MAX(1) AS peak_concurrent_viewers -- Simplified - would need time-series data for actual calculation
    FROM {{ source('zoom_silver', 'si_participants') }}
    WHERE participant_id IS NOT NULL
      AND webinar_id IS NOT NULL
    GROUP BY webinar_id
),

webinar_facts AS (
    SELECT 
        w.webinar_id,
        TRIM(UPPER(COALESCE(w.webinar_topic, 'UNKNOWN WEBINAR'))) AS webinar_topic,
        w.start_time,
        w.end_time,
        COALESCE(w.registrants, 0) AS registrants,
        COALESCE(a.actual_attendees, 0) AS actual_attendees,
        CASE 
            WHEN COALESCE(w.registrants, 0) > 0 THEN 
                ROUND((COALESCE(a.actual_attendees, 0) * 100.0 / w.registrants), 2)
            ELSE 0 
        END AS attendance_rate,
        ROUND(COALESCE(a.avg_watch_time, 0), 2) AS average_watch_time,
        COALESCE(a.peak_concurrent_viewers, 0) AS peak_concurrent_viewers,
        0 AS q_and_a_questions, -- Placeholder - would need additional data
        0 AS poll_responses, -- Placeholder - would need additional data
        COALESCE(w.load_date, CURRENT_DATE()) AS load_date,
        COALESCE(w.update_date, CURRENT_DATE()) AS update_date,
        COALESCE(w.source_system, 'ZOOM_SILVER') AS source_system
    FROM webinar_base w
    LEFT JOIN webinar_attendance a ON w.webinar_id = a.webinar_id
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
    attendance_rate,
    average_watch_time,
    peak_concurrent_viewers,
    q_and_a_questions,
    poll_responses,
    load_date,
    update_date,
    source_system
FROM webinar_facts
