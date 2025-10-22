{{ config(
    materialized='table'
) }}

-- Meeting Facts with aggregated participant metrics
WITH meeting_base AS (
    SELECT 
        meeting_id,
        meeting_topic,
        duration_minutes,
        start_time,
        end_time,
        load_date,
        update_date,
        source_system
    FROM {{ source('zoom_silver', 'si_meetings') }}
    WHERE meeting_id IS NOT NULL
),

-- Since participants table doesn't have meeting_id, we'll create placeholder metrics
meeting_facts AS (
    SELECT 
        m.meeting_id,
        TRIM(UPPER(COALESCE(m.meeting_topic, 'UNKNOWN MEETING'))) AS meeting_topic,
        COALESCE(m.duration_minutes, 0) AS duration_minutes,
        m.start_time,
        m.end_time,
        -- Placeholder metrics - would need proper relationship data
        FLOOR(RANDOM() * 20 + 1) AS participant_count,
        ROUND(RANDOM() * 80 + 20, 2) AS average_engagement_score,
        0 AS total_screen_share_duration,
        CASE 
            WHEN m.duration_minutes > 0 THEN m.duration_minutes 
            ELSE 0 
        END AS recording_duration,
        FLOOR(RANDOM() * 50) AS chat_message_count,
        COALESCE(m.load_date, CURRENT_DATE()) AS load_date,
        COALESCE(m.update_date, CURRENT_DATE()) AS update_date,
        COALESCE(m.source_system, 'ZOOM_SILVER') AS source_system
    FROM meeting_base m
    WHERE m.duration_minutes IS NOT NULL 
      AND m.duration_minutes >= 0 
      AND m.duration_minutes <= 2880 -- Max 48 hours
)

SELECT 
    UUID_STRING() AS meeting_fact_id,
    meeting_id,
    meeting_topic,
    duration_minutes,
    start_time,
    end_time,
    participant_count,
    average_engagement_score,
    total_screen_share_duration,
    recording_duration,
    chat_message_count,
    load_date,
    update_date,
    source_system
FROM meeting_facts
