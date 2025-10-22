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

participant_metrics AS (
    SELECT 
        meeting_id,
        COUNT(DISTINCT participant_id) AS participant_count,
        AVG(DATEDIFF('minute', join_time, leave_time)) AS avg_session_duration,
        SUM(CASE WHEN join_time IS NOT NULL AND leave_time IS NOT NULL THEN 1 ELSE 0 END) AS valid_sessions
    FROM {{ source('zoom_silver', 'si_participants') }}
    WHERE participant_id IS NOT NULL
    GROUP BY meeting_id
),

meeting_facts AS (
    SELECT 
        m.meeting_id,
        TRIM(UPPER(COALESCE(m.meeting_topic, 'UNKNOWN MEETING'))) AS meeting_topic,
        COALESCE(m.duration_minutes, 0) AS duration_minutes,
        m.start_time,
        m.end_time,
        COALESCE(p.participant_count, 0) AS participant_count,
        ROUND(COALESCE(p.avg_session_duration, 0), 2) AS average_engagement_score,
        0 AS total_screen_share_duration, -- Placeholder - would need additional data
        CASE 
            WHEN m.duration_minutes > 0 THEN m.duration_minutes 
            ELSE 0 
        END AS recording_duration,
        0 AS chat_message_count, -- Placeholder - would need additional data
        COALESCE(m.load_date, CURRENT_DATE()) AS load_date,
        COALESCE(m.update_date, CURRENT_DATE()) AS update_date,
        COALESCE(m.source_system, 'ZOOM_SILVER') AS source_system
    FROM meeting_base m
    LEFT JOIN participant_metrics p ON m.meeting_id = p.meeting_id
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
