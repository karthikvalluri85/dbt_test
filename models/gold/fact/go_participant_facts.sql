{{ config(
    materialized='table'
) }}

-- Participant Facts with engagement metrics
WITH participant_base AS (
    SELECT 
        participant_id,
        join_time,
        leave_time,
        load_date,
        update_date,
        source_system
    FROM {{ source('zoom_silver', 'si_participants') }}
    WHERE participant_id IS NOT NULL
      AND join_time IS NOT NULL
      AND leave_time IS NOT NULL
      AND leave_time >= join_time
),

participant_facts AS (
    SELECT 
        participant_id,
        join_time,
        leave_time,
        DATEDIFF('minute', join_time, leave_time) AS session_duration_minutes,
        -- Simplified engagement score calculation
        CASE 
            WHEN DATEDIFF('minute', join_time, leave_time) >= 1 THEN 
                ROUND(RANDOM() * 0.8 + 0.2, 2) -- Placeholder engagement score between 0.2-1.0
            ELSE 0
        END AS engagement_score,
        -- Placeholder metrics - would need additional data sources
        ROUND(DATEDIFF('minute', join_time, leave_time) * RANDOM() * 0.3, 0) AS microphone_usage_minutes,
        ROUND(DATEDIFF('minute', join_time, leave_time) * RANDOM() * 0.4, 0) AS camera_usage_minutes,
        FLOOR(RANDOM() * 3) AS screen_share_count,
        FLOOR(RANDOM() * 10) AS chat_messages_sent,
        FLOOR(RANDOM() * 5) AS reactions_count,
        load_date,
        update_date,
        source_system
    FROM participant_base
    WHERE DATEDIFF('minute', join_time, leave_time) >= 0
      AND DATEDIFF('minute', join_time, leave_time) <= 1440 -- Max 24 hours
)

SELECT 
    UUID_STRING() AS participant_fact_id,
    participant_id,
    join_time,
    leave_time,
    session_duration_minutes,
    engagement_score,
    microphone_usage_minutes,
    camera_usage_minutes,
    screen_share_count,
    chat_messages_sent,
    reactions_count,
    COALESCE(load_date, CURRENT_DATE()) AS load_date,
    COALESCE(update_date, CURRENT_DATE()) AS update_date,
    COALESCE(source_system, 'ZOOM_SILVER') AS source_system
FROM participant_facts
