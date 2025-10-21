{{ config(
    materialized='incremental',
    unique_key='participant_id',
    on_schema_change='sync_all_columns',
    tags=['participants', 'silver']
) }}

-- Silver layer participants table with data quality checks and transformations
WITH bronze_participants AS (
    SELECT 
        leave_time,
        join_time,
        load_timestamp,
        update_timestamp,
        source_system
    FROM {{ source('bronze', 'bz_participants') }}
    WHERE join_time IS NOT NULL
      AND (leave_time IS NULL OR leave_time >= join_time)
),

deduped_participants AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY join_time, COALESCE(leave_time, '9999-12-31')
            ORDER BY COALESCE(update_timestamp, load_timestamp) DESC
        ) AS row_num
    FROM bronze_participants
),

transformed_participants AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['join_time', 'coalesce(leave_time, "9999-12-31")']) }} AS participant_id,
        leave_time,
        join_time,
        CURRENT_DATE() AS load_date,
        CURRENT_DATE() AS update_date,
        COALESCE(source_system, 'ZOOM_BRONZE_SCHEMA') AS source_system
    FROM deduped_participants
    WHERE row_num = 1
)

SELECT 
    participant_id,
    leave_time,
    join_time,
    load_date,
    update_date,
    source_system
FROM transformed_participants

{% if is_incremental() %}
    WHERE update_date > (SELECT COALESCE(MAX(update_date), '1900-01-01') FROM {{ this }})
{% endif %}
