{{ config(
    materialized='incremental',
    unique_key='billing_event_id',
    on_schema_change='sync_all_columns',
    tags=['billing_events', 'silver']
) }}

-- Silver layer billing events table with data quality checks and transformations
WITH bronze_billing_events AS (
    SELECT 
        amount,
        event_type,
        event_date,
        load_timestamp,
        update_timestamp,
        source_system
    FROM {{ source('bronze', 'bz_billing_events') }}
    WHERE amount IS NOT NULL
      AND amount >= 0
      AND event_type IS NOT NULL
      AND event_date IS NOT NULL
),

deduped_billing_events AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY amount, event_type, event_date
            ORDER BY COALESCE(update_timestamp, load_timestamp) DESC
        ) AS row_num
    FROM bronze_billing_events
),

transformed_billing_events AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['amount', 'event_type', 'event_date']) }} AS billing_event_id,
        ROUND(amount, 2) AS amount,
        UPPER(TRIM(event_type)) AS event_type,
        event_date,
        CURRENT_DATE() AS load_date,
        CURRENT_DATE() AS update_date,
        COALESCE(source_system, 'ZOOM_BRONZE_SCHEMA') AS source_system
    FROM deduped_billing_events
    WHERE row_num = 1
)

SELECT 
    billing_event_id,
    amount,
    event_type,
    event_date,
    load_date,
    update_date,
    source_system
FROM transformed_billing_events

{% if is_incremental() %}
    WHERE update_date > (SELECT COALESCE(MAX(update_date), '1900-01-01') FROM {{ this }})
{% endif %}
