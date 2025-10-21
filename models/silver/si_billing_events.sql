{{
  config(
    materialized='incremental',
    unique_key='billing_event_id',
    on_schema_change='sync_all_columns'
  )
}}

-- Silver layer billing events table with data quality checks and transformations
WITH bronze_billing_events AS (
  SELECT 
    amount,
    event_type,
    event_date,
    load_timestamp,
    update_timestamp,
    source_system,
    -- Generate unique billing_event_id from event details
    {{ dbt_utils.generate_surrogate_key(['event_type', 'event_date', 'amount']) }} AS billing_event_id
  FROM {{ source('zoom_bronze', 'bz_billing_events') }}
  WHERE event_type IS NOT NULL AND event_date IS NOT NULL -- Data quality check
),

-- Data quality checks and transformations
cleansed_billing_events AS (
  SELECT 
    billing_event_id,
    ROUND(COALESCE(amount, 0), 2) AS amount,
    UPPER(TRIM(event_type)) AS event_type,
    event_date,
    load_timestamp,
    update_timestamp,
    source_system,
    CURRENT_DATE() AS load_date,
    CURRENT_DATE() AS update_date
  FROM bronze_billing_events
  WHERE 
    -- Data quality validations
    UPPER(TRIM(event_type)) IN ('CHARGE', 'REFUND', 'CREDIT', 'PAYMENT') -- Valid event types
    AND COALESCE(amount, 0) >= 0 -- Amount must be non-negative
    AND event_date <= CURRENT_DATE() -- Event date cannot be in the future
),

-- Deduplication logic - keep latest record per billing event
deduped_billing_events AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY billing_event_id 
      ORDER BY COALESCE(update_timestamp, load_timestamp) DESC, load_timestamp DESC
    ) AS row_num
  FROM cleansed_billing_events
)

SELECT 
  billing_event_id,
  amount,
  event_type,
  event_date,
  load_date,
  update_date,
  source_system
FROM deduped_billing_events
WHERE row_num = 1

{% if is_incremental() %}
  AND (
    COALESCE(update_timestamp, load_timestamp) > (SELECT COALESCE(MAX(update_date), '1900-01-01') FROM {{ this }})
    OR load_timestamp > (SELECT COALESCE(MAX(load_date), '1900-01-01') FROM {{ this }})
  )
{% endif %}
