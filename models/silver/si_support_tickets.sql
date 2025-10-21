{{
  config(
    materialized='incremental',
    unique_key='ticket_id',
    on_schema_change='sync_all_columns'
  )
}}

-- Silver layer support tickets table with data quality checks and transformations
WITH bronze_support_tickets AS (
  SELECT 
    resolution_status,
    open_date,
    ticket_type,
    load_timestamp,
    update_timestamp,
    source_system,
    -- Generate unique ticket_id from ticket details
    {{ dbt_utils.generate_surrogate_key(['ticket_type', 'open_date']) }} AS ticket_id
  FROM {{ source('zoom_bronze', 'bz_support_tickets') }}
  WHERE ticket_type IS NOT NULL AND open_date IS NOT NULL -- Data quality check
),

-- Data quality checks and transformations
cleansed_support_tickets AS (
  SELECT 
    ticket_id,
    UPPER(TRIM(COALESCE(resolution_status, 'OPEN'))) AS resolution_status,
    open_date,
    UPPER(TRIM(ticket_type)) AS ticket_type,
    load_timestamp,
    update_timestamp,
    source_system,
    CURRENT_DATE() AS load_date,
    CURRENT_DATE() AS update_date
  FROM bronze_support_tickets
  WHERE 
    -- Data quality validations
    UPPER(TRIM(ticket_type)) IN ('TECHNICAL', 'BILLING', 'GENERAL') -- Valid ticket types
    AND open_date <= CURRENT_DATE() -- Open date cannot be in the future
),

-- Deduplication logic - keep latest record per ticket
deduped_support_tickets AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY ticket_id 
      ORDER BY COALESCE(update_timestamp, load_timestamp) DESC, load_timestamp DESC
    ) AS row_num
  FROM cleansed_support_tickets
)

SELECT 
  ticket_id,
  resolution_status,
  open_date,
  ticket_type,
  load_date,
  update_date,
  source_system
FROM deduped_support_tickets
WHERE row_num = 1

{% if is_incremental() %}
  AND (
    COALESCE(update_timestamp, load_timestamp) > (SELECT COALESCE(MAX(update_date), '1900-01-01') FROM {{ this }})
    OR load_timestamp > (SELECT COALESCE(MAX(load_date), '1900-01-01') FROM {{ this }})
  )
{% endif %}
