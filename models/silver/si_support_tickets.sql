{{ config(
    materialized='incremental',
    unique_key='ticket_id',
    on_schema_change='sync_all_columns',
    tags=['support_tickets', 'silver']
) }}

-- Silver layer support tickets table with data quality checks and transformations
WITH bronze_support_tickets AS (
    SELECT 
        resolution_status,
        open_date,
        ticket_type,
        load_timestamp,
        update_timestamp,
        source_system
    FROM {{ source('bronze', 'bz_support_tickets') }}
    WHERE resolution_status IS NOT NULL
      AND open_date IS NOT NULL
      AND ticket_type IS NOT NULL
),

deduped_tickets AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY ticket_type, open_date, resolution_status
            ORDER BY COALESCE(update_timestamp, load_timestamp) DESC
        ) AS row_num
    FROM bronze_support_tickets
),

transformed_tickets AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['ticket_type', 'open_date', 'resolution_status']) }} AS ticket_id,
        UPPER(TRIM(resolution_status)) AS resolution_status,
        open_date,
        UPPER(TRIM(ticket_type)) AS ticket_type,
        CURRENT_DATE() AS load_date,
        CURRENT_DATE() AS update_date,
        COALESCE(source_system, 'ZOOM_BRONZE_SCHEMA') AS source_system
    FROM deduped_tickets
    WHERE row_num = 1
)

SELECT 
    ticket_id,
    resolution_status,
    open_date,
    ticket_type,
    load_date,
    update_date,
    source_system
FROM transformed_tickets

{% if is_incremental() %}
    WHERE update_date > (SELECT COALESCE(MAX(update_date), '1900-01-01') FROM {{ this }})
{% endif %}
