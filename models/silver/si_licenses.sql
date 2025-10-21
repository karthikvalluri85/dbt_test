{{ config(
    materialized='incremental',
    unique_key='license_id',
    on_schema_change='sync_all_columns',
    tags=['licenses', 'silver']
) }}

-- Silver layer licenses table with data quality checks and transformations
WITH bronze_licenses AS (
    SELECT 
        license_type,
        end_date,
        start_date,
        load_timestamp,
        update_timestamp,
        source_system
    FROM {{ source('bronze', 'bz_licenses') }}
    WHERE license_type IS NOT NULL
      AND start_date IS NOT NULL
      AND end_date IS NOT NULL
      AND end_date >= start_date
),

deduped_licenses AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY license_type, start_date, end_date
            ORDER BY COALESCE(update_timestamp, load_timestamp) DESC
        ) AS row_num
    FROM bronze_licenses
),

transformed_licenses AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['license_type', 'start_date', 'end_date']) }} AS license_id,
        UPPER(TRIM(license_type)) AS license_type,
        end_date,
        start_date,
        CURRENT_DATE() AS load_date,
        CURRENT_DATE() AS update_date,
        COALESCE(source_system, 'ZOOM_BRONZE_SCHEMA') AS source_system
    FROM deduped_licenses
    WHERE row_num = 1
)

SELECT 
    license_id,
    license_type,
    end_date,
    start_date,
    load_date,
    update_date,
    source_system
FROM transformed_licenses

{% if is_incremental() %}
    WHERE update_date > (SELECT COALESCE(MAX(update_date), '1900-01-01') FROM {{ this }})
{% endif %}
