{{
  config(
    materialized='incremental',
    unique_key='license_id',
    on_schema_change='sync_all_columns'
  )
}}

-- Silver layer licenses table with data quality checks and transformations
WITH bronze_licenses AS (
  SELECT 
    license_type,
    end_date,
    start_date,
    load_timestamp,
    update_timestamp,
    source_system,
    -- Generate unique license_id from license details
    {{ dbt_utils.generate_surrogate_key(['license_type', 'start_date']) }} AS license_id
  FROM {{ source('zoom_bronze', 'bz_licenses') }}
  WHERE license_type IS NOT NULL AND start_date IS NOT NULL -- Data quality check
),

-- Data quality checks and transformations
cleansed_licenses AS (
  SELECT 
    license_id,
    UPPER(TRIM(license_type)) AS license_type,
    end_date,
    start_date,
    load_timestamp,
    update_timestamp,
    source_system,
    CURRENT_DATE() AS load_date,
    CURRENT_DATE() AS update_date
  FROM bronze_licenses
  WHERE 
    -- Data quality validations
    UPPER(TRIM(license_type)) IN ('BASIC', 'PRO', 'ENTERPRISE') -- Valid license types
    AND (end_date IS NULL OR end_date >= start_date) -- End date must be after start date if provided
    AND start_date <= CURRENT_DATE() -- Start date cannot be in the future
),

-- Deduplication logic - keep latest record per license
deduped_licenses AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY license_id 
      ORDER BY COALESCE(update_timestamp, load_timestamp) DESC, load_timestamp DESC
    ) AS row_num
  FROM cleansed_licenses
)

SELECT 
  license_id,
  license_type,
  end_date,
  start_date,
  load_date,
  update_date,
  source_system
FROM deduped_licenses
WHERE row_num = 1

{% if is_incremental() %}
  AND (
    COALESCE(update_timestamp, load_timestamp) > (SELECT COALESCE(MAX(update_date), '1900-01-01') FROM {{ this }})
    OR load_timestamp > (SELECT COALESCE(MAX(load_date), '1900-01-01') FROM {{ this }})
  )
{% endif %}
