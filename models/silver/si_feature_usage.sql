{{
  config(
    materialized='incremental',
    unique_key='feature_usage_id',
    on_schema_change='sync_all_columns'
  )
}}

-- Silver layer feature usage table with data quality checks and transformations
WITH bronze_feature_usage AS (
  SELECT 
    feature_name,
    usage_date,
    usage_count,
    load_timestamp,
    update_timestamp,
    source_system,
    -- Generate unique feature_usage_id from usage details
    {{ dbt_utils.generate_surrogate_key(['feature_name', 'usage_date']) }} AS feature_usage_id
  FROM {{ source('zoom_bronze', 'bz_feature_usage') }}
  WHERE feature_name IS NOT NULL AND usage_date IS NOT NULL -- Data quality check
),

-- Data quality checks and transformations
cleansed_feature_usage AS (
  SELECT 
    feature_usage_id,
    UPPER(TRIM(feature_name)) AS feature_name,
    usage_date,
    COALESCE(usage_count, 0) AS usage_count,
    load_timestamp,
    update_timestamp,
    source_system,
    CURRENT_DATE() AS load_date,
    CURRENT_DATE() AS update_date
  FROM bronze_feature_usage
  WHERE 
    -- Data quality validations
    UPPER(TRIM(feature_name)) IN ('SCREEN SHARE', 'RECORDING', 'CHAT', 'BREAKOUT ROOMS') -- Valid feature names
    AND COALESCE(usage_count, 0) >= 0 -- Usage count must be non-negative
    AND usage_date <= CURRENT_DATE() -- Usage date cannot be in the future
),

-- Deduplication logic - keep latest record per feature usage
deduped_feature_usage AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY feature_usage_id 
      ORDER BY COALESCE(update_timestamp, load_timestamp) DESC, load_timestamp DESC
    ) AS row_num
  FROM cleansed_feature_usage
)

SELECT 
  feature_usage_id,
  feature_name,
  usage_date,
  usage_count,
  load_date,
  update_date,
  source_system
FROM deduped_feature_usage
WHERE row_num = 1

{% if is_incremental() %}
  AND (
    COALESCE(update_timestamp, load_timestamp) > (SELECT COALESCE(MAX(update_date), '1900-01-01') FROM {{ this }})
    OR load_timestamp > (SELECT COALESCE(MAX(load_date), '1900-01-01') FROM {{ this }})
  )
{% endif %}
