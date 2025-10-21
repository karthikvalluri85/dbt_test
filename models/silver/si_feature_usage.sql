{{ config(
    materialized='incremental',
    unique_key='feature_usage_id',
    on_schema_change='sync_all_columns',
    tags=['feature_usage', 'silver']
) }}

-- Silver layer feature usage table with data quality checks and transformations
WITH bronze_feature_usage AS (
    SELECT 
        feature_name,
        usage_date,
        usage_count,
        load_timestamp,
        update_timestamp,
        source_system
    FROM {{ source('bronze', 'bz_feature_usage') }}
    WHERE feature_name IS NOT NULL
      AND usage_date IS NOT NULL
      AND usage_count IS NOT NULL
      AND usage_count >= 0
),

deduped_feature_usage AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY feature_name, usage_date
            ORDER BY COALESCE(update_timestamp, load_timestamp) DESC
        ) AS row_num
    FROM bronze_feature_usage
),

transformed_feature_usage AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['feature_name', 'usage_date']) }} AS feature_usage_id,
        UPPER(TRIM(feature_name)) AS feature_name,
        usage_date,
        COALESCE(usage_count, 0) AS usage_count,
        CURRENT_DATE() AS load_date,
        CURRENT_DATE() AS update_date,
        COALESCE(source_system, 'ZOOM_BRONZE_SCHEMA') AS source_system
    FROM deduped_feature_usage
    WHERE row_num = 1
)

SELECT 
    feature_usage_id,
    feature_name,
    usage_date,
    usage_count,
    load_date,
    update_date,
    source_system
FROM transformed_feature_usage

{% if is_incremental() %}
    WHERE update_date > (SELECT COALESCE(MAX(update_date), '1900-01-01') FROM {{ this }})
{% endif %}
