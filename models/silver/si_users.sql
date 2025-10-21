{{ config(
    materialized='incremental',
    unique_key='user_id',
    on_schema_change='sync_all_columns',
    tags=['users', 'silver']
) }}

-- Silver layer users table with data quality checks and transformations
WITH bronze_users AS (
    SELECT 
        email,
        user_name,
        plan_type,
        company,
        load_timestamp,
        update_timestamp,
        source_system
    FROM {{ source('bronze', 'bz_users') }}
    WHERE email IS NOT NULL
      AND user_name IS NOT NULL
      AND plan_type IS NOT NULL
),

deduped_users AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY LOWER(TRIM(email))
            ORDER BY COALESCE(update_timestamp, load_timestamp) DESC
        ) AS row_num
    FROM bronze_users
),

transformed_users AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['email']) }} AS user_id,
        LOWER(TRIM(email)) AS email,
        TRIM(user_name) AS user_name,
        UPPER(TRIM(plan_type)) AS plan_type,
        TRIM(company) AS company,
        CURRENT_DATE() AS load_date,
        CURRENT_DATE() AS update_date,
        COALESCE(source_system, 'ZOOM_BRONZE_SCHEMA') AS source_system
    FROM deduped_users
    WHERE row_num = 1
)

SELECT 
    user_id,
    email,
    user_name,
    plan_type,
    company,
    load_date,
    update_date,
    source_system
FROM transformed_users

{% if is_incremental() %}
    WHERE update_date > (SELECT COALESCE(MAX(update_date), '1900-01-01') FROM {{ this }})
{% endif %}
