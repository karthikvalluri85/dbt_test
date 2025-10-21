{{
  config(
    materialized='incremental',
    unique_key='user_id',
    on_schema_change='sync_all_columns',
    pre_hook="INSERT INTO {{ this.schema }}.si_audit_log (audit_id, pipeline_name, execution_start_time, execution_status, source_table, target_table, executed_by, load_date) SELECT '{{ invocation_id }}' || '_si_users_START', 'SI_USERS_TRANSFORM', CURRENT_TIMESTAMP(), 'STARTED', 'bz_users', 'si_users', 'DBT_SYSTEM', CURRENT_DATE()",
    post_hook="INSERT INTO {{ this.schema }}.si_audit_log (audit_id, pipeline_name, execution_end_time, execution_status, source_table, target_table, executed_by, load_date) SELECT '{{ invocation_id }}' || '_si_users_END', 'SI_USERS_TRANSFORM', CURRENT_TIMESTAMP(), 'COMPLETED', 'bz_users', 'si_users', 'DBT_SYSTEM', CURRENT_DATE()"
  )
}}

-- Silver layer users table with data quality checks and transformations
WITH bronze_users AS (
  SELECT 
    email,
    user_name,
    plan_type,
    company,
    load_timestamp,
    update_timestamp,
    source_system,
    -- Generate unique user_id from email
    {{ dbt_utils.generate_surrogate_key(['email']) }} AS user_id
  FROM {{ source('zoom_bronze', 'bz_users') }}
  WHERE email IS NOT NULL -- Data quality check: exclude null emails
),

-- Data quality checks and transformations
cleansed_users AS (
  SELECT 
    user_id,
    LOWER(TRIM(email)) AS email,
    TRIM(user_name) AS user_name,
    UPPER(TRIM(plan_type)) AS plan_type,
    TRIM(company) AS company,
    load_timestamp,
    update_timestamp,
    source_system,
    CURRENT_DATE() AS load_date,
    CURRENT_DATE() AS update_date
  FROM bronze_users
  WHERE 
    -- Data quality validations
    REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$') -- Valid email format
    AND plan_type IN ('BASIC', 'PRO', 'BUSINESS', 'ENTERPRISE') -- Valid plan types
    AND LENGTH(TRIM(user_name)) > 0 -- Non-empty user name
),

-- Deduplication logic - keep latest record per user
deduped_users AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY user_id 
      ORDER BY update_timestamp DESC, load_timestamp DESC
    ) AS row_num
  FROM cleansed_users
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
FROM deduped_users
WHERE row_num = 1

{% if is_incremental() %}
  AND (
    update_timestamp > (SELECT MAX(update_date) FROM {{ this }})
    OR load_timestamp > (SELECT MAX(load_date) FROM {{ this }})
  )
{% endif %}
