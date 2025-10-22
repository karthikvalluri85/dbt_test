{{ config(
    materialized='table'
) }}

-- Time Dimension Generation for 5 years (2020-2025)
WITH date_spine AS (
    SELECT 
        DATEADD('day', (ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1), '2020-01-01'::DATE) AS date_value
    FROM TABLE(GENERATOR(ROWCOUNT => 2191)) -- 6 years of dates
),

filtered_dates AS (
    SELECT date_value
    FROM date_spine
    WHERE date_value <= '2025-12-31'
),

time_attributes AS (
    SELECT 
        date_value AS full_date,
        TO_NUMBER(TO_CHAR(date_value, 'YYYYMMDD')) AS date_key,
        DAYOFWEEK(date_value) AS day_of_week,
        DAYNAME(date_value) AS day_name,
        DAY(date_value) AS day_of_month,
        DAYOFYEAR(date_value) AS day_of_year,
        WEEKOFYEAR(date_value) AS week_of_year,
        MONTH(date_value) AS month_number,
        MONTHNAME(date_value) AS month_name,
        QUARTER(date_value) AS quarter_number,
        'Q' || QUARTER(date_value) AS quarter_name,
        YEAR(date_value) AS year_number,
        CASE WHEN DAYOFWEEK(date_value) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
        FALSE AS is_holiday, -- Simplified - can be enhanced with holiday calendar
        CASE 
            WHEN MONTH(date_value) >= 4 THEN YEAR(date_value)
            ELSE YEAR(date_value) - 1
        END AS fiscal_year,
        CASE 
            WHEN MONTH(date_value) IN (4, 5, 6) THEN 1
            WHEN MONTH(date_value) IN (7, 8, 9) THEN 2
            WHEN MONTH(date_value) IN (10, 11, 12) THEN 3
            ELSE 4
        END AS fiscal_quarter
    FROM filtered_dates
)

SELECT 
    UUID_STRING() AS time_dimension_id,
    date_key,
    full_date,
    day_of_week,
    day_name,
    day_of_month,
    day_of_year,
    week_of_year,
    month_number,
    month_name,
    quarter_number,
    quarter_name,
    year_number,
    is_weekend,
    is_holiday,
    fiscal_year,
    fiscal_quarter,
    CURRENT_DATE() AS load_date,
    CURRENT_DATE() AS update_date,
    'GENERATED' AS source_system
FROM time_attributes
