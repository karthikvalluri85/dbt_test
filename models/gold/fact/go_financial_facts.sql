{{ config(
    materialized='table',
    pre_hook="INSERT INTO {{ this.schema }}.go_process_audit (audit_id, pipeline_run_id, pipeline_name, execution_start_time, execution_status, source_system, target_table, load_date) SELECT UUID_STRING(), '{{ invocation_id }}', 'go_financial_facts', CURRENT_TIMESTAMP(), 'STARTED', 'ZOOM_SILVER', 'go_financial_facts', CURRENT_DATE()",
    post_hook="UPDATE {{ this.schema }}.go_process_audit SET execution_end_time = CURRENT_TIMESTAMP(), execution_status = 'SUCCESS', execution_duration_seconds = DATEDIFF('second', execution_start_time, CURRENT_TIMESTAMP()), records_processed = (SELECT COUNT(*) FROM {{ this }}), update_date = CURRENT_DATE() WHERE pipeline_run_id = '{{ invocation_id }}' AND target_table = 'go_financial_facts'"
) }}

-- Financial Facts with revenue impact calculations
WITH financial_base AS (
    SELECT 
        billing_event_id,
        amount,
        event_type,
        event_date,
        load_date,
        update_date,
        source_system
    FROM {{ source('zoom_silver', 'si_billing_events') }}
    WHERE billing_event_id IS NOT NULL
      AND amount IS NOT NULL
      AND event_type IS NOT NULL
      AND event_date IS NOT NULL
),

financial_calculations AS (
    SELECT 
        billing_event_id,
        ROUND(amount, 2) AS amount,
        UPPER(TRIM(event_type)) AS event_type,
        event_date,
        CASE 
            WHEN UPPER(TRIM(event_type)) IN ('CHARGE', 'PAYMENT') THEN ROUND(amount, 2)
            WHEN UPPER(TRIM(event_type)) IN ('REFUND', 'CREDIT') THEN ROUND(-amount, 2)
            ELSE 0
        END AS revenue_impact,
        CASE 
            WHEN UPPER(TRIM(event_type)) = 'CHARGE' THEN ROUND(amount * 0.029 + 0.30, 2)
            ELSE 0
        END AS transaction_fee,
        load_date,
        update_date,
        source_system
    FROM financial_base
    WHERE amount >= 0 -- Ensure non-negative amounts
),

financial_facts AS (
    SELECT 
        billing_event_id,
        amount,
        event_type,
        event_date,
        revenue_impact,
        SUM(revenue_impact) OVER (
            ORDER BY event_date, billing_event_id 
            ROWS UNBOUNDED PRECEDING
        ) AS cumulative_revenue,
        transaction_fee,
        ROUND(amount - transaction_fee, 2) AS net_amount,
        load_date,
        update_date,
        source_system
    FROM financial_calculations
)

SELECT 
    UUID_STRING() AS financial_fact_id,
    billing_event_id,
    amount,
    event_type,
    event_date,
    revenue_impact,
    cumulative_revenue,
    transaction_fee,
    net_amount,
    COALESCE(load_date, CURRENT_DATE()) AS load_date,
    COALESCE(update_date, CURRENT_DATE()) AS update_date,
    COALESCE(source_system, 'ZOOM_SILVER') AS source_system
FROM financial_facts
