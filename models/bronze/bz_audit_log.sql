-- Bronze Audit Log Model
-- This model creates the audit log table to track processing information
-- for all bronze layer transformations

{{ config(
    materialized='table',
    pre_hook="",
    post_hook=""
) }}

with audit_base as (
    select
        cast(null as number) as record_id,
        cast(null as varchar(255)) as source_table,
        cast(null as timestamp_ntz) as load_timestamp,
        cast(null as varchar(100)) as processed_by,
        cast(null as number) as processing_time,
        cast(null as varchar(50)) as status
    where 1=0  -- This ensures no data is loaded initially
)

select * from audit_base
