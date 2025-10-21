{{
  config(
    materialized='table',
    pre_hook="",
    post_hook=""
  )
}}

/*
=============================================================================
BRONZE LAYER AUDIT LOG MODEL
=============================================================================
Purpose: Create audit log table for tracking bronze layer data processing
Author: DBT Data Engineering Team
Created: {{ run_started_at }}
Description: This model creates the audit log infrastructure for bronze layer
=============================================================================
*/

-- Create audit log table structure
SELECT 
    -- Audit Attributes
    CAST(NULL AS NUMBER) AS record_id,
    CAST(NULL AS VARCHAR(255)) AS source_table,
    CAST(NULL AS TIMESTAMP_NTZ) AS load_timestamp,
    CAST(NULL AS VARCHAR(100)) AS processed_by,
    CAST(NULL AS NUMBER) AS processing_time,
    CAST(NULL AS VARCHAR(50)) AS status
WHERE 1=0  -- This ensures no data is inserted, only structure is created
