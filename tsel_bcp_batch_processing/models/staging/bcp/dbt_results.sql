{{
  config(
    materialized = 'incremental',
    file_format='iceberg',
    location_root='s3://poc-test-bcp-tsel-processed-bucket/data/iceberg/ctlfw',
    unique_key = 'result_id',
    incremental_strategy='append',
    schema= 'glue_catalog.ctlfw',
  )
}}

with empty_table as (
    select
        '1' as result_id,
        '1' as invocation_id,
        '1' as job_id,
        '1' as database_name,
        '1' as schema_name,
        '1' as name,
        '1' as resource_type,
        '1' as status,
        cast('1' as float) as execution_time,
        cast('1' as int) as rows_affected
)

select * from empty_table
-- This is a filter so we will never actually insert these values
where 1 = 0
