{{
    config(
        materialized='incremental',
        file_format='iceberg',
        location_root='s3://poc-test-bcp-tsel-processed-bucket/data/iceberg/processed_temp',
        partition_by=['event_date_hour'],
        incremental_strategy='append',
        schema= 'glue_catalog.processed_temp',
	tags=["daily"],
	post_hook = "insert into glue_catalog.ctlfw.table_load_partitions
        select '{{ this }}' as tgt_tbl_name,
        max(event_date_hour) as lst_tgt_prtn_hr_val,
        substring(max(event_date_hour), 1, 10) as lst_tgt_prtn_dt_val,
        '{{ invocation_id }}' || '.' || '{{ model.unique_id }}' as lst_updtd_job_id
        from {{ this }}",
    )
}}


{% set sql_statement %}

select
      cast(date_format(to_timestamp(lst_tgt_prtn_hr_val, 'yyyy-MM-dd--HH') + interval 1 hours, 'yyyy-MM-dd--HH') as string) as time_ts_current
      from (      select max(lst_tgt_prtn_hr_val) lst_tgt_prtn_hr_val from glue_catalog.ctlfw.table_load_partitions where tgt_tbl_name = '{{ this }}')

{% endset %}


{%- set time_ts_current = dbt_utils.get_single_value(sql_statement, default="'2024-01-31--00'") -%}

with bcp_flowlog_upcc_joined_balnus (
select
msisdn,
rat,
application,
application_category,
vol_inc,
vol_out,
client_ip,
client_port,
location,
monitoring_key_tag_exploded,
provinsi,
kabupaten,
kecamatan,
quota_name,
quota_status,
reserved_2,
cgi,
quota_usage,
event_date,
event_date_hour
from {{ ref('bcp_flowlog_upcc_joined_balnus') }}
where event_date_hour >= '{{ time_ts_current }}'

),
bcp_flowlog_upcc_joined_jateng (
select
msisdn,
rat,
application,
application_category,
vol_inc,
vol_out,
client_ip,
client_port,
location,
monitoring_key_tag_exploded,
provinsi,
kabupaten,
kecamatan,
quota_name,
quota_status,
reserved_2,
cgi,
quota_usage,
event_date,
event_date_hour
from {{ ref('bcp_flowlog_upcc_joined_jateng') }}
where event_date_hour >= '{{ time_ts_current }}'

),
bcp_flowlog_upcc_union(
select * from bcp_flowlog_upcc_joined_balnus
union all
select * from bcp_flowlog_upcc_joined_jateng
)
select 
msisdn,
rat,
application,
application_category,
vol_inc,
vol_out,
client_ip,
client_port,
location,
monitoring_key_tag_exploded,
provinsi,
kabupaten,
kecamatan,
quota_name,
quota_status,
reserved_2,
cgi,
quota_usage,
event_date,
current_timestamp() as load_ts,
'etl_user' as load_user,
'{{ invocation_id }}' as wrkflw_exec_id,
'{{ invocation_id }}' || '.' || '{{ model.unique_id }}' as job_id,
event_date_hour
from 
bcp_flowlog_upcc_union
