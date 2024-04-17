{{
    config(
        materialized='incremental',
        file_format='iceberg',
        location_root='s3://poc-test-bcp-tsel-processed-bucket/data/iceberg/processed_target',
        partition_by=['event_date'],
        incremental_strategy='append',
        schema= 'glue_catalog.processed_target',
	post_hook = "insert into glue_catalog.ctlfw.table_load_partitions
        select '{{ this }}' as tgt_tbl_name,
        max(event_date_hour) as lst_tgt_prtn_hr_val,
        substring(max(event_date_hour), 1, 10) as lst_tgt_prtn_dt_val,
        '{{ invocation_id }}' || '{{ model.unique_id }}' as lst_updtd_job_id
        from {{ ref('bcp_flowlog_upcc_union') }}",
    )
}}

with bcp_flowlog_upcc as (
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
kabupaten,
kecamatan,
quota_name,
quota_status,
reserved_2,
cgi,
quota_usage,
event_date,
event_date_hour
from 
{{ ref('bcp_flowlog_upcc_union') }}

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where event_date_hour > (select max(lst_tgt_prtn_hr_val) from glue_catalog.ctlfw.table_load_partitions where tgt_tbl_name = '{{ this }}' )

{% endif %}
),
bcp_flowlog_upcc_dd as (
select
msisdn,
rat,
application,
application_category,
sum(vol_inc) as vol_inc,
sum(vol_out) as vol_out,
client_ip,
client_port,
location,
monitoring_key_tag_exploded,
kabupaten,
kecamatan,
quota_name,
quota_status,
reserved_2,
cgi,
sum(quota_usage) quota_usage,
event_date
from
bcp_flowlog_upcc
group by
msisdn,
rat,
application,
application_category,
client_ip,
client_port,
location,
monitoring_key_tag_exploded,
kabupaten,
kecamatan,
quota_name,
quota_status,
reserved_2,
cgi,
event_date
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
kabupaten,
kecamatan,
quota_name,
quota_status,
reserved_2,
cgi,
quota_usage,
current_timestamp() as load_ts,
'etl_user' as load_user,
'{{ invocation_id }}' as wrkflw_exec_id,
'{{ invocation_id }}' || '{{ model.unique_id }}' as job_id,
event_date
from
bcp_flowlog_upcc_dd


