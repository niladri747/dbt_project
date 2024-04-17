{{
    config(
        materialized='incremental',
        file_format='iceberg',
        location_root='s3://poc-test-bcp-tsel-processed-bucket/data/iceberg/processed_temp',
        partition_by=['event_date_hour'],
        incremental_strategy='append',
        schema= 'glue_catalog.processed_temp',
        post_hook = "insert into glue_catalog.ctlfw.table_load_partitions
        select '{{ this }}' as tgt_tbl_name,
        max(event_date_hour) as lst_tgt_prtn_hr_val,
        substring(max(event_date_hour), 1, 10) as lst_tgt_prtn_dt_val,
        '{{ invocation_id }}' || '{{ model.unique_id }}' as lst_updtd_job_id
        from {{ this }}",
    )
}}

{% set sql_statement1 %}

select
      cast(date_format(to_timestamp(lst_tgt_prtn_hr_val, 'yyyy-MM-dd--HH') + interval 2 hours, 'yyyy-MM-dd--HH') as string) as time_ts_plus
      from (      select max(lst_tgt_prtn_hr_val) lst_tgt_prtn_hr_val from glue_catalog.ctlfw.table_load_partitions where tgt_tbl_name = '{{ this }}')

{% endset %}

{% set sql_statement2 %}
    
select
      cast(date_format(to_timestamp(lst_tgt_prtn_hr_val, 'yyyy-MM-dd--HH') + interval 1 hours, 'yyyy-MM-dd--HH') as string) as time_ts_current
      from (      select max(lst_tgt_prtn_hr_val) lst_tgt_prtn_hr_val from glue_catalog.ctlfw.table_load_partitions where tgt_tbl_name = '{{ this }}')

{% endset %}

{% set sql_statement3 %}
    
select
      lst_tgt_prtn_hr_val
      from (      select max(lst_tgt_prtn_hr_val) lst_tgt_prtn_hr_val from glue_catalog.ctlfw.table_load_partitions where tgt_tbl_name = '{{ this }}')

{% endset %}

{%- set time_ts_plus = dbt_utils.get_single_value(sql_statement1, default="'2024-01-31--00'") -%}

{%- set time_ts_current = dbt_utils.get_single_value(sql_statement2, default="'2024-01-31--00'") -%}

{%- set time_ts_minus = dbt_utils.get_single_value(sql_statement3, default="'2024-01-31--00'") -%}

with stg_bcp_flowlog_balnus as(
select
tokenized_msisdn as msisdn,
timestamp,
to_timestamp(timestamp, 'yyyyMMddHHmmss') as timestamp_ts,
rat,
application,
application_category,
vol_inc,
vol_out,
client_ip,
client_port,
location,
monitoring_key_tag,
explode(split(monitoring_key_tag,',')) monitoring_key_tag_exploded,
event_date_hour
from {{ ref('bcp_flowlog_balnus') }}
where 
event_date_hour = '{{ time_ts_current }}'



),

stg_upcc_edr as (
select 
tokenized_msisdn as msisdn,
time,
to_timestamp(time, 'yyyy-MM-dd HH:mm:ss') as time_ts,
kabupaten,
kecamatan,
quota_name,
quota_status,
ifnull(reserved_2,'0') reserved_2,
cgi,
quota_usage,
event_date,
event_date_hour
from {{ ref('upcc_selected') }}
where event_date_hour between '{{ time_ts_minus }}' and '{{ time_ts_plus }}'
),

bcp_flowlog_upcc_joined_balnus as (
select 
upcc_edr.msisdn,
bcp_flowlog.rat,
bcp_flowlog.application,
bcp_flowlog.application_category,
bcp_flowlog.vol_inc,
bcp_flowlog.vol_out,
bcp_flowlog.client_ip,
bcp_flowlog.client_port,
bcp_flowlog.location,
bcp_flowlog.monitoring_key_tag,
bcp_flowlog.monitoring_key_tag_exploded,
upcc_edr.kabupaten,
upcc_edr.kecamatan,
upcc_edr.quota_name,
upcc_edr.quota_status,
upcc_edr.reserved_2,
upcc_edr.cgi,
upcc_edr.quota_usage,
upcc_edr.event_date,
bcp_flowlog.event_date_hour
from stg_bcp_flowlog_balnus bcp_flowlog
inner join stg_upcc_edr upcc_edr
on bcp_flowlog.msisdn = upcc_edr.msisdn
and bcp_flowlog.monitoring_key_tag_exploded = upcc_edr.reserved_2
and timestamp_ts >= (time_ts - interval 1 hours) AND timestamp_ts <= (time_ts + interval 1 hours)
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
monitoring_key_tag,
monitoring_key_tag_exploded,
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
'{{ invocation_id }}' || '{{ model.unique_id }}' as job_id,
event_date_hour
from bcp_flowlog_upcc_joined_balnus


