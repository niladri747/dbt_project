select
*
from
{{ source('dev_raw','bcp_flowlog_jateng') }}
where event_date_hour='2024-01-31--01'
;
