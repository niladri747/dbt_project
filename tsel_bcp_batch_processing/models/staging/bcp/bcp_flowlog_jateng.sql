select
*
from
{{ source('dev_raw','bcp_flowlog_jateng') }}
where 1=2
;
