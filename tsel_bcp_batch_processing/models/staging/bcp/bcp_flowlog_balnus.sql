select
*
from
{{ source('dev_raw','bcp_flowlog_balnus') }}
where 1=2
;
