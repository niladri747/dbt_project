select
*
from
{{ source('dev_raw','bcp_flowlog_balnus') }}
;
