select
*
from
{{ source('dev_raw','upcc_selected') }}
where event_date_hour='2024-01-31--01'
;
