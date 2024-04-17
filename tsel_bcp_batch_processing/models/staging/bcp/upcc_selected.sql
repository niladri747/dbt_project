select
*
from
{{ source('dev_raw','upcc_selected') }}
where 1=2
;
