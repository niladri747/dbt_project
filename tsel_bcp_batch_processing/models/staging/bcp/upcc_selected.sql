select
*
from
{{ source('dev_raw','upcc_selected') }}
;
