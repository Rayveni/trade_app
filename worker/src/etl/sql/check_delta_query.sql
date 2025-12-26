with dataset as (
select start_param ,
 lag(start_param, 1, 0) OVER (
        PARTITION BY table_name
        ORDER BY start_param
    ) AS param_lag
from etl_log
where status_flg=true
and table_name='{table}'
)
select max(start_param-param_lag) as max_value from dataset;