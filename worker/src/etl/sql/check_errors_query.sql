select status_flg from etl_log
where table_name='{table}' and status_flg=false
limit 1