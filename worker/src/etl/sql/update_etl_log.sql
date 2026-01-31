update  etl_log
set status_flg=true,
    sys_updated=now()
where table_name='{table}' and secid='{secid}'