insert into etl_log(table_name,status_flg,secid)
select 'finam_data_divs' as table_name
        ,false as status_flg
        ,secid 
from nonqual_shares;