with
	sec_list as (
		select distinct secid
		from securities_dict
		where
			is_traded={is_traded}
			and "group"='{group}'
			and primary_boardid='{primary_boardid}'
	)
insert into
	finam_data (secid)
select *
from sec_list on CONFLICT (secid) DO
update
set
	dividends_html=null,
	news_html=null,
	error_message=null,
	dividends_flag=false,
	sys_updated=now () 
RETURNING secid;