with _temp_table as (
		select distinct secid, dividend_record_date, dividend_value, case when rate='None' then null else rate end rate
		from
			(
				values %s
			) as TempTable (secid, dividend_record_date, dividend_value, rate)
	)
insert into
	finam_divs (secid,dividend_record_date , dividend_value, rate)

select secid, cast(dividend_record_date as date) , dividend_value, cast(rate as real)
from 
	_temp_table on CONFLICT (secid, dividend_record_date, dividend_value) DO
update
set
	secid=EXCLUDED.secid,
	dividend_record_date=EXCLUDED.dividend_record_date,
	dividend_value=EXCLUDED.dividend_value,
	rate=EXCLUDED.rate,
	sys_updated=now ();
