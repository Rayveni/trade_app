update finam_data
set
	dividends_html=%s,
	error_message={error_message},
	dividends_flag={dividends_flag},
	sys_updated=now ()
where
	secid='{secid}';

