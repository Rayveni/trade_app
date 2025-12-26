
CREATE TABLE IF NOT EXISTS securities_dict (
  secid varchar(51) PRIMARY KEY,
  shortname varchar(189),
  regnumber varchar(189),
  "name" varchar(765),
  isin varchar(51),
  is_traded int,
  emitent_id int,
  emitent_title varchar(765),
  emitent_inn varchar(30),
  emitent_okpo varchar(24),
  "type" varchar(93),
  "group" varchar(93),
  primary_boardid varchar(12),
  marketprice_boardid varchar(12),
  sys_created timestamp  default now(),
  sys_updated timestamp   null
);
create index idx_is_traded
on securities_dict (is_traded);

CREATE TABLE IF NOT EXISTS temp_securities_dict (LIKE securities_dict including all);

CREATE TABLE IF NOT EXISTS etl_log (
  table_name varchar(51)  not null,
  status_flg bool not null default false,
  start_param int not null default 0,
  oper_date date not null default date '1900-01-01',
  query json null,
  error_message text null,
  sys_created timestamp  default now(),
  sys_updated timestamp   null
  
);

CREATE UNIQUE INDEX etl_log_mm_idx ON etl_log (table_name,start_param,oper_date);
