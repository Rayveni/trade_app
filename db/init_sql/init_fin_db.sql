
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



CREATE TABLE IF NOT EXISTS securitytypes (
  id int  not null PRIMARY KEY,
  name varchar(100) not null ,
  title varchar(800)  null ,
  sys_created timestamp  default now(),
  sys_updated timestamp   null
  
);

CREATE TABLE IF NOT EXISTS securitygroups (
  id int  not null PRIMARY KEY,
  name varchar(100) not null ,
  title varchar(800)  null ,
  is_hidden smallint null,
  sys_created timestamp  default now(),
  sys_updated timestamp   null
  
);

CREATE TABLE IF NOT EXISTS engines (
  id int  not null PRIMARY KEY,
  name varchar(100) not null ,
  title varchar(800)  null ,
  sys_created timestamp  default now(),
  sys_updated timestamp   null
  
);
CREATE TABLE IF NOT EXISTS markets (
  engine varchar(100) not null,
  id int  not null ,
  name varchar(100) not null ,
  title varchar(800)  null ,
  sys_created timestamp  default now(),
  sys_updated timestamp   null,
  PRIMARY KEY (engine, id)
);

CREATE TABLE IF NOT EXISTS securities_hist (
  boardid varchar(12) not null,
  tradedate date not null,
  shortname varchar(189) not null,
  secid varchar(51) not null,
  numtrades numeric  null,
  value numeric not null,
  open numeric  null,
  low numeric  null,
  high numeric  null,
  legalcloseprice numeric  null,
  waprice numeric  null,
  close numeric  null,
  volume numeric  null,
  marketprice2 numeric  null,
  marketprice3 numeric  null,
  admittedquote numeric  null,
  mp2valtrd numeric  null,
  marketprice3tradesvalue numeric  null,
  admittedvalue numeric  null,
  waval numeric  null,
  tradingsession int  null,
  currencyid varchar(9)  null,
  trendclspr numeric  null,
  trade_session_date date  null,
  sys_created timestamp  default now(),
  sys_updated timestamp   null
  );

  CREATE UNIQUE INDEX securities_hist_mm_idx ON securities_hist (tradedate desc,secid ,boardid);


CREATE TABLE IF NOT EXISTS boards (
  engine varchar(100) not null,
  market varchar(100) not null ,
  id int  not null ,
  board_group_id int  not null,
  boardid varchar(20) null,
  title varchar(500)  null ,
  is_traded int  not null,  
  sys_created timestamp  default now(),
  sys_updated timestamp   null ,
  PRIMARY KEY (engine,market, id)
);

create or replace view nonqual_shares as
	select *
    	from securities_dict
	where is_traded = 1
	 	  and "group" = 'stock_shares'
	      and primary_boardid = 'TQBR';

CREATE TABLE IF NOT EXISTS finam_data  (
  secid varchar(51) PRIMARY KEY,
  dividends_html bytea null,
  news_html bytea null,
  status int not null default 0,
  final_flg bool not null default false,
  error_message text null,
  sys_created timestamp not null default now(),
  sys_updated timestamp null 
) ;


CREATE TABLE IF NOT EXISTS finam_divs  (
  secid varchar(51) ,
  dividend_record_date date not null,
  dividend_value money not null,
  rate real null,
  sys_created timestamp not null default now(),
  sys_updated timestamp null 
) ;
 CREATE UNIQUE INDEX finam_dives_mm_idx ON finam_divs (secid desc,dividend_record_date);
 

 