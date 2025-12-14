CREATE TABLE IF NOT EXISTS raw_securities (
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