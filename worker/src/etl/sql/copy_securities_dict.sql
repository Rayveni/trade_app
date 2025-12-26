INSERT INTO {target_table} (secid, shortname, regnumber, name, isin, is_traded, emitent_id, emitent_title, emitent_inn, emitent_okpo, type, "group", primary_boardid, marketprice_boardid)
SELECT secid, shortname, regnumber, name, isin, is_traded, emitent_id, emitent_title, emitent_inn, emitent_okpo, type, "group", primary_boardid, marketprice_boardid
from {source_table}
ON CONFLICT (secid) DO UPDATE
SET sys_updated =now(),
    shortname=EXCLUDED.shortname, 
	regnumber=EXCLUDED.regnumber,
	name=EXCLUDED.name, 
	isin=EXCLUDED.isin, 
	is_traded=EXCLUDED.is_traded,
	emitent_id=EXCLUDED.emitent_id,
	emitent_title=EXCLUDED.emitent_title, 
	emitent_inn=EXCLUDED.emitent_inn, 
	emitent_okpo=EXCLUDED.emitent_okpo,
	type=EXCLUDED.type,
	"group"=EXCLUDED."group", 
	primary_boardid=EXCLUDED.primary_boardid,
	marketprice_boardid=EXCLUDED.marketprice_boardid