INSERT INTO finam_data (secid, dividends_html, dividends_update)
VALUES ('{secid}',%s,now())
ON CONFLICT (secid) DO UPDATE
SET dividends_html=%s,
    dividends_update=now();