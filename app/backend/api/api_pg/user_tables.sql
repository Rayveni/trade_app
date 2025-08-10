select schemaname,tablename 
from pg_catalog.pg_tables 
where schemaname not in ('pg_catalog','information_schema')