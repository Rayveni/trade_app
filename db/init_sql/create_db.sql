CREATE USER $db_user SUPERUSER;
CREATE DATABASE $db_name WITH OWNER $db_user;
GRANT ALL PRIVILEGES ON DATABASE $db_name TO $db_user;
GRANT ALL ON SCHEMA public TO $db_user;
GRANT CONNECT ON DATABASE $db_name TO $db_user;
alter user $db_user password $db_pswd;

