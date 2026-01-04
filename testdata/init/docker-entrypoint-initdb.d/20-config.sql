alter system set ssl           = 'on';
alter system set ssl_ca_file   = '/ssl/root.crt';
alter system set ssl_cert_file = '/ssl/server.crt';
alter system set ssl_key_file  = '/ssl/server.key';

create role pqgossltest  with login nocreatedb nocreaterole nosuperuser;
create role pqgosslcert  with login nocreatedb nocreaterole nosuperuser;
create role pqgopassword with login nocreatedb nocreaterole nosuperuser password 'wordpass';
create role pqgoscram    with login nocreatedb nocreaterole nosuperuser password 'wordpass';
create role pqgomd5      with login nocreatedb nocreaterole nosuperuser password 'wordpass';
-- md5 is deprecated and PostgreSQL will automatically treat md5 as scram in
-- most places, but we want to force it for the purpose of testing.
update pg_authid set rolpassword = 'md5' || md5('wordpasspqgomd5') where rolname = 'pqgomd5';
