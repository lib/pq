alter system set ssl           = 'on';
alter system set ssl_ca_file   = '/tmp/testcontainers-go/postgres/ca_cert.pem';
alter system set ssl_cert_file = '/tmp/testcontainers-go/postgres/server.cert';
alter system set ssl_key_file  = '/tmp/testcontainers-go/postgres/server.key';

create role pqgossl      with login nocreatedb nocreaterole nosuperuser;
create role pqgosslcert  with login nocreatedb nocreaterole nosuperuser;
create role pqgopassword with login nocreatedb nocreaterole nosuperuser password 'wordpass';
create role pqgoscram    with login nocreatedb nocreaterole nosuperuser password 'wordpass';
create role pqgomd5      with login nocreatedb nocreaterole nosuperuser password 'wordpass';
-- md5 is deprecated and PostgreSQL will automatically treat md5 as scram in
-- most places, but we want to force it for the purpose of testing.
update pg_authid set rolpassword = 'md5' || md5('wordpasspqgomd5') where rolname = 'pqgomd5';
