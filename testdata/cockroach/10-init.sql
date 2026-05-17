create user pqgo         with login nocreatedb nocreaterole;
create user pqgossl      with login nocreatedb nocreaterole;
create user pqgosslcert  with login nocreatedb nocreaterole;
create user pqgopassword with login nocreatedb nocreaterole password 'wordpass';
create user pqgoscram    with login nocreatedb nocreaterole password 'wordpass';
create user pqgomd5      with login nocreatedb nocreaterole password 'wordpass';

grant system viewsystemtable, viewclustersetting, viewclustermetadata to pqgo;

create database pqgo;
alter role pqgo set experimental_enable_temp_tables=on;
alter role pqgo set autocommit_before_ddl=off;
alter role pqgo set default_int_size=4;

set cluster setting server.host_based_authentication.configuration = '
# TYPE     DATABASE  USER         ADDRESS  METHOD
local      all       all                   trust
# cockroach doesnt support md5, just use scram-sha-256 for it (PostgreSQL also
# silently "upgrades" md5 to scram-sha-256 since 18, and we need to do a hack to
# force it to md5).
host       all       pqgomd5      all      scram-sha-256
host       all       pqgopassword all      password
host       all       pqgoscram    all      scram-sha-256
host       all       postgres     all      trust
hostnossl  all       pqgossl      all      reject
hostnossl  all       pqgosslcert  all      reject
hostssl    all       pqgossl      all      trust
hostssl    all       pqgosslcert  all      cert
host       all       all          all      trust
';
