alter system set ssl           = 'on';
alter system set ssl_ca_file   = '/ssl/root.crt';
alter system set ssl_cert_file = '/ssl/server.crt';
alter system set ssl_key_file  = '/ssl/server.key';

create role pqgossltest with login nocreatedb nocreaterole nosuperuser;
create role pqgosslcert with login nocreatedb nocreaterole nosuperuser;
