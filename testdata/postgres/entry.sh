#!/usr/bin/env bash
# Wrap the entrypoint so we can copy the SSL files and set the correct
# permissions. This can't be done from the /docker-entrypoint-initdb.d
# directory, as that runs as the postgres user rather than root.
set -eu

mkdir -p /docker-entrypoint-initdb.d /ssl2 /tmp/testcontainers-go/postgres

cd /ssl
cp *.key *.crt /ssl2
chown postgres:postgres /ssl2/*
chmod 600 /ssl2/*

cp root.crt /tmp/testcontainers-go/postgres/ca_cert.pem
cp server.crt /tmp/testcontainers-go/postgres/server.cert
cp server.key /tmp/testcontainers-go/postgres/server.key
chown postgres:postgres /tmp/testcontainers-go/postgres/*
chmod 600 /tmp/testcontainers-go/postgres/*

cd /init
cp ./docker-entrypoint-initdb.d/* /docker-entrypoint-initdb.d
echo '127.0.0.1 postgres' >>/etc/hosts

exec /usr/local/bin/docker-entrypoint.sh postgres "$@"
