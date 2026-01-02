#!/usr/bin/env bash
# Wrap the entrypoint so we can copy the SSL files and set the correct
# permissions. This can't be done from the /docker-entrypoint-initdb.d
# directory, as that runs as the postgres user rather than root.
set -eu

mkdir -p /docker-entrypoint-initdb.d /ssl

cd /init
cp ./docker-entrypoint-initdb.d/* /docker-entrypoint-initdb.d

cp *.key *.crt /ssl
chown postgres:postgres /ssl/*
chmod 600 /ssl/*

echo '127.0.0.1 postgres' >>/etc/hosts

exec /usr/local/bin/docker-entrypoint.sh postgres "$@"
