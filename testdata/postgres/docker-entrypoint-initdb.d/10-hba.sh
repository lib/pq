#!/bin/sh
set -eu

cat <<EOF >"$PGDATA/pg_hba.conf"
# TYPE     DATABASE  USER         ADDRESS  METHOD
local      all       all                   trust
host       all       pqgomd5      all      md5
host       all       pqgopassword all      password
host       all       pqgoscram    all      scram-sha-256
host       all       postgres     all      trust
hostnossl  all       pqgossl      all      reject
hostnossl  all       pqgosslcert  all      reject
hostssl    all       pqgossl      all      trust
hostssl    all       pqgosslcert  all      cert
host       all       all          all      trust
EOF
