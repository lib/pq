#!/bin/sh
set -eu

cat <<EOF >"$PGDATA/pg_hba.conf"
local     all         all                               trust
host      all         postgres    all                   trust
hostnossl all         pqgossltest all                   reject
hostnossl all         pqgosslcert all                   reject
hostssl   all         pqgossltest all                   trust
hostssl   all         pqgosslcert all                   cert
host      all         all         all                   trust
EOF
