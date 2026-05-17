#!/bin/sh
set -euC

mkdir -p /ssl2
cp /ssl/root.crt   /ssl2/ca.crt
cp /ssl/server.crt /ssl2/node.crt
cp /ssl/server.key /ssl2/node.key

# Reload the certificates
kill -HUP $(</cockroach/server_pid)
