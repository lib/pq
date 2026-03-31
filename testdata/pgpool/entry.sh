#!/bin/sh

exec /opt/pgpool-II/bin/pgpool -n \
	-f /init/pgpool.conf \
	-F ${PGPOOL_INSTALL_DIR}/etc/pcp.conf \
	-a /init/pool_hba.conf \
	-k ${PGPOOL_INSTALL_DIR}/etc/.pgpoolkey
