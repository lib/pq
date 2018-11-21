#!/bin/bash

set -eu

client_configure() {
	sudo chmod 600 $PQSSLCERTTEST_PATH/postgresql.key
}

pgdg_repository() {
	local sourcelist='sources.list.d/postgresql.list'

	curl -sS 'https://www.postgresql.org/media/keys/ACCC4CF8.asc' | sudo apt-key add -
	echo deb http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -cs)-pgdg main $PGVERSION | sudo tee "/etc/apt/$sourcelist"
	sudo apt-get -o Dir::Etc::sourcelist="$sourcelist" -o Dir::Etc::sourceparts='-' -o APT::Get::List-Cleanup='0' update
}

postgresql_configure() {
	local instance=$1
	case $instance in
	primary)
		sudo pg_createcluster -p 5432 $PGVERSION $instance
		;;
	secondary)
		sudo pg_createcluster -p 54321 $PGVERSION $instance
		sudo rm -rf /var/lib/postgresql/$PGVERSION/$instance
		sudo -u $PGUSER pg_basebackup -D /var/lib/postgresql/$PGVERSION/$instance -R -Xs -P -d "host=${PGHOST} port=5432"
		;;
	*)
		echo "first argument to postgresql_configure must be 'primary' or 'secondary'"
		;;
	esac

	sudo tee /etc/postgresql/$PGVERSION/$instance/pg_hba.conf > /dev/null <<-config
		local     all         all                               trust
		hostnossl all         pqgossltest 127.0.0.1/32          reject
		hostnossl all         pqgosslcert 127.0.0.1/32          reject
		hostssl   all         pqgossltest 127.0.0.1/32          trust
		hostssl   all         pqgosslcert 127.0.0.1/32          cert
		host      all         all         127.0.0.1/32          trust
		host      replication all         127.0.0.1/32          trust
		hostnossl all         pqgossltest ::1/128               reject
		hostnossl all         pqgosslcert ::1/128               reject
		hostssl   all         pqgossltest ::1/128               trust
		hostssl   all         pqgosslcert ::1/128               cert
		host      all         all         ::1/128               trust
		host      replication all         ::1/128               trust
	config

	xargs sudo install -o postgres -g postgres -m 600 -t /var/lib/postgresql/$PGVERSION/$instance/ <<-certificates
		certs/root.crt
		certs/server.crt
		certs/server.key
	certificates

	sort -VCu <<-versions ||
		$PGVERSION
		9.2
	versions
	sudo tee -a /etc/postgresql/$PGVERSION/$instance/postgresql.conf > /dev/null <<-config
		ssl_ca_file     = 'root.crt'
		ssl_cert_file   = 'server.crt'
		ssl_key_file    = 'server.key'
		wal_level       = hot_standby
		hot_standby     = on
		max_wal_senders = 2
	config

	echo 127.0.0.1 postgres | sudo tee -a /etc/hosts > /dev/null

	sudo pg_ctlcluster $PGVERSION $instance start
}

postgresql_install() {
	xargs sudo apt-get -y -o Dpkg::Options::='--force-confdef' -o Dpkg::Options::='--force-confnew' install <<-packages
		postgresql-$PGVERSION
		postgresql-server-dev-$PGVERSION
		postgresql-contrib-$PGVERSION
	packages
	# disable packaged default cluster; will add our own with postgresql_configure
	sudo service postgresql stop
	sudo pg_dropcluster $PGVERSION main
}

postgresql_uninstall() {
	sudo service postgresql stop
	xargs sudo apt-get -y --purge remove <<-packages
		libpq-dev
		libpq5
		postgresql
		postgresql-client-common
		postgresql-common
	packages
	sudo rm -rf /var/lib/postgresql
}

megacheck_install() {
	# Lock megacheck version at $MEGACHECK_VERSION to prevent spontaneous
	# new error messages in old code.
	go get -d honnef.co/go/tools/...
	git -C $GOPATH/src/honnef.co/go/tools/ checkout $MEGACHECK_VERSION
	go install honnef.co/go/tools/cmd/megacheck
	megacheck --version
}

golint_install() {
	go get golang.org/x/lint/golint
}

$@
