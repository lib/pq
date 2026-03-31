.PHONY: all root-ssl server-ssl client-ssl intermediate-ssl server-intermediate-ssl client-intermediate-ssl

# Rebuilds self-signed root/server/client certs/keys in a consistent way
all: root-ssl server-ssl client-ssl intermediate-ssl server-intermediate-ssl client-intermediate-ssl
	rm -f .srl *.srl
	cat root.crt intermediate.crt > root+intermediate.crt

root-ssl:
	openssl req -new -sha256 -nodes -newkey rsa:2048 \
		-config ./certs/root.cnf \
		-keyout /tmp/root.key \
		-out /tmp/root.csr
	openssl x509 -req -days 3653 -sha256 \
		-in /tmp/root.csr  \
		-extfile /etc/ssl/openssl.cnf -extensions v3_ca \
		-signkey /tmp/root.key \
		-out ./certs/root.crt

server-ssl:
	openssl req -new -sha256 -nodes -newkey rsa:2048 \
		-config ./certs/server.cnf \
		-keyout ./certs/server.key \
		-out /tmp/server.csr
	openssl x509 -req -days 3653 -sha256 \
		-extfile ./certs/server.cnf -extensions req_ext \
		-CA ./certs/root.crt -CAkey /tmp/root.key -CAcreateserial \
		-in /tmp/server.csr \
		-out ./certs/server.crt

client-ssl:
	openssl req -new -sha256 -nodes -newkey rsa:2048 \
		-config ./certs/postgresql.cnf \
		-keyout ./certs/postgresql.key \
		-out /tmp/postgresql.csr
	openssl x509 -req -days 3653 -sha256 \
		-CA ./certs/root.crt -CAkey /tmp/root.key -CAcreateserial \
		-in /tmp/postgresql.csr \
		-out ./certs/postgresql.crt

intermediate-ssl:
	openssl req -new -sha256 -nodes -newkey rsa:2048 \
		-config intermediate.cnf \
		-keyout /tmp/intermediate.key \
		-out /tmp/intermediate.csr
	openssl x509 -req -days 3653 -sha256 \
		-extfile <(printf "basicConstraints=critical,CA:TRUE,pathlen:0\nkeyUsage=critical,keyCertSign,cRLSign\nsubjectKeyIdentifier=hash\nauthorityKeyIdentifier=keyid,issuer") \
		-CA root.crt -CAkey /tmp/root.key -CAcreateserial \
		-in /tmp/intermediate.csr \
		-out intermediate.crt

server-intermediate-ssl:
	openssl req -new -sha256 -nodes -newkey rsa:2048 \
		-config server_intermediate.cnf \
		-keyout server_intermediate.key \
		-out /tmp/server_intermediate.csr
	openssl x509 -req -days 3653 -sha256 \
		-extfile server_intermediate.cnf -extensions req_ext \
		-CA intermediate.crt -CAkey /tmp/intermediate.key -CAcreateserial \
		-in /tmp/server_intermediate.csr \
		-out server_intermediate.crt

client-intermediate-ssl:
	openssl req -new -sha256 -nodes -newkey rsa:2048 \
		-config client_intermediate.cnf \
		-keyout client_intermediate.key \
		-out /tmp/client_intermediate.csr
	openssl x509 -req -days 3653 -sha256 \
		-CA intermediate.crt -CAkey /tmp/intermediate.key -CAcreateserial \
		-in /tmp/client_intermediate.csr \
		-out client_intermediate.crt
