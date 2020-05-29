module github.com/lib/pq/auth/kerberos

go 1.13

replace github.com/lib/pq => ../..

require (
	github.com/alexbrainman/sspi v0.0.0-20180613141037-e580b900e9f5
	github.com/jcmturner/gokrb5/v8 v8.2.0
	github.com/lib/pq v1.6.0
)
