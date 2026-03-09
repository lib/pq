package pq

import (
	"fmt"
	"os"
	"testing"

	"github.com/lib/pq/internal/pqtest"
)

func TestSSLClientCertificateIntermediate(t *testing.T) {
	pqtest.SkipPgpool(t)
	pqtest.SkipPgbouncer(t)

	startSSLTest(t, "pqgosslcert")

	err := os.Chmod("testdata/init/client_intermediate.key", 0600)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		connect string
		wantErr string
	}{
		{
			// Client cert signed by intermediate CA, sslrootcert has
			// root+intermediate bundle. The server's ssl_ca_file has only root.crt,
			// so sslAppendIntermediates must send the intermediate in the TLS chain.
			name: "file certs",
			connect: "sslmode=require user=pqgosslcert " +
				"sslrootcert=testdata/init/root+intermediate.crt " +
				"sslcert=testdata/init/client_intermediate.crt " +
				"sslkey=testdata/init/client_intermediate.key",
		},
		{
			name: "inline certs",
			connect: fmt.Sprintf(
				"sslmode=require user=pqgosslcert sslinline=true sslrootcert='%s' sslcert='%s' sslkey='%s'",
				pqtest.Read(t, "testdata/init/root+intermediate.crt"),
				pqtest.Read(t, "testdata/init/client_intermediate.crt"),
				pqtest.Read(t, "testdata/init/client_intermediate.key"),
			),
		},
		{
			// Without the intermediate in sslrootcert, sslAppendIntermediates has
			// nothing to append, so the server can't verify the client cert chain.
			name: "fails without intermediate in sslrootcert",
			connect: "sslmode=require user=pqgosslcert " +
				"sslrootcert=testdata/init/root.crt " +
				"sslcert=testdata/init/client_intermediate.crt " +
				"sslkey=testdata/init/client_intermediate.key",
			wantErr: "unknown certificate authority",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db, err := openSSLConn(t, tt.connect)
			if !pqtest.ErrorContains(err, tt.wantErr) {
				t.Fatalf("wrong error\nwant: %s\nhave: %s", tt.wantErr, err)
			}
			if err == nil {
				rows, err := db.Query("select 1")
				if err != nil {
					t.Fatal(err)
				}
				if err := rows.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
