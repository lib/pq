package pq

// This file contains SSL tests

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func shouldSkipSSLTests(t *testing.T) bool {
	// Require some special variables for testing certificates
	if os.Getenv("PQSSLCERTTEST_PATH") == "" {
		return true
	}

	value := os.Getenv("PQGOSSLTESTS")
	if value == "" || value == "0" {
		return true
	} else if value == "1" {
		return false
	} else {
		t.Fatalf("unexpected value %q for PQGOSSLTESTS", value)
	}
	panic("not reached")
}

func openSSLConn(t *testing.T, conninfo string) (*sql.DB, error) {
	db, err := openTestConnConninfo(conninfo)
	if err != nil {
		// should never fail
		t.Fatal(err)
	}
	// Do something with the connection to see whether it's working or not.
	tx, err := db.Begin()
	if err == nil {
		return db, tx.Rollback()
	}
	_ = db.Close()
	return nil, err
}

func checkSSLSetup(t *testing.T, conninfo string) {
	db, err := openSSLConn(t, conninfo)
	if err == nil {
		db.Close()
		t.Fatal("expected error with conninfo=%q", conninfo)
	}
}

// Connect over SSL and run a simple query to test the basics
func TestSSLConnection(t *testing.T) {
	if shouldSkipSSLTests(t) {
		t.Log("skipping SSL test")
		return
	}
	// Environment sanity check: should fail without SSL
	checkSSLSetup(t, "sslmode=disable user=pqgossltest")

	db, err := openSSLConn(t, "sslmode=require user=pqgossltest")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query("SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	rows.Close()
}

func getCertConninfo(t *testing.T, source string) string {
	var sslkey string
	var sslcert string

	certpath := os.Getenv("PQSSLCERTTEST_PATH")

	switch source {
	case "missingkey":
		sslkey = "/tmp/filedoesnotexist"
		sslcert = filepath.Join(certpath, "postgresql.crt")
	case "missingcert":
		sslkey = filepath.Join(certpath, "postgresql.key")
		sslcert = "/tmp/filedoesnotexist"
	case "certtwice":
		sslkey = filepath.Join(certpath, "postgresql.crt")
		sslcert = filepath.Join(certpath, "postgresql.crt")
	case "valid":
		sslkey = filepath.Join(certpath, "postgresql.key")
		sslcert = filepath.Join(certpath, "postgresql.crt")
	default:
		t.Fatalf("invalid source %q", source)
	}
	return fmt.Sprintf("sslmode=require user=pqgosslcert sslkey=%s sslcert=%s", sslkey, sslcert)
}

// Authenticate over SSL using client certificates
func TestSSLClientCertificates(t *testing.T) {
	if shouldSkipSSLTests(t) {
		t.Log("skipping SSL test")
		return
	}
	// Environment sanity check: should fail without SSL
	checkSSLSetup(t, "sslmode=disable user=pqgossltest")

	// Should also fail without a valid certificate
	db, err := openSSLConn(t, "sslmode=require user=pqgosslcert")
	if err == nil {
		db.Close()
		t.Fatal("expected error")
	}
	pge, ok := err.(*Error)
	if !ok {
		t.Fatal("expected pq.Error")
	}
	if pge.Code.Name() != "invalid_authorization_specification" {
		t.Fatalf("unexpected error code %q", pge.Code.Name())
	}

	// Should work
	db, err = openSSLConn(t, getCertConninfo(t, "valid"))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query("SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	rows.Close()
}

// Test errors with ssl certificates
func TestSSLClientCertificatesMissingFiles(t *testing.T) {
	if shouldSkipSSLTests(t) {
		t.Log("skipping SSL test")
		return
	}
	// Environment sanity check: should fail without SSL
	checkSSLSetup(t, "sslmode=disable user=pqgossltest")

	// Key missing, should fail
	_, err := openSSLConn(t, getCertConninfo(t, "missingkey"))
	if err == nil {
		t.Fatal("expected error")
	}
	// should be a PathError
	_, ok := err.(*os.PathError)
	if !ok {
		t.Fatalf("expected PathError, got %#+v", err)
	}

	// Cert missing, should fail
	_, err = openSSLConn(t, getCertConninfo(t, "missingcert"))
	if err == nil {
		t.Fatal("expected error")
	}
	// should be a PathError
	_, ok = err.(*os.PathError)
	if !ok {
		t.Fatalf("expected PathError, got %#+v", err)
	}

	// Key has wrong permissions, should fail
	_, err = openSSLConn(t, getCertConninfo(t, "certtwice"))
	if err == nil {
		t.Fatal("expected error")
	}
	if err != ErrSSLKeyHasWorldPermissions {
		t.Fatalf("expected ErrSSLKeyHasWorldPermissions, got %#+v", err)
	}
}
