package pq

import "testing"

// Create the users if required. The *md5* users have MD5 verifiers in
// pg_authid, and *scram* users have SCRAM-SHA-256. pg_hba.conf is configured
// (in .travis.sh) to authenticate *1 users with md5 and *2 with scram.
func checkSCRAMSetup(t *testing.T) bool {
	db := openTestConn(t)
	defer db.Close()
	if getServerVersion(t, db) < 100000 {
		// skip the test if the postgres version is less than 10
		return false
	}
	_, err := db.Exec(`
		DROP USER IF EXISTS pqgomd5u1, pqgomd5u2, pqgoscramu1, pqgoscramu2;
		SET password_encryption='md5';
		CREATE USER pqgomd5u1 PASSWORD 'se%r-*tpΣβ';
		CREATE USER pqgomd5u2 PASSWORD 'se%r-*tpΣβ';
		SET password_encryption='scram-sha-256';
		CREATE USER pqgoscramu1 PASSWORD 'se%r-*tpΣβ';
		CREATE USER pqgoscramu2 PASSWORD 'se%r-*tpΣβ';
`)
	if err != nil {
		t.Fatal(err)
	}
	return true
}

func testSCRAMSuccessCase(t *testing.T, conninfo string) {
	if !checkSCRAMSetup(t) {
		return
	}
	db, err := openTestConnConninfo(conninfo)
	if err != nil {
		t.Fatal(err)
	}
	var dummy int
	if err := db.QueryRow("SELECT 1").Scan(&dummy); err != nil {
		t.Fatal(err)
	}
	db.Close()
}

// user has SCRAM verifier, pg_hba picks scram
func TestSCRAMBothScram(t *testing.T) {
	testSCRAMSuccessCase(t, `user=pqgoscramu2 password=se%r-*tpΣβ`)
}

// user has SCRAM verifier, pg_hba picks md5 (should pass)
func TestSCRAMUserScramServerMD5(t *testing.T) {
	testSCRAMSuccessCase(t, `user=pqgoscramu1 password=se%r-*tpΣβ`)
}

// user has MD5 verifier, pg_hba picks scram (should fail because
// "user does not have a valid SCRAM identifier")
func TestSCRAMUserMD5ServerScram(t *testing.T) {
	if !checkSCRAMSetup(t) {
		return
	}
	db, err := openTestConnConninfo(`user=pqgomd5u2 password=se%r-*tpΣβ`)
	if err != nil {
		t.Fatal(err)
	}
	var dummy int
	if err := db.QueryRow("SELECT 1").Scan(&dummy); err == nil {
		t.Fatalf("auth should have failed")
	}
	db.Close()
}

// user has MD5 verifier, pg_hba picks md5 (should succeed, but no scram
// involved)
func TestSCRAMNoScram(t *testing.T) {
	testSCRAMSuccessCase(t, `user=pqgomd5u1 password=se%r-*tpΣβ`)
}

// user has SCRAM verifier, pg_hba picks scram, but wrong password
func TestSCRAMWrongPass(t *testing.T) {
	if !checkSCRAMSetup(t) {
		return
	}
	db, err := openTestConnConninfo(`user=pqgoscramu2 password=incorrect`)
	if err != nil {
		t.Fatal(err)
	}
	var dummy int
	if err := db.QueryRow("SELECT 1").Scan(&dummy); err == nil {
		t.Fatalf("auth should have failed")
	}
	db.Close()
}

// this password fails the RFC 4013 profile, but postgres (and therefore lib/pq)
// allows it.
func TestSCRAMStrangePasswords(t *testing.T) {
	db := openTestConn(t)
	if getServerVersion(t, db) < 100000 {
		// skip the test if the postgres version is less than 10
		db.Close()
		return
	}
	_, err := db.Exec(`
		DROP USER IF EXISTS pqgoscramu2;
		SET password_encryption='scram-sha-256';
		CREATE USER pqgoscramu2 PASSWORD 'a😄b';
`)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	db2, err := openTestConnConninfo(`user=pqgoscramu2 password=a😄b`)
	if err != nil {
		t.Fatal(err)
	}
	var dummy int
	if err := db2.QueryRow("SELECT 1").Scan(&dummy); err != nil {
		t.Fatal(err)
	}
	db2.Close()
}
