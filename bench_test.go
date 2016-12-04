// +build go1.1

package pq

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"database/sql"
	"database/sql/driver"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lib/pq/oid"
)

var (
	selectStringQuery = "SELECT '" + strings.Repeat("0123456789", 10) + "'"
	selectSeriesQuery = "SELECT generate_series(1, 100)"
)

func BenchmarkSelectString(b *testing.B) {
	var result string
	benchQuery(b, selectStringQuery, &result)
}

func BenchmarkSelectSeries(b *testing.B) {
	var result int
	benchQuery(b, selectSeriesQuery, &result)
}

func benchQuery(b *testing.B, query string, result interface{}) {
	b.StopTimer()
	db := openTestConn(b)
	defer db.Close()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		benchQueryLoop(b, db, query, result)
	}
}

func benchQueryLoop(b *testing.B, db *sql.DB, query string, result interface{}) {
	rows, err := db.Query(query)
	if err != nil {
		b.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(result)
		if err != nil {
			b.Fatal("failed to scan", err)
		}
	}
}

// reading from circularConn yields content[:prefixLen] once, followed by
// content[prefixLen:] over and over again. It never returns EOF.
type circularConn struct {
	content   string
	prefixLen int
	pos       int
	net.Conn  // for all other net.Conn methods that will never be called
}

func (r *circularConn) Read(b []byte) (n int, err error) {
	n = copy(b, r.content[r.pos:])
	r.pos += n
	if r.pos >= len(r.content) {
		r.pos = r.prefixLen
	}
	return
}

func (r *circularConn) Write(b []byte) (n int, err error) { return len(b), nil }

func (r *circularConn) Close() error { return nil }

func fakeConn(content string, prefixLen int) *conn {
	c := &circularConn{content: content, prefixLen: prefixLen}
	return &conn{buf: bufio.NewReader(c), c: c}
}

// This benchmark is meant to be the same as BenchmarkSelectString, but takes
// out some of the factors this package can't control. The numbers are less noisy,
// but also the costs of network communication aren't accurately represented.
func BenchmarkMockSelectString(b *testing.B) {
	b.StopTimer()
	// taken from a recorded run of BenchmarkSelectString
	// See: http://www.postgresql.org/docs/current/static/protocol-message-formats.html
	const response = "1\x00\x00\x00\x04" +
		"t\x00\x00\x00\x06\x00\x00" +
		"T\x00\x00\x00!\x00\x01?column?\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\xc1\xff\xfe\xff\xff\xff\xff\x00\x00" +
		"Z\x00\x00\x00\x05I" +
		"2\x00\x00\x00\x04" +
		"D\x00\x00\x00n\x00\x01\x00\x00\x00d0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
		"C\x00\x00\x00\rSELECT 1\x00" +
		"Z\x00\x00\x00\x05I" +
		"3\x00\x00\x00\x04" +
		"Z\x00\x00\x00\x05I"
	c := fakeConn(response, 0)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		benchMockQuery(b, c, selectStringQuery)
	}
}

var seriesRowData = func() string {
	var buf bytes.Buffer
	for i := 1; i <= 100; i++ {
		digits := byte(2)
		if i >= 100 {
			digits = 3
		} else if i < 10 {
			digits = 1
		}
		buf.WriteString("D\x00\x00\x00")
		buf.WriteByte(10 + digits)
		buf.WriteString("\x00\x01\x00\x00\x00")
		buf.WriteByte(digits)
		buf.WriteString(strconv.Itoa(i))
	}
	return buf.String()
}()

func BenchmarkMockSelectSeries(b *testing.B) {
	b.StopTimer()
	var response = "1\x00\x00\x00\x04" +
		"t\x00\x00\x00\x06\x00\x00" +
		"T\x00\x00\x00!\x00\x01?column?\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\xc1\xff\xfe\xff\xff\xff\xff\x00\x00" +
		"Z\x00\x00\x00\x05I" +
		"2\x00\x00\x00\x04" +
		seriesRowData +
		"C\x00\x00\x00\x0fSELECT 100\x00" +
		"Z\x00\x00\x00\x05I" +
		"3\x00\x00\x00\x04" +
		"Z\x00\x00\x00\x05I"
	c := fakeConn(response, 0)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		benchMockQuery(b, c, selectSeriesQuery)
	}
}

func benchMockQuery(b *testing.B, c *conn, query string) {
	stmt, err := c.Prepare(query)
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()
	rows, err := stmt.Query(nil)
	if err != nil {
		b.Fatal(err)
	}
	defer rows.Close()
	var dest [1]driver.Value
	for {
		if err := rows.Next(dest[:]); err != nil {
			if err == io.EOF {
				break
			}
			b.Fatal(err)
		}
	}
}

func BenchmarkPreparedSelectString(b *testing.B) {
	var result string
	benchPreparedQuery(b, selectStringQuery, &result)
}

func BenchmarkPreparedSelectSeries(b *testing.B) {
	var result int
	benchPreparedQuery(b, selectSeriesQuery, &result)
}

func benchPreparedQuery(b *testing.B, query string, result interface{}) {
	b.StopTimer()
	db := openTestConn(b)
	defer db.Close()
	stmt, err := db.Prepare(query)
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		benchPreparedQueryLoop(b, db, stmt, result)
	}
}

func benchPreparedQueryLoop(b *testing.B, db *sql.DB, stmt *sql.Stmt, result interface{}) {
	rows, err := stmt.Query()
	if err != nil {
		b.Fatal(err)
	}
	if !rows.Next() {
		rows.Close()
		b.Fatal("no rows")
	}
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&result)
		if err != nil {
			b.Fatal("failed to scan")
		}
	}
}

// See the comment for BenchmarkMockSelectString.
func BenchmarkMockPreparedSelectString(b *testing.B) {
	b.StopTimer()
	const parseResponse = "1\x00\x00\x00\x04" +
		"t\x00\x00\x00\x06\x00\x00" +
		"T\x00\x00\x00!\x00\x01?column?\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\xc1\xff\xfe\xff\xff\xff\xff\x00\x00" +
		"Z\x00\x00\x00\x05I"
	const responses = parseResponse +
		"2\x00\x00\x00\x04" +
		"D\x00\x00\x00n\x00\x01\x00\x00\x00d0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
		"C\x00\x00\x00\rSELECT 1\x00" +
		"Z\x00\x00\x00\x05I"
	c := fakeConn(responses, len(parseResponse))

	stmt, err := c.Prepare(selectStringQuery)
	if err != nil {
		b.Fatal(err)
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		benchPreparedMockQuery(b, c, stmt)
	}
}

func BenchmarkMockPreparedSelectSeries(b *testing.B) {
	b.StopTimer()
	const parseResponse = "1\x00\x00\x00\x04" +
		"t\x00\x00\x00\x06\x00\x00" +
		"T\x00\x00\x00!\x00\x01?column?\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\xc1\xff\xfe\xff\xff\xff\xff\x00\x00" +
		"Z\x00\x00\x00\x05I"
	var responses = parseResponse +
		"2\x00\x00\x00\x04" +
		seriesRowData +
		"C\x00\x00\x00\x0fSELECT 100\x00" +
		"Z\x00\x00\x00\x05I"
	c := fakeConn(responses, len(parseResponse))

	stmt, err := c.Prepare(selectSeriesQuery)
	if err != nil {
		b.Fatal(err)
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		benchPreparedMockQuery(b, c, stmt)
	}
}

func benchPreparedMockQuery(b *testing.B, c *conn, stmt driver.Stmt) {
	rows, err := stmt.Query(nil)
	if err != nil {
		b.Fatal(err)
	}
	defer rows.Close()
	var dest [1]driver.Value
	for {
		if err := rows.Next(dest[:]); err != nil {
			if err == io.EOF {
				break
			}
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeInt64(b *testing.B) {
	for i := 0; i < b.N; i++ {
		encode(&parameterStatus{}, int64(1234), oid.T_int8)
	}
}

func BenchmarkEncodeFloat64(b *testing.B) {
	for i := 0; i < b.N; i++ {
		encode(&parameterStatus{}, 3.14159, oid.T_float8)
	}
}

var testByteString = []byte("abcdefghijklmnopqrstuvwxyz")

func BenchmarkEncodeByteaHex(b *testing.B) {
	for i := 0; i < b.N; i++ {
		encode(&parameterStatus{serverVersion: 90000}, testByteString, oid.T_bytea)
	}
}
func BenchmarkEncodeByteaEscape(b *testing.B) {
	for i := 0; i < b.N; i++ {
		encode(&parameterStatus{serverVersion: 84000}, testByteString, oid.T_bytea)
	}
}

func BenchmarkEncodeBool(b *testing.B) {
	for i := 0; i < b.N; i++ {
		encode(&parameterStatus{}, true, oid.T_bool)
	}
}

var testTimestamptz = time.Date(2001, time.January, 1, 0, 0, 0, 0, time.Local)

func BenchmarkEncodeTimestamptz(b *testing.B) {
	for i := 0; i < b.N; i++ {
		encode(&parameterStatus{}, testTimestamptz, oid.T_timestamptz)
	}
}

var testIntBytes = []byte("1234")

func BenchmarkDecodeInt64(b *testing.B) {
	for i := 0; i < b.N; i++ {
		decode(&parameterStatus{}, testIntBytes, oid.T_int8, formatText)
	}
}

var testFloatBytes = []byte("3.14159")

func BenchmarkDecodeFloat64(b *testing.B) {
	for i := 0; i < b.N; i++ {
		decode(&parameterStatus{}, testFloatBytes, oid.T_float8, formatText)
	}
}

var testBoolBytes = []byte{'t'}

func BenchmarkDecodeBool(b *testing.B) {
	for i := 0; i < b.N; i++ {
		decode(&parameterStatus{}, testBoolBytes, oid.T_bool, formatText)
	}
}

func TestDecodeBool(t *testing.T) {
	db := openTestConn(t)
	rows, err := db.Query("select true")
	if err != nil {
		t.Fatal(err)
	}
	rows.Close()
}

var testTimestamptzBytes = []byte("2013-09-17 22:15:32.360754-07")

func BenchmarkDecodeTimestamptz(b *testing.B) {
	for i := 0; i < b.N; i++ {
		decode(&parameterStatus{}, testTimestamptzBytes, oid.T_timestamptz, formatText)
	}
}

func BenchmarkDecodeTimestamptzMultiThread(b *testing.B) {
	oldProcs := runtime.GOMAXPROCS(0)
	defer runtime.GOMAXPROCS(oldProcs)
	runtime.GOMAXPROCS(runtime.NumCPU())
	globalLocationCache = newLocationCache()

	f := func(wg *sync.WaitGroup, loops int) {
		defer wg.Done()
		for i := 0; i < loops; i++ {
			decode(&parameterStatus{}, testTimestamptzBytes, oid.T_timestamptz, formatText)
		}
	}

	wg := &sync.WaitGroup{}
	b.ResetTimer()
	for j := 0; j < 10; j++ {
		wg.Add(1)
		go f(wg, b.N/10)
	}
	wg.Wait()
}

func BenchmarkLocationCache(b *testing.B) {
	globalLocationCache = newLocationCache()
	for i := 0; i < b.N; i++ {
		globalLocationCache.getLocation(rand.Intn(10000))
	}
}

func BenchmarkLocationCacheMultiThread(b *testing.B) {
	oldProcs := runtime.GOMAXPROCS(0)
	defer runtime.GOMAXPROCS(oldProcs)
	runtime.GOMAXPROCS(runtime.NumCPU())
	globalLocationCache = newLocationCache()

	f := func(wg *sync.WaitGroup, loops int) {
		defer wg.Done()
		for i := 0; i < loops; i++ {
			globalLocationCache.getLocation(rand.Intn(10000))
		}
	}

	wg := &sync.WaitGroup{}
	b.ResetTimer()
	for j := 0; j < 10; j++ {
		wg.Add(1)
		go f(wg, b.N/10)
	}
	wg.Wait()
}

// Stress test the performance of parsing results from the wire.
func BenchmarkResultParsing(b *testing.B) {
	b.StopTimer()

	db := openTestConn(b)
	defer db.Close()
	_, err := db.Exec("BEGIN")
	if err != nil {
		b.Fatal(err)
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		res, err := db.Query("SELECT generate_series(1, 50000)")
		if err != nil {
			b.Fatal(err)
		}
		res.Close()
	}
}

func BenchmarkSetupSSLClientCertificatesParse(b *testing.B) {
	b.StopTimer()
	keypath := tempfile(b, dummyKey)
	certpath := tempfile(b, dummyCert)
	defer os.Remove(keypath)
	defer os.Remove(certpath)
	defer b.StopTimer()
	var tlsConfig tls.Config
	o := values{
		"sslkey":  keypath,
		"sslcert": certpath,
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		setupSSLClientCertificates(&tlsConfig, o)
	}
}

func BenchmarkSetupSSLClientCertificatesNoFile(b *testing.B) {
	var tlsConfig tls.Config
	o := make(values)
	for i := 0; i < b.N; i++ {
		setupSSLClientCertificates(&tlsConfig, o)
	}
}

func tempfile(b *testing.B, body string) (path string) {
	f, err := ioutil.TempFile("", "pqtest")
	if err != nil {
		b.Fatal(err)
	}
	_, err = io.WriteString(f, body)
	if err != nil {
		b.Fatal(err)
	}
	err = f.Close()
	if err != nil {
		b.Fatal(err)
	}
	return f.Name()
}

const dummyCert = `
-----BEGIN CERTIFICATE-----
MIIC+zCCAeOgAwIBAgIQf4S2Cm7JGlEJ2xZdnLXJxTANBgkqhkiG9w0BAQsFADAS
MRAwDgYDVQQKEwdBY21lIENvMB4XDTE1MTAwNDIxNDI0OFoXDTE2MTAwMzIxNDI0
OFowEjEQMA4GA1UEChMHQWNtZSBDbzCCASIwDQYJKoZIhvcNAQEBBQADggEPADCC
AQoCggEBAMx4GBx3vZjlpCu5fBra3keCq+ZNiU4WX3QXOUygWaBaB5KRZhFrcfY+
mzMbptSyWYjA6+PnPyODDxn2vxPqLVommuspR4QkqIk+HuSVL3eAtH6jv/u91H+G
COSrvqf4YCD99A81D0LryhL+x2GCyOaVsnHtwAbWsImZ/rhSkS3+DyLf/L96+GlF
UsXNs7Vp+NeAjUEscZ0b7jkcrhpKMD/WBkmYR45Yxuq+KEZ7ilFHKu+a2b0xRHHC
fCodqbSlZ6vvp2vluAJLvDF/Nk3jGSq10efHw8dz6iYMlxjRagQLgpViyKR6K8Az
XJUmluFYGQQQsqET27ecFl1TngBGqd8CAwEAAaNNMEswDgYDVR0PAQH/BAQDAgWg
MBMGA1UdJQQMMAoGCCsGAQUFBwMBMAwGA1UdEwEB/wQCMAAwFgYDVR0RBA8wDYIL
ZXhhbXBsZS5vcmcwDQYJKoZIhvcNAQELBQADggEBAFQDL+Xx/EPdZgWvtTe/I4Ch
ZwT78N7jZ39G4AEMusdf9WUY3RqimbWiLD8rub43uBr/uO1MYvKJTKp8dS7TVCYa
YwxzS2I6bj4cOIXUVeXN/TiViWZwqshhVUCPvS4g4hXOMyIi0fqOjgaAvNMwxeDh
fqG4nOD2Gpwax/1KrH3GawUFghFvC5T3CHC8Q/T7LXFeQ//jTtUMmJ+9PQzTTr/p
Nx0ffnMXXiu7fdo42KTJoOA21bvZWQ5okA1Y6catVN8CNRNKIGIN/9w36ffDw8Un
5Va3a5X5CnQvU2CK5o1E2IUEpbDwrmSKzn8mmpqBRurbFCD5q1fW0BafA+tmAwg=
-----END CERTIFICATE-----
`

const dummyKey = `
-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEAzHgYHHe9mOWkK7l8GtreR4Kr5k2JThZfdBc5TKBZoFoHkpFm
EWtx9j6bMxum1LJZiMDr4+c/I4MPGfa/E+otWiaa6ylHhCSoiT4e5JUvd4C0fqO/
+73Uf4YI5Ku+p/hgIP30DzUPQuvKEv7HYYLI5pWyce3ABtawiZn+uFKRLf4PIt/8
v3r4aUVSxc2ztWn414CNQSxxnRvuORyuGkowP9YGSZhHjljG6r4oRnuKUUcq75rZ
vTFEccJ8Kh2ptKVnq++na+W4Aku8MX82TeMZKrXR58fDx3PqJgyXGNFqBAuClWLI
pHorwDNclSaW4VgZBBCyoRPbt5wWXVOeAEap3wIDAQABAoIBAQC5VOWt8A8Hpqb/
BvppsRcnRFchwggBop/UrzQ9s15pzRDuFiKpCXXbmHW+hoLaaepj3VIzWijNvH6U
ryYVG/8Nps5m9xyet0eYVplT4bGLpTp1S2G6Ah+5kzk+ZDnFMImZffaZTiPOKcEZ
JJx+UzhcYTXEtJaI3FJZ9x593kE/qFk2Ylp+cSHsgvglxjZvD3hcBjIt37ovLGck
zo826EjmS7xtUOuBWpSyrSjeQ2qHSL95hoiCUC2XKCTunCfy8kXhuhWzHzCwljPI
ULgo0vVzzA9K8wsMVVXl2qEqvlSZGXFQtWV1gIwXbKSlqn0BDs4Y7QyBrlAop9Vf
LggeqlYxAoGBAONcI4KoycT+mcg1Z4+cvX8vO8NDlkSW1RGsDyPNmIEAC6yZhj7+
TaMrxeprFduZf6H8dFUhri56X57h7CuW7xHD2ryX524df8Mlhkcrf0F2QCB4pflx
GvxgNR0XsxypZfbGdvgDTHXVz0FOD+SOpZORaZxw+9qrJ/omwHtqkQb3AoGBAOY5
xvMbHHAiqqjGbAJ8LEb/Fztf24z7N0ouPoA7AFFAS9T1M+PYGmL3Rkfml5i4DLwA
+tGs/jA8ZEyQbfhsw/9hJmLojfyq6VG7Dvm0DEER8LIfpBXRP+tkpFSunUBWYC++
J2k0igbz0kwLDlvo81T2Wg9ugWG/E1+qiMcouTJZAoGBAIXR/5yyGEB40q8Cr/fZ
e7fWZ0ihCVtJpBOIwEiEhJS5ICXxHxEIwU2fQBif+veMO5FudFJ/RnRY1ts/grCN
YB2Gt8J1bmRjvIVyGrzdH0O6hDgYiyhsqEOPpPOAtY3TLw629eM4ndJljF2Vwsj2
JQLcfdr0rWihgSA9muGJcd81AoGAM/k5I6qsKdh5pG5e9dSofkKaMQo720DfQ3zb
GUG4mZ8lP2c3lqkzk8H0+Mhi0tRB87NY7Drci3Emx24XlWygdqes7clIPJEs6QmM
oOx3k70EFII2HcLGZlKrEn70+xBE2KJZ7VMyEc27XPVmAXO+cyDGRhORW8qyCffK
twNHg8kCgYB5SYu5nkPisCKnNCfDVHci5IrwqwoC6ch0/kDx9oSpyUTSv2iiWqL4
nKqj7r41SowYPGlYYxKZ9eAKnJBCovZ2wUbHDmKrmmDIigjv5Y3wlRzFAH/0Satf
S0hoa0mHG3ZKxIH08lrCAr/IvMck2X0F6JC0JoQCzBKTN4WOJrC6yQ==
-----END RSA PRIVATE KEY-----
`
