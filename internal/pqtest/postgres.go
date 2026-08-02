package pqtest

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

const (
	useTestcontainersEnv = "PQTEST_USE_TESTCONTAINERS"
	postgresImageEnv     = "PQTEST_POSTGRES_IMAGE"
	defaultPostgresImage = "postgres:18"
)

var postgresTestServer struct {
	sync.Mutex
	done      bool
	managed   bool
	err       error
	container *postgres.PostgresContainer
	hostaddr  string
}

// Setup configures the PostgreSQL server used by the test suite. By default it
// starts a PostgreSQL container when no external server is configured through
// PGHOST, PGHOSTADDR, or PGPORT.
//
// Set PQTEST_USE_TESTCONTAINERS to explicitly enable or disable the container,
// and PQTEST_POSTGRES_IMAGE to select a different image.
func Setup() error {
	postgresTestServer.Lock()
	defer postgresTestServer.Unlock()

	if postgresTestServer.done {
		return postgresTestServer.err
	}
	postgresTestServer.done = true

	useContainer, err := useTestcontainers()
	if err != nil {
		postgresTestServer.err = err
		return err
	}
	if !useContainer || fuzzing() {
		configureExternalPostgres()
		return nil
	}

	root, err := repositoryRoot()
	if err != nil {
		postgresTestServer.err = err
		return err
	}

	image := os.Getenv(postgresImageEnv)
	if image == "" {
		image = defaultPostgresImage
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	postgresDir := filepath.Join(root, "testdata", "postgres", "docker-entrypoint-initdb.d")
	sslDir := filepath.Join(root, "testdata", "ssl")
	container, err := postgres.Run(ctx, image,
		postgres.WithDatabase("pqgo"),
		postgres.WithUsername("pqgo"),
		postgres.WithPassword("unused"),
		postgres.WithOrderedInitScripts(
			filepath.Join(postgresDir, "10-hba.sh"),
			filepath.Join(postgresDir, "20-config.sql"),
		),
		postgres.WithSSLCert(
			filepath.Join(sslDir, "root.crt"),
			filepath.Join(sslDir, "server.crt"),
			filepath.Join(sslDir, "server.key"),
		),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		_ = testcontainers.TerminateContainer(container)
		postgresTestServer.err = fmt.Errorf("start PostgreSQL test container %q: %w", image, err)
		return postgresTestServer.err
	}

	host, err := container.Host(ctx)
	if err != nil {
		_ = testcontainers.TerminateContainer(container)
		postgresTestServer.err = fmt.Errorf("get PostgreSQL test container host: %w", err)
		return postgresTestServer.err
	}
	port, err := container.MappedPort(ctx, "5432/tcp")
	if err != nil {
		_ = testcontainers.TerminateContainer(container)
		postgresTestServer.err = fmt.Errorf("get PostgreSQL test container port: %w", err)
		return postgresTestServer.err
	}
	hostaddr, err := resolveHostAddress(host)
	if err != nil {
		_ = testcontainers.TerminateContainer(container)
		postgresTestServer.err = err
		return err
	}

	postgresTestServer.container = container
	postgresTestServer.managed = true
	postgresTestServer.hostaddr = hostaddr
	clearManagedPostgresEnvironment()
	os.Setenv("PGHOST", host)
	os.Setenv("PGPORT", port.Port())
	os.Setenv("PGDATABASE", "pqgo")
	os.Setenv("PGUSER", "pqgo")
	os.Setenv("PGCONNECT_TIMEOUT", "20")
	os.Setenv("PGAPPNAME", "pqgo")
	return nil
}

// clearManagedPostgresEnvironment keeps a developer's libpq environment from
// changing the isolated Testcontainers configuration. Tests that exercise an
// environment option set it explicitly after TestMain has called Setup.
func clearManagedPostgresEnvironment() {
	for _, name := range []string{
		"PGAPPNAME", "PGCHANNELBINDING", "PGCLIENTENCODING", "PGCONNECT_TIMEOUT",
		"PGDATABASE", "PGDATESTYLE", "PGGEQO", "PGGSSDELEGATION", "PGGSSENCMODE",
		"PGGSSLIB", "PGHOST", "PGHOSTADDR", "PGKRBSRVNAME", "PGLOADBALANCEHOSTS",
		"PGMAXPROTOCOLVERSION", "PGMINPROTOCOLVERSION", "PGOPTIONS", "PGPASSFILE",
		"PGPASSWORD", "PGPORT", "PGREALM", "PGREQUIREAUTH", "PGREQUIREPEER",
		"PGREQUIRESSL", "PGSERVICE", "PGSERVICEFILE", "PGSSLCERT", "PGSSLCERTMODE",
		"PGSSLCOMPRESSION", "PGSSLCRL", "PGSSLCRLDIR", "PGSSLKEY",
		"PGSSLMAXPROTOCOLVERSION", "PGSSLMINPROTOCOLVERSION", "PGSSLMODE",
		"PGSSLNEGOTIATION", "PGSSLROOTCERT", "PGSSLSNI", "PGTARGETSESSIONATTRS",
		"PGTZ", "PGUSER",
	} {
		os.Unsetenv(name)
	}
}

// Teardown terminates a PostgreSQL container created by Setup. It is a no-op
// when the tests use an externally configured server.
func Teardown() error {
	postgresTestServer.Lock()
	defer postgresTestServer.Unlock()

	if !postgresTestServer.managed {
		return nil
	}
	postgresTestServer.managed = false
	container := postgresTestServer.container
	postgresTestServer.container = nil
	postgresTestServer.hostaddr = ""
	if err := testcontainers.TerminateContainer(container); err != nil {
		return fmt.Errorf("terminate PostgreSQL test container: %w", err)
	}
	return nil
}

// Main supplies the standard TestMain lifecycle for packages whose tests need
// PostgreSQL.
func Main(m *testing.M) int {
	if err := Setup(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	code := m.Run()
	if err := Teardown(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		if code == 0 {
			code = 1
		}
	}
	return code
}

func useTestcontainers() (bool, error) {
	if value, ok := os.LookupEnv(useTestcontainersEnv); ok {
		use, err := strconv.ParseBool(value)
		if err != nil {
			return false, fmt.Errorf("parse %s: %w", useTestcontainersEnv, err)
		}
		return use, nil
	}
	if os.Getenv(postgresImageEnv) != "" {
		return true, nil
	}
	for _, name := range []string{"PGHOST", "PGHOSTADDR", "PGPORT"} {
		if os.Getenv(name) != "" {
			return false, nil
		}
	}
	return true, nil
}

func configureExternalPostgres() {
	defaultTo := func(name, value string) {
		if _, ok := os.LookupEnv(name); !ok {
			os.Setenv(name, value)
		}
	}
	defaultTo("PGHOST", "localhost")
	defaultTo("PGDATABASE", "pqgo")
	defaultTo("PGUSER", "pqgo")
	defaultTo("PGCONNECT_TIMEOUT", "20")
	os.Setenv("PGAPPNAME", "pqgo")
}

func repositoryRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("get working directory: %w", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			if _, err := os.Stat(filepath.Join(dir, "testdata", "postgres")); err == nil {
				return dir, nil
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("find repository root from %q", dir)
		}
		dir = parent
	}
}

func resolveHostAddress(host string) (string, error) {
	if ip := net.ParseIP(host); ip != nil {
		return ip.String(), nil
	}
	ips, err := net.LookupIP(host)
	if err != nil {
		return "", fmt.Errorf("resolve PostgreSQL test container host %q: %w", host, err)
	}
	for _, ip := range ips {
		if ip4 := ip.To4(); ip4 != nil {
			return ip4.String(), nil
		}
	}
	if len(ips) > 0 {
		return ips[0].String(), nil
	}
	return "", fmt.Errorf("resolve PostgreSQL test container host %q: no addresses", host)
}

func fuzzing() bool {
	for _, arg := range os.Args {
		if strings.HasPrefix(arg, "-test.fuzz") {
			return true
		}
	}
	return false
}

func addContainerHostaddr(conninfo string) string {
	postgresTestServer.Lock()
	hostaddr := postgresTestServer.hostaddr
	managed := postgresTestServer.managed
	postgresTestServer.Unlock()
	if !managed || hostaddr == "" || strings.HasPrefix(conninfo, "postgres://") || strings.HasPrefix(conninfo, "postgresql://") {
		return conninfo
	}

	var hasHost, hasHostaddr, hasPort bool
	for _, field := range strings.Fields(conninfo) {
		key, _, ok := strings.Cut(field, "=")
		if !ok {
			continue
		}
		switch key {
		case "host":
			hasHost = true
		case "hostaddr":
			hasHostaddr = true
		case "port":
			hasPort = true
		}
	}
	if !hasHost || hasHostaddr || hasPort {
		return conninfo
	}
	return conninfo + " hostaddr=" + hostaddr
}
