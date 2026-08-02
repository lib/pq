package hstore

import (
	"os"
	"testing"

	"github.com/lib/pq/internal/pqtest"
)

func TestMain(m *testing.M) {
	os.Exit(pqtest.Main(m))
}
