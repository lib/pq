package pq

import (
	"net/netip"
	"testing"
)

func TestConfigRegressionLeadingEmptyMultihostItemsUseDefaults(t *testing.T) {
	t.Run("host", func(t *testing.T) {
		cfg, err := newConfig("host=,secondary", nil)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Host != "localhost" || len(cfg.Multi) != 1 || cfg.Multi[0].Host != "secondary" {
			t.Fatalf("leading empty host did not select localhost: host=%q multi=%+v", cfg.Host, cfg.Multi)
		}
	})

	t.Run("hostaddr", func(t *testing.T) {
		cfg, err := newConfig("host=primary,secondary hostaddr=,192.0.2.1", nil)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Hostaddr.IsValid() || len(cfg.Multi) != 1 || cfg.Multi[0].Hostaddr != netip.MustParseAddr("192.0.2.1") {
			t.Fatalf("leading empty hostaddr did not select hostname lookup: hostaddr=%v multi=%+v", cfg.Hostaddr, cfg.Multi)
		}
	})

	t.Run("port", func(t *testing.T) {
		cfg, err := newConfig("host=primary,secondary port=,6543", nil)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Port != 5432 || len(cfg.Multi) != 1 || cfg.Multi[0].Port != 6543 {
			t.Fatalf("leading empty port did not select 5432: port=%d multi=%+v", cfg.Port, cfg.Multi)
		}
	})
}

func TestConfigRegressionLeadingEmptyHostMismatchDoesNotPanic(t *testing.T) {
	panicValue, err := regressionCallWithoutPanic(func() error {
		_, err := newConfig("host=,b,c hostaddr=192.0.2.1,192.0.2.2", nil)
		return err
	})
	if panicValue != nil {
		t.Fatalf("mismatched multihost lists caused a panic: %v", panicValue)
	}
	if err == nil {
		t.Fatal("mismatched multihost lists were accepted")
	}
}
