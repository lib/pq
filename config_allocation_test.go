package pq

import (
	"net/netip"
	"testing"
)

func TestConnectorConfigAllocationOwnership(t *testing.T) {
	t.Run("empty runtime map", func(t *testing.T) {
		runtimeParameters := map[string]string{}
		connector, err := NewConnectorConfig(Config{
			Host: "localhost", Port: 5432, SSLMode: SSLModeDisable,
			Runtime: runtimeParameters,
		})
		if err != nil {
			t.Fatal(err)
		}
		runtimeParameters["application_name"] = "mutated"
		if len(connector.cfg.Runtime) != 0 {
			t.Fatalf("connector retained caller's empty Runtime map: %v", connector.cfg.Runtime)
		}
	})

	t.Run("mutable fields", func(t *testing.T) {
		runtimeParameters := map[string]string{"application_name": "original"}
		hosts := []ConfigMultihost{{
			Host: "replica", Port: 5433, Hostaddr: netip.MustParseAddr("192.0.2.2"),
		}}
		auth := RequireAuths{RequireAuthMD5}
		connector, err := NewConnectorConfig(Config{
			Host: "primary", Port: 5432, SSLMode: SSLModeDisable,
			Runtime: runtimeParameters, Multi: hosts, RequireAuth: auth,
		})
		if err != nil {
			t.Fatal(err)
		}

		runtimeParameters["application_name"] = "mutated"
		hosts[0].Host = "mutated"
		auth[0] = RequireAuthPassword
		if got := connector.cfg.Runtime["application_name"]; got != "original" {
			t.Errorf("Runtime alias: got %q, want original", got)
		}
		if got := connector.cfg.Multi[0].Host; got != "replica" {
			t.Errorf("Multi alias: got %q, want replica", got)
		}
		if got := connector.cfg.RequireAuth[0]; got != RequireAuthMD5 {
			t.Errorf("RequireAuth alias: got %q, want %q", got, RequireAuthMD5)
		}
	})
}
