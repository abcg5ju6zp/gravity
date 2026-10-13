package discovery

import (
	"encoding/json"
	"testing"

	"beryju.io/gravity/pkg/tests"
)

func TestDecodeRoleConfigRetentionDefault(t *testing.T) {
	tests.Setup(t)
	r := New(newDiscoveryTestInstance())

	// Legacy config without the retention field defaults to 30 days.
	cfg := r.decodeRoleConfig([]byte(`{"enabled":true}`))
	if cfg.ApplyRetentionSeconds != DefaultApplyRetentionSeconds {
		t.Fatalf("expected default retention %d, got %d", DefaultApplyRetentionSeconds, cfg.ApplyRetentionSeconds)
	}

	// Empty payload keeps the default too.
	cfg = r.decodeRoleConfig(nil)
	if cfg.ApplyRetentionSeconds != DefaultApplyRetentionSeconds {
		t.Fatalf("expected default retention for empty payload, got %d", cfg.ApplyRetentionSeconds)
	}

	// Explicit override is honored.
	raw, err := json.Marshal(RoleConfig{Enabled: true, ApplyRetentionSeconds: 120})
	if err != nil {
		t.Fatal(err)
	}
	cfg = r.decodeRoleConfig(raw)
	if cfg.ApplyRetentionSeconds != 120 {
		t.Fatalf("expected overridden retention 120, got %d", cfg.ApplyRetentionSeconds)
	}
}
