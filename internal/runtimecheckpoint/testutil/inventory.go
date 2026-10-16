// Package testutil supplies explicit frozen conversion provenance for isolated
// domain fixtures. It is not used by production binaries or database tooling.
package testutil

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

func Inventory(t testing.TB, b runtimecheckpoint.Binding, service string) string {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"version": 1, "service": service, "database": b.Database, "server_uuid": b.ServerUUID, "collected_at": "2026-01-01T00:00:00Z", "plans": []any{}, "coverage": []any{}, "unreadable": []any{}, "unlisted_source_ranges": "unknown", "runtime_dependency_proof": "requires_frozen_scope_comparison", "full_history_certified": false, "evidence": []any{map[string]string{"purpose": "isolated frozen typed fixture; no source history completeness claim"}}})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "input-inventory.json")
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
