package runtimecheckpoint

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func inventoryFixture() map[string]any {
	return map[string]any{"version": 1, "service": "across", "database": "test_across", "server_uuid": "server", "collected_at": "2026-01-01T00:00:00Z", "plans": []any{map[string]any{"kind": "plan", "table": "across_capture", "state": 5, "classification": "verified_rows_only"}}, "coverage": []any{}, "unreadable": []any{map[string]any{"kind": "unreadable", "table": "across_capture", "key_hash": "aa", "key_hex": "bb", "reason_hex": "cc"}}, "unlisted_source_ranges": "unknown", "runtime_dependency_proof": "requires_frozen_scope_comparison", "full_history_certified": false}
}
func TestInputInventoryRetainsUnknownAndRejectsChangedResume(t *testing.T) {
	doc := inventoryFixture()
	path := filepath.Join(t.TempDir(), "inventory.json")
	raw, _ := json.Marshal(doc)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	input, err := ReadConversionInput(path, testBinding(), "across")
	if err != nil {
		t.Fatal(err)
	}
	if input.Inventory.PendingOrUnknown != 1 || input.Inventory.Unreadable != 1 || input.Inventory.FullHistoryCertified {
		t.Fatal(input.Inventory)
	}
	files, dir := createTest(t)
	evidence, err := input.Persist(files, InputInventory{})
	if err != nil {
		t.Fatal(err)
	}
	if err = files.Checkpoint([]byte("state with provenance"), evidence.References()); err != nil {
		t.Fatal(err)
	}
	files = reopenTest(t, files, dir)
	if err = evidence.Verify(files); err != nil {
		t.Fatal(err)
	}
	doc["collected_at"] = "2026-01-02T00:00:00Z"
	raw, _ = json.Marshal(doc)
	os.WriteFile(path, raw, 0600)
	changed, err := ReadConversionInput(path, testBinding(), "across")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = changed.Persist(files, evidence); err == nil {
		t.Fatal("conversion input changed on resume")
	}
}
func TestInputInventoryIdentityAndCompletenessFailClosed(t *testing.T) {
	for _, change := range []func(map[string]any){func(v map[string]any) { v["database"] = "other" }, func(v map[string]any) { v["server_uuid"] = "other" }, func(v map[string]any) { v["full_history_certified"] = true }, func(v map[string]any) { v["unlisted_source_ranges"] = "empty" }, func(v map[string]any) { v["plans"].([]any)[0].(map[string]any)["classification"] = "verified_table" }, func(v map[string]any) { delete(v, "full_history_certified") }} {
		doc := inventoryFixture()
		change(doc)
		raw, _ := json.Marshal(doc)
		path := filepath.Join(t.TempDir(), "inventory.json")
		os.WriteFile(path, raw, 0600)
		if _, err := ReadConversionInput(path, testBinding(), "across"); err == nil {
			t.Fatal("unproven completeness accepted")
		}
	}
}

func TestInputInventoryVerificationChecksFrozenSummary(t *testing.T) {
	files, _ := createTest(t)
	raw, _ := json.Marshal(inventoryFixture())
	input, err := parseConversionInput(raw, testBinding(), "across")
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := input.Persist(files, InputInventory{})
	if err != nil {
		t.Fatal(err)
	}
	evidence.Unreadable = 0
	if err = evidence.Verify(files); err == nil {
		t.Fatal("lost unreadable range accepted")
	}
}
