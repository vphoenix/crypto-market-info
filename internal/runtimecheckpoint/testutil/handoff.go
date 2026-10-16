package testutil

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
	"os"
	"path/filepath"
	"testing"
)

func Handoff(t testing.TB, b runtimecheckpoint.Binding, payloads ...[]byte) (string, []string) {
	t.Helper()
	dir := t.TempDir()
	paths := []string{}
	in := runtimecheckpoint.HandoffInput[runtimecheckpoint.HandoffPayload]{Version: 1, Binding: b, Reason: "isolated finite import regression"}
	for i, raw := range payloads {
		path := filepath.Join(dir, fmt.Sprintf("%d.operation", i))
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		in.Operations = append(in.Operations, runtimecheckpoint.HandoffPayload{File: path, SHA256: hex.EncodeToString(sum[:])})
		paths = append(paths, path)
	}
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "plan.json")
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return path, paths
}
