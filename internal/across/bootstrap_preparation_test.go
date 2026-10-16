package across_test

import (
	"context"
	"encoding/json"
	"github.com/vphoenix/crypto-market-info/internal/across"
	"os"
	"path/filepath"
	"testing"

	"github.com/vphoenix/crypto-market-info/internal/storage/backend"
	"github.com/vphoenix/crypto-market-info/internal/storage/mysql"
)

// This opt-in check exercises the real startup restore without constructing any
// network reader, price client, collector loop, or writable store.
func TestPreparedMySQLStartupRecovery(t *testing.T) {
	root := os.Getenv("CMI_BOOTSTRAP_PREPARATION")
	if root == "" {
		t.Skip("explicit preparation directory required")
	}
	m, e := across.LoadManifest("../../config/across-research.json")
	if e != nil {
		t.Fatal(e)
	}
	s, e := backend.OpenAcross(context.Background(), backend.Config{Name: "mysql", MySQL: mysql.Config{Socket: "/home/ubuntu/crypto-market-info/var/storage/mysql/run/mysql.sock", Database: "crypto_market_info_across", Username: "cmi_query", PasswordFile: "/home/ubuntu/.config/crypto-market-info-storage/cmi_query-password", MaxConnections: 1, ReadOnly: true}})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	report, e := across.PreparedStartupRecovery(context.Background(), s, m)
	if e != nil {
		t.Fatal(e)
	}
	b, e := json.MarshalIndent(report, "", "  ")
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(root, "across-startup-recovery.json"), append(b, '\n'), 0600); e != nil {
		t.Fatal(e)
	}
}
