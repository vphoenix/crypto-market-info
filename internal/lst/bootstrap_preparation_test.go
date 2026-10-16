package lst_test

import (
	"context"
	"encoding/json"
	"github.com/vphoenix/crypto-market-info/internal/lst"
	"os"
	"path/filepath"
	"testing"

	"github.com/vphoenix/crypto-market-info/internal/storage/backend"
	"github.com/vphoenix/crypto-market-info/internal/storage/mysql"
)

func TestPreparedMySQLStartupRecovery(t *testing.T) {
	root := os.Getenv("CMI_BOOTSTRAP_PREPARATION")
	if root == "" {
		t.Skip("explicit preparation directory required")
	}
	m, e := lst.LoadManifest("../../config/lst-lido-ethereum.json")
	if e != nil {
		t.Fatal(e)
	}
	s, e := backend.OpenLST(context.Background(), backend.Config{Name: "mysql", MySQL: mysql.Config{Socket: "/home/ubuntu/crypto-market-info/var/storage/mysql/run/mysql.sock", Database: "crypto_market_info_lst", Username: "cmi_query", PasswordFile: "/home/ubuntu/.config/crypto-market-info-storage/cmi_query-password", MaxConnections: 1, ReadOnly: true}})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	state := filepath.Join(root, "rollback", "lst-state")
	if _, e = os.Stat(filepath.Join(state, "live-logs-from-26113044.gob")); e != nil {
		t.Fatal(e)
	}
	report, e := lst.PreparedStartupRecovery(context.Background(), s, m, state)
	if e != nil {
		t.Fatal(e)
	}
	b, e := json.MarshalIndent(report, "", "  ")
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(root, "lst-startup-recovery.json"), append(b, '\n'), 0600); e != nil {
		t.Fatal(e)
	}
}
