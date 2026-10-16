package mysql

import (
	"context"
	"github.com/google/uuid"
	"os"
	"path/filepath"
	"testing"
)

func TestInstalledAccountBoundaries(t *testing.T) {
	if os.Getenv("CMI_ROLES_TEST") != "1" {
		t.Skip("installed account permission inspection; no facts written")
	}
	ctx := context.Background()
	directory := filepath.Dir(os.Getenv("CMI_MYSQL_TEST_PASSWORD_FILE"))
	for _, role := range []string{"query", "live", "migrate"} {
		db, e := Connect(ctx, Config{Socket: os.Getenv("CMI_MYSQL_TEST_SOCKET"), Username: "cmi_" + role, PasswordFile: filepath.Join(directory, "cmi_"+role+"-password"), MaxConnections: 1})
		if e != nil {
			t.Fatal(role, e)
		}
		catalog, _ := Mappings()
		allowed := []any{}
		seen := map[string]bool{}
		for _, t := range catalog.Tables {
			if !seen[t.Database] {
				allowed = append(allowed, t.Database)
				seen[t.Database] = true
			}
		}
		var count int
		if e = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cmi_storage.schema_version WHERE database_name IN ("+placeholders(len(allowed))+")", allowed...).Scan(&count); e != nil || count != 125 {
			db.Close()
			t.Fatal(role, count, e)
		}
		_, e = db.ExecContext(ctx, "INSERT INTO crypto_market_info.instrument SELECT * FROM crypto_market_info.instrument WHERE 0")
		if (role == "query" && e == nil) || (role != "query" && e != nil) {
			db.Close()
			t.Fatal("fact DML permissions", role, e)
		}
		if _, e = db.ExecContext(ctx, "UPDATE cmi_storage.producer_generation SET generation=generation WHERE service=?", "cmi_permission_absent"); e == nil {
			db.Close()
			t.Fatal("role can activate producers", role)
		}
		if role == "live" {
			tx, e := db.BeginTx(ctx, nil)
			if e != nil {
				t.Fatal(e)
			}
			rows, e := tx.QueryContext(ctx, "SELECT generation FROM cmi_storage.producer_generation WHERE service=? FOR UPDATE", "cmi_permission_absent")
			if e != nil {
				tx.Rollback()
				t.Fatal("live fence needs additional privileges", e)
			}
			rows.Close()
			tx.Rollback()
		}
		if role == "migrate" {
			for _, name := range []string{"migration_table", "migration_resolution", "migration_conflict", "history_coverage"} {
				if _, e = db.ExecContext(ctx, "INSERT INTO cmi_storage.`"+name+"` SELECT * FROM cmi_storage.`"+name+"` WHERE 0"); e != nil {
					t.Fatal("migration control grant", name, e)
				}
			}
		}
		table := "cmi_role_probe_" + uuid.New().String()[:8]
		if _, e = db.ExecContext(ctx, "CREATE TABLE crypto_market_info.`"+table+"` (x INT)"); e == nil {
			db.Close()
			t.Fatal("role has unexpected DDL permissions", role, table)
		}
		db.Close()
	}
}
