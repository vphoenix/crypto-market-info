package mysql

import (
	"context"
	"crypto/sha256"
	"testing"
)

func TestMinuteSchemaUsesOneReadAndDetectsEveryTableMismatch(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	c.db.SetMaxOpenConns(1)
	c.db.SetMaxIdleConns(1)
	selects := func() uint64 {
		t.Helper()
		var name string
		var count uint64
		if err := c.db.QueryRowContext(ctx, "SHOW SESSION STATUS LIKE 'Com_select'").Scan(&name, &count); err != nil {
			t.Fatal(err)
		}
		return count
	}
	before := selects()
	if err := c.InitMinuteSchema(ctx); err != nil {
		t.Fatal(err)
	}
	if count := selects() - before; count != 1 {
		t.Fatal("schema check borrowed the pool repeatedly", count)
	}
	for _, name := range MinuteSchemaTables() {
		if _, err := c.db.ExecContext(ctx, "UPDATE cmi_storage.schema_version SET version=2 WHERE database_name=? AND table_name=?", c.database, name); err != nil {
			t.Fatal(err)
		}
		if err := c.InitMinuteSchema(ctx); err == nil {
			t.Fatal("batched check skipped an incompatible member", name)
		}
		if _, err := c.db.ExecContext(ctx, "UPDATE cmi_storage.schema_version SET version=1 WHERE database_name=? AND table_name=?", c.database, name); err != nil {
			t.Fatal(err)
		}
	}
	last := MinuteSchemaTables()[len(MinuteSchemaTables())-1]
	table, err := c.mapping(last)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256([]byte(table.DDL))
	if _, err = c.db.ExecContext(ctx, "UPDATE cmi_storage.schema_version SET ddl_hash=? WHERE database_name=? AND table_name=?", make([]byte, 32), c.database, last); err != nil {
		t.Fatal(err)
	}
	if err = c.InitMinuteSchema(ctx); err == nil {
		t.Fatal("batched check accepted an incorrect DDL hash")
	}
	if _, err = c.db.ExecContext(ctx, "DELETE FROM cmi_storage.schema_version WHERE database_name=? AND table_name=?", c.database, last); err != nil {
		t.Fatal(err)
	}
	if err = c.InitMinuteSchema(ctx); err == nil {
		t.Fatal("batched check accepted a missing member")
	}
	if _, err = c.db.ExecContext(ctx, "INSERT INTO cmi_storage.schema_version(database_name,table_name,version,ddl_hash,installed_at) VALUES(?,?,1,?,UTC_TIMESTAMP(6))", c.database, last, hash[:]); err != nil {
		t.Fatal(err)
	}
	if err = c.InitMinuteSchema(ctx); err != nil {
		t.Fatal("compatible metadata did not recover", err)
	}
	if stats := c.db.Stats(); stats.InUse != 0 || stats.OpenConnections > 1 {
		t.Fatal("schema verification leaked a connection", stats)
	}
}
