package mysql

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

func TestReopenedPhysicalConnectionRejectsChangedSchemaAndInvalidatesCache(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	var invalidated atomic.Int32
	cfg := Config{Socket: os.Getenv("CMI_MYSQL_TEST_SOCKET"), Username: os.Getenv("CMI_MYSQL_TEST_USER"), PasswordFile: os.Getenv("CMI_MYSQL_TEST_PASSWORD_FILE"), MaxConnections: 1, ReadOnly: true, Database: c.database, Family: "crypto_market_info", SchemaTables: []string{"instrument"}, SchemaInvalidated: func() { invalidated.Add(1) }}
	db, e := Connect(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	db.SetMaxIdleConns(0)
	if _, e = c.db.ExecContext(ctx, "UPDATE cmi_storage.schema_version SET version=2 WHERE database_name=? AND table_name='instrument'", c.database); e != nil {
		t.Fatal(e)
	}
	defer c.db.ExecContext(ctx, "UPDATE cmi_storage.schema_version SET version=1 WHERE database_name=? AND table_name='instrument'", c.database)
	bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if e = db.PingContext(bounded); e == nil {
		t.Fatal("reconnected to incompatible schema")
	}
	if invalidated.Load() == 0 {
		t.Fatal("stale metadata cache was not invalidated")
	}
	if _, e = c.db.ExecContext(ctx, "UPDATE cmi_storage.schema_version SET version=1 WHERE database_name=? AND table_name='instrument'", c.database); e != nil {
		t.Fatal(e)
	}
	if e = db.PingContext(ctx); e != nil {
		t.Fatal("compatible schema could not reconnect", e)
	}
}
