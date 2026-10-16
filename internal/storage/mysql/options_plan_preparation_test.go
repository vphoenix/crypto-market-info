package mysql

import (
	"context"
	"crypto/sha256"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func minimalOptionsPlanClient(t *testing.T) *Client {
	t.Helper()
	if os.Getenv("MYSQL_INTEGRATION") != "1" {
		t.Skip("isolated MySQL required")
	}
	ctx := context.Background()
	cfg := Config{Socket: os.Getenv("CMI_MYSQL_TEST_SOCKET"), Username: os.Getenv("CMI_MYSQL_TEST_USER"), PasswordFile: os.Getenv("CMI_MYSQL_TEST_PASSWORD_FILE"), MaxConnections: 2}
	db, err := Connect(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	target := "cmi_test_planprep_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	t.Cleanup(func() {
		db.ExecContext(context.Background(), "DROP DATABASE `"+target+"`")
		for _, table := range []string{"schema_version", "identity_allocator"} {
			db.ExecContext(context.Background(), "DELETE FROM cmi_storage.`"+table+"` WHERE database_name=?", target)
		}
		db.ExecContext(context.Background(), "DELETE FROM cmi_storage.producer_generation WHERE service=?", target)
		db.Close()
	})
	for _, statement := range splitDDL(controlDDL) {
		if _, err = db.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = db.ExecContext(ctx, "CREATE DATABASE `"+target+"`"); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"instrument", "derivative_contract_spec", "derivative_trading_rule", "options_live_run", "options_metadata_observation", "options_index_minute", "options_live_minute_commit", "options_catalog_scope_observation", "options_lifecycle_observation", "options_collection_plan", "options_catalog_quality_evidence_minute", "options_catalog_live_minute_commit"} {
		mapping, e := Mapping("crypto_market_info", name)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = db.ExecContext(ctx, strings.Replace(mapping.DDL, "`crypto_market_info`", "`"+target+"`", 1)); e != nil {
			t.Fatal(e)
		}
		hash := sha256.Sum256([]byte(mapping.DDL))
		if _, e = db.ExecContext(ctx, "INSERT INTO cmi_storage.schema_version(database_name,table_name,version,ddl_hash,installed_at) VALUES(?,?,1,?,UTC_TIMESTAMP(6))", target, name, hash[:]); e != nil {
			t.Fatal(e)
		}
	}
	if err = initOptionsPlanQueryIndexes(ctx, db, target); err != nil {
		t.Fatal(err)
	}
	holder := uuid.New()
	generation, err := ActivateProducer(ctx, db, target, 0, holder)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Database, cfg.Family = target, "crypto_market_info"
	cfg.Producer = Producer{Service: target, Generation: generation, Holder: holder}
	c, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestOptionsPlanReferenceWaitDoesNotHoldGeneration(t *testing.T) {
	c := minimalOptionsPlanClient(t)
	ctx := context.Background()
	run, envelope := catalogDBFixture(t, c)
	first, err := c.LoadOptionsPlan(ctx, envelope.PlanID)
	if err != nil {
		t.Fatal(err)
	}
	second := first
	second.ID = uuid.New()
	second.PreviousID, second.PreviousHash = first.ID, first.Hash()
	second.Revision++
	second.CreatedAt = first.CreatedAt.Add(time.Minute)
	second.EffectiveMinute = first.EffectiveMinute.Add(time.Minute)
	adminCfg := Config{Socket: os.Getenv("CMI_MYSQL_TEST_SOCKET"), Username: os.Getenv("CMI_MYSQL_TEST_USER"), PasswordFile: os.Getenv("CMI_MYSQL_TEST_PASSWORD_FILE"), MaxConnections: 2}
	admin, err := Connect(ctx, adminCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	blocker, err := admin.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	if _, err = blocker.ExecContext(ctx, "LOCK TABLES "+c.table("options_live_run")+" WRITE"); err != nil {
		t.Fatal(err)
	}
	defer blocker.ExecContext(context.Background(), "UNLOCK TABLES")
	writeCtx, cancelWrite := context.WithTimeout(ctx, 10*time.Second)
	defer cancelWrite()
	done := make(chan error, 1)
	go func() { done <- c.WriteOptionsPlan(writeCtx, second) }()
	deadline := time.Now().Add(3 * time.Second)
	for {
		var waiting int
		err = admin.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE DB=? AND STATE='Waiting for table metadata lock' AND INFO LIKE '%options_live_run%'", c.database).Scan(&waiting)
		if err != nil {
			t.Fatal(err)
		}
		if waiting > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("publication did not reach blocked reference read")
		}
		time.Sleep(20 * time.Millisecond)
	}
	probe, err := admin.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	var generation uint64
	err = probe.QueryRowContext(ctx, "SELECT generation FROM cmi_storage.producer_generation WHERE service=? FOR UPDATE NOWAIT", c.producer.Service).Scan(&generation)
	probe.Rollback()
	if err != nil || generation != c.producer.Generation {
		t.Fatal("reference read pinned the generation lock", generation, err)
	}
	if _, err = blocker.ExecContext(ctx, "UNLOCK TABLES"); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("publication did not finish after reference read resumed")
	}
	latest, err := c.LatestOptionsPlan(ctx)
	if err != nil || latest.ID != second.ID {
		t.Fatal("successor/boundary changed", latest.ID, err)
	}
	// A committed retry still verifies the existing identity without re-reading
	// immutable references. It does not create a new publication or boundary.
	if _, err = c.db.ExecContext(ctx, "DELETE FROM "+c.table("options_live_run")+" WHERE run_id=?", run.ID[:]); err != nil {
		t.Fatal(err)
	}
	if err = c.WriteOptionsPlan(ctx, second); err != nil {
		t.Fatal("durable publication retry changed", err)
	}
}
