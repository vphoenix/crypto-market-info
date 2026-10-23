package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/options"
	"github.com/vphoenix/crypto-market-info/internal/replay"
	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
)

func qualityEvidenceFixture() options.QualityEvidence {
	q := options.QualityEvidence{RunID: uuid.New(), InstrumentID: 1, MinuteTime: time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC), BatchID: strings.Repeat("a", 64)}
	id := uuid.MustParse("00000000-0000-4000-8000-000000000001")
	for n := 0; n < 60; n++ {
		var value *uuid.UUID
		var at *time.Time
		kind := uint8(0)
		if n >= 13 && n < 49 {
			v := id
			value = &v
			tm := q.MinuteTime.Add(time.Duration(n)*time.Second + 123456*time.Microsecond)
			at = &tm
			kind = 2
		}
		q.StateIDs = append(q.StateIDs, value)
		q.StateKinds = append(q.StateKinds, kind)
		q.RuleObservationIDs = append(q.RuleObservationIDs, value)
		q.PlatformIDs = append(q.PlatformIDs, value)
		q.MaintenanceIDs = append(q.MaintenanceIDs, value)
		q.LockIDs = append(q.LockIDs, value)
		q.LifecycleEpochs = append(q.LifecycleEpochs, id)
		q.LifecycleConfirmed = append(q.LifecycleConfirmed, at)
	}
	q.RowHash = q.Hash()
	return q
}

func sqlRoundTripQualityRecord(t *testing.T, name string, r record) record {
	t.Helper()
	table, err := Mapping("crypto_market_info", name)
	if err != nil {
		t.Fatal(err)
	}
	out := record{}
	for _, col := range table.Columns {
		v, err := codec.SQLValue(col.Type, r[col.Name])
		if err != nil {
			t.Fatal(col.Name, err)
		}
		out[col.Name], err = codec.FromSQL(col.Type, v)
		if err != nil {
			t.Fatal(col.Name, err)
		}
	}
	return out
}

func TestQualityProfilePreservesAllSecondsNullsAndMicroseconds(t *testing.T) {
	q := qualityEvidenceFixture()
	if err := q.Validate(); err != nil {
		t.Fatal(err)
	}
	p, m, err := splitQualityEvidence(q)
	if err != nil {
		t.Fatal(err)
	}
	p = sqlRoundTripQualityRecord(t, qualityProfile, p)
	m = sqlRoundTripQualityRecord(t, qualityMember, m)
	for k, v := range p {
		m[k] = v
	}
	old, _ := Mapping("crypto_market_info", "options_catalog_quality_evidence_minute")
	if err = expandAliases(old, m); err != nil {
		t.Fatal(err)
	}
	got, err := evidenceFromRecord(m)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, q) {
		t.Fatal("shared evidence did not exactly reconstruct every field")
	}
	q.RunID = uuid.New()
	q.InstrumentID++
	q.BatchID = strings.Repeat("b", 64)
	q.RowHash = q.Hash()
	shared, _, err := splitQualityEvidence(q)
	if err != nil {
		t.Fatal(err)
	}
	if shared["profile_hash"] != p["profile_hash"] {
		t.Fatal("member identity prevents evidence sharing")
	}
	q.LifecycleConfirmed[14] = nil
	q.RowHash = q.Hash()
	different, _, err := splitQualityEvidence(q)
	if err != nil {
		t.Fatal(err)
	}
	if different["profile_hash"] == p["profile_hash"] {
		t.Fatal("NULL evidence collapsed into a timestamp")
	}
	q.MinuteTime = q.MinuteTime.Add(time.Minute)
	q.RowHash = q.Hash()
	next, _, err := splitQualityEvidence(q)
	if err != nil {
		t.Fatal(err)
	}
	if next["profile_hash"] == different["profile_hash"] {
		t.Fatal("profile digest does not bind the UTC minute")
	}
}

func TestQualitySchemaKeepsFrozenMappingAndMinuteLayout(t *testing.T) {
	catalog, err := Mappings()
	if err != nil {
		t.Fatal(err)
	}
	if len(catalog.Tables) != 125 {
		t.Fatal("source migration inventory changed")
	}
	for _, name := range QualityStorageTableNames() {
		table, err := Mapping("crypto_market_info", name)
		if err != nil {
			t.Fatal(err)
		}
		if table.Key[0] != "minute_time" || !strings.Contains(table.DDL, "ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8") {
			t.Fatal("quality layout/compression", name)
		}
		if strings.Contains(table.DDL, "CREATE TABLE IF NOT EXISTS `crypto_market_info`.`derivative_book_quality_minute` ") {
			t.Fatal("extension would overwrite old table")
		}
	}
	book, _ := Mapping("crypto_market_info", qualityMinuteV2)
	if !strings.Contains(book.DDL, "PRIMARY KEY (`minute_time`,`instrument_id`,`compact_batch_id`)") || !strings.Contains(book.DDL, "KEY `instrument_minute`") {
		t.Fatal("minute layout or lookup index missing")
	}
}

func TestMySQLQualityStoragePublicationRollbackAndMixedHistory(t *testing.T) {
	if os.Getenv("CMI_MYSQL_TEST_DATABASE") != "" {
		t.Skip("quality schema suite requires its own isolated database")
	}
	c := integrationClient(t)
	ctx := context.Background()
	run, envelope := catalogDBFixture(t, c)
	if err := InstallQualityStorageSchema(ctx, c.db, c.database); err != nil {
		t.Fatal(err)
	}
	if err := InstallQualityStorageSchema(ctx, c.db, c.database); err != nil {
		t.Fatal(err)
	}
	c.qualityStorageV2 = true
	if err := c.discoverQualityStorage(ctx); err != nil {
		t.Fatal(err)
	}
	// Exercise the actual writer's per-job clients, not only direct writes.
	// Both preparation and publication must retain the installed storage format.
	raw, err := codec.EncodeOptionsCatalog(envelope)
	if err != nil {
		t.Fatal(err)
	}
	delivery := c.Delivery("options_catalog", run.ID.String(), envelope.Live.MinuteTime, uint32(len(envelope.Live.Books)), raw)
	preparer := c.WithProducer(c.producer)
	preparer.prefetchMinuteIndexes = true
	prepared, err := preparer.PrepareDelivery(ctx, delivery)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	if err := c.WithProducer(c.producer).CommitPrepared(ctx, prepared); err != nil {
		t.Fatal(err)
	}
	if ok, err := c.Committed(ctx, delivery); err != nil || !ok {
		t.Fatal("producer-bound quality delivery not committed", ok, err)
	}
	reader, err := Open(ctx, Config{Socket: os.Getenv("CMI_MYSQL_TEST_SOCKET"), Username: os.Getenv("CMI_MYSQL_TEST_USER"), PasswordFile: os.Getenv("CMI_MYSQL_TEST_PASSWORD_FILE"), Database: c.database, Family: "crypto_market_info", ReadOnly: true, MaxConnections: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	reader = reader.WithProducer(c.producer)
	got, err := reader.LoadOptionsCatalogMinute(ctx, run.ID, envelope.Live.MinuteTime)
	if err != nil || got.ID() != envelope.ID() {
		t.Fatal("new format unreadable with write switch off", err)
	}
	for n := range got.Live.Books {
		if model.DerivativeBatchHash(got.Live.Books[n]) != model.DerivativeBatchHash(envelope.Live.Books[n]) || !reflect.DeepEqual(got.Live.Books[n].Quality, envelope.Live.Books[n].Quality) {
			t.Fatal("book or quality changed")
		}
		for sec := 0; sec < 60; sec++ {
			a, ea := replay.ReplayDerivative(envelope.Live.Books[n], uint8(sec))
			b, eb := replay.ReplayDerivative(got.Live.Books[n], uint8(sec))
			if !reflect.DeepEqual(a, b) || fmt.Sprint(ea) != fmt.Sprint(eb) {
				t.Fatal("second replay changed", n, sec)
			}
		}
	}
	var oldCount, newCount int
	if err = c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+c.table("options_catalog_quality_evidence_minute")).Scan(&oldCount); err != nil {
		t.Fatal(err)
	}
	if err = c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+c.table(qualityMember)).Scan(&newCount); err != nil || oldCount != 0 || newCount != len(envelope.Evidence) {
		t.Fatal("unexpected evidence storage", oldCount, newCount, err)
	}
	c.qualityStorageV2 = false
	if err = c.WriteOptionsCatalogMinute(ctx, envelope); err != nil {
		t.Fatal("retry after disabling new writes", err)
	}
	if _, err = reader.LoadOptionsCatalogMinute(ctx, run.ID, envelope.Live.MinuteTime); err != nil {
		t.Fatal("retry duplicated formats", err)
	}
	// Old physical evidence and new physical members remain readable in one client.
	legacy := qualityEvidenceFixture()
	if err = c.transaction(ctx, func(tx *sql.Tx) error {
		return putModelsBulk(ctx, c, tx, "options_catalog_quality_evidence_minute", []options.QualityEvidence{legacy})
	}); err != nil {
		t.Fatal(err)
	}
	back, err := reader.loadQualityEvidence(ctx, legacy.RunID, legacy.MinuteTime, legacy.BatchID)
	if err != nil || len(back) != 1 || !reflect.DeepEqual(back[0], legacy) {
		t.Fatal("legacy history changed", err)
	}
	// Inject a failure after creating both halves. Neither may survive rollback.
	q := qualityEvidenceFixture()
	q.MinuteTime = q.MinuteTime.Add(time.Minute)
	q.RowHash = q.Hash()
	c.qualityStorageV2 = true
	failure := errors.New("publication fault")
	err = c.transaction(ctx, func(tx *sql.Tx) error {
		if err := c.putQualityEvidence(ctx, tx, []options.QualityEvidence{q}); err != nil {
			return err
		}
		return failure
	})
	if !errors.Is(err, failure) {
		t.Fatal(err)
	}
	// Two independent transactions see the same absent profile before either
	// INSERT. The loser rolls back completely and retries the original identity.
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	concurrent := make([]options.QualityEvidence, 2)
	for n := range concurrent {
		concurrent[n] = qualityEvidenceFixture()
		concurrent[n].MinuteTime = q.MinuteTime.Add(time.Minute)
		concurrent[n].InstrumentID = uint32(n + 1)
		concurrent[n].BatchID = strings.Repeat(fmt.Sprint(n+1), 64)
		concurrent[n].RowHash = concurrent[n].Hash()
	}
	type outcome struct {
		index int
		err   error
	}
	finished := make(chan outcome, 2)
	for n := range concurrent {
		go func(n int) {
			tx, err := c.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
			if err != nil {
				finished <- outcome{n, err}
				return
			}
			fault := context.WithValue(ctx, minuteFaultKey{}, func(stage string, _ *sql.Tx) error {
				if stage == "before_quality_profile_insert" {
					entered <- struct{}{}
					<-release
				}
				return nil
			})
			err = c.putQualityEvidence(fault, tx, []options.QualityEvidence{concurrent[n]})
			if err == nil {
				err = tx.Commit()
			} else {
				tx.Rollback()
			}
			finished <- outcome{n, err}
		}(n)
	}
	for range 2 {
		select {
		case <-entered:
		case <-time.After(15 * time.Second):
			close(release)
			t.Fatal("concurrent profile first-insert barrier timed out")
		}
	}
	close(release)
	loser := -1
	for range 2 {
		result := <-finished
		if result.err != nil {
			var temporary interface{ Temporary() bool }
			if !errors.Is(result.err, errMinuteInsertCollision) || !errors.As(result.err, &temporary) || !temporary.Temporary() {
				t.Fatal("profile race became permanent", result.err)
			}
			loser = result.index
		}
	}
	if loser < 0 {
		t.Fatal("first INSERT race was not exercised")
	}
	if err = c.transaction(ctx, func(tx *sql.Tx) error {
		return c.putQualityEvidence(ctx, tx, []options.QualityEvidence{concurrent[loser]})
	}); err != nil {
		t.Fatal("original profile identity could not be retried", err)
	}
	for _, entry := range concurrent {
		restored, err := reader.loadQualityEvidence(ctx, entry.RunID, entry.MinuteTime, entry.BatchID)
		if err != nil || len(restored) != 1 || restored[0].Hash() != entry.Hash() {
			t.Fatal("concurrent member content changed", err)
		}
	}
	// Same profile key with changed physical content is always a hard conflict.
	profile, _, err := splitQualityEvidence(concurrent[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.db.ExecContext(ctx, "UPDATE "+c.table(qualityProfile)+" SET compact_state_kinds_values=CONCAT(compact_state_kinds_values,0x00) WHERE minute_time=? AND profile_hash=?", concurrent[0].MinuteTime, []byte(profile["profile_hash"].(string))); err != nil {
		t.Fatal(err)
	}
	if err = c.transaction(ctx, func(tx *sql.Tx) error { return c.putQualityEvidence(ctx, tx, []options.QualityEvidence{concurrent[0]}) }); err == nil {
		t.Fatal("corrupt existing profile accepted")
	}
	if _, err = reader.loadQualityEvidence(ctx, concurrent[0].RunID, concurrent[0].MinuteTime, concurrent[0].BatchID); err == nil {
		t.Fatal("corrupt profile silently read")
	}
	for _, name := range []string{qualityMember, qualityProfile} {
		var count int
		if err = c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+c.table(name)+" WHERE minute_time=?", q.MinuteTime).Scan(&count); err != nil || count != 0 {
			t.Fatal("partial quality publication", name, count, err)
		}
	}
	// A dangling reference must fail rather than silently falling back to old data.
	if _, err = c.db.ExecContext(ctx, "UPDATE "+c.table(qualityMember)+" SET profile_hash=?", strings.Repeat("x", 32)); err != nil {
		t.Fatal(err)
	}
	if _, err = reader.LoadOptionsCatalogMinute(ctx, run.ID, envelope.Live.MinuteTime); err == nil {
		t.Fatal("dangling profile accepted")
	}
}
