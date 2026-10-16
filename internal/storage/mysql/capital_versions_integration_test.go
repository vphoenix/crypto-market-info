package mysql

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
)

// Opt-in isolated suite only. Never enable MYSQL_INTEGRATION against the live
// socket; the normal offline run skips this fixture without opening a database.
func TestMySQLCapitalVersionPublicationAndLegacyRetry(t *testing.T) {
	if os.Getenv("CMI_MYSQL_TEST_DATABASE") != "" {
		t.Skip("capital extensions require a fresh isolated fixture, not the reusable minute database")
	}
	c := integrationClient(t)
	ctx := context.Background()
	// Opting in must fail before any writer can use an absent extension.
	config := Config{Socket: os.Getenv("CMI_MYSQL_TEST_SOCKET"), Username: os.Getenv("CMI_MYSQL_TEST_USER"), PasswordFile: os.Getenv("CMI_MYSQL_TEST_PASSWORD_FILE"), Database: c.database, Family: c.family, MaxConnections: 1, ReadOnly: true, CapitalRuleVersions: true}
	if unchecked, err := Open(ctx, config); err == nil {
		unchecked.Close()
		t.Fatal("missing capital schema accepted")
	}
	if err := InstallCapitalVersionSchema(ctx, c.db, c.database); err != nil {
		t.Fatal(err)
	}
	if err := InstallCapitalVersionSchema(ctx, c.db, c.database); err != nil {
		t.Fatal("repeated install is not idempotent", err)
	}
	reader, err := Open(ctx, config)
	if err != nil {
		t.Fatal("installed extension failed startup check", err)
	}
	reader.Close()
	for _, name := range CapitalVersionTableNames() {
		var format, options string
		if err := c.db.QueryRowContext(ctx, "SELECT ROW_FORMAT,CREATE_OPTIONS FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?", c.database, name).Scan(&format, &options); err != nil || !strings.EqualFold(format, "Compressed") || !strings.Contains(strings.ToLower(options), "key_block_size=8") {
			t.Fatal("extension is not natively compressed", name, format, options, err)
		}
	}
	if err := c.InitDiscoverySchema(ctx); err != nil {
		t.Fatal(err)
	}
	forecast := discoveryFixture(t, c)
	p := capitalFixture()
	p.InstrumentID = forecast.InstrumentID
	p.Base = "TEST"
	p.SourceMS = forecast.SourceMS
	p.ObservedMS = forecast.ObservedMS
	p.EffectiveMS = p.SourceMS
	p.ValidUntilMS = p.ObservedMS + 3600000
	for n := range p.Sources {
		p.Sources[n].URL = strings.ReplaceAll(p.Sources[n].URL, "BTC", "TEST")
		p.Sources[n].SourceMS = p.SourceMS
		p.Sources[n].ObservedMS = p.ObservedMS
	}
	if err := c.WriteDiscoveryCapital(ctx, p); err != nil {
		t.Fatal(err)
	}
	_, legacy, err := c.LoadDiscoveryCapital(ctx, p.InstrumentID, time.Now().Add(time.Millisecond))
	if err != nil || legacy.Codec != model.DiscoveryCodec {
		t.Fatal("legacy fact unreadable", err)
	}
	// An old incomplete intent is completed in its original representation even
	// after enabling v2. The first-known timestamp must not move.
	pending := p
	pending.ID = "legacy-intent-before-cutover"
	intent := publicationRecord{Kind: pending.Kind(), ID: pending.ID, Hash: model.DiscoveryHash(pending.Body()), Codec: model.DiscoveryCodec, SourceMS: pending.SourceMS, ObservedMS: pending.ObservedMS, KnownMS: model.CeilMilliseconds(time.Now()), InstrumentID: pending.InstrumentID, EffectiveMS: pending.EffectiveMS, SourceCount: uint16(len(pending.Sources)), ProvenanceHash: model.DiscoverySourcesHash(pending.Sources)}
	known, err := c.beginDiscoveryIntent(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	c.capitalRuleVersions = true
	if err := c.WriteDiscoveryCapital(ctx, pending); err != nil {
		t.Fatal(err)
	}
	rows, err := c.records(ctx, c.db, "source_batch_status", "WHERE source_kind=? AND source_id=?", []byte(pending.Kind()), []byte(pending.ID))
	if err != nil || len(rows) != 1 {
		t.Fatal(err)
	}
	retried, err := scanProof(rows[0])
	if err != nil || retried.Codec != model.DiscoveryCodec || retried.KnownMS != known {
		t.Fatal("cutover rewrote old intent", err)
	}
	p.ID = "v2-first"
	p.SourceMS++
	p.ObservedMS++
	p.EffectiveMS++
	p.ValidUntilMS++
	for n := range p.Sources {
		p.Sources[n].SourceMS = p.SourceMS
		p.Sources[n].ObservedMS = p.ObservedMS
	}
	// Force failure after all three new fact tables have been populated but
	// before provenance/publication. Only the separately durable intent survives.
	trigger := "`" + c.database + "`.`capital_provenance_failure`"
	if _, err := c.db.ExecContext(ctx, "CREATE TRIGGER "+trigger+" BEFORE INSERT ON "+c.table("public_observation_provenance")+" FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='capital atomic rollback test'"); err != nil {
		t.Fatal(err)
	}
	if err := c.WriteDiscoveryCapital(ctx, p); err == nil {
		t.Fatal("injected provenance failure was accepted")
	}
	for _, name := range CapitalVersionTableNames() {
		var count int
		if err := c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+c.table(name)).Scan(&count); err != nil || count != 0 {
			t.Fatal("failed publication left partial version facts", name, count, err)
		}
	}
	var published int
	if err := c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+c.table("source_batch_status")+" WHERE source_id=?", []byte(p.ID)).Scan(&published); err != nil || published != 0 {
		t.Fatal("failed transaction published a fact", published, err)
	}
	var firstKnown int64
	if err := c.db.QueryRowContext(ctx, "SELECT known_from_ms FROM "+c.table("source_publication_intents")+" WHERE source_kind=? AND source_id=?", []byte(p.Kind()), []byte(p.ID)).Scan(&firstKnown); err != nil {
		t.Fatal("first knowledge did not survive rollback", err)
	}
	if _, err := c.db.ExecContext(ctx, "DROP TRIGGER "+trigger); err != nil {
		t.Fatal(err)
	}
	if err := c.WriteDiscoveryCapital(ctx, p); err != nil {
		t.Fatal(err)
	}
	got, proof, err := c.LoadDiscoveryCapital(ctx, p.InstrumentID, time.Now().Add(time.Millisecond))
	if err != nil || proof.Codec != model.CapitalVersionsCodec || proof.KnownMS != firstKnown || model.DiscoveryHash(got.Body()) != model.DiscoveryHash(p.Body()) {
		t.Fatal("v2 roundtrip", err)
	}
	c.capitalRuleVersions = false // Existing v2 retries must not switch back to v1.
	if err := c.WriteDiscoveryCapital(ctx, p); err != nil {
		t.Fatal(err)
	}
	_, again, err := c.LoadDiscoveryCapital(ctx, p.InstrumentID, time.Now().Add(time.Millisecond))
	if err != nil || again != proof {
		t.Fatal("retry changed first knowledge/publication", err)
	}
	c.capitalRuleVersions = true
	next := p
	next.ID = "v2-next"
	next.ObservedMS++
	next.Sources = append([]model.PublicSource(nil), p.Sources...)
	for n := range next.Sources {
		next.Sources[n].ObservedMS = next.ObservedMS
	}
	if err := c.WriteDiscoveryCapital(ctx, next); err != nil {
		t.Fatal(err)
	}
	var curves, risks int
	if err := c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+c.table("okx_discount_rule_version")).Scan(&curves); err != nil {
		t.Fatal(err)
	}
	if err := c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+c.table("okx_risk_rule_version")).Scan(&risks); err != nil {
		t.Fatal(err)
	}
	if curves != 2 || risks != 2 {
		t.Fatal("repeated observations duplicated curves", curves, risks)
	}
	// A committed newest observation whose reference disappears is an error,
	// never a reason to return the older, apparently valid legacy observation.
	v, err := model.SplitCapitalVersions(next)
	if err != nil {
		t.Fatal(err)
	}
	key, _ := compactBinaryDigest(v.Perp.ID)
	if _, err := c.db.ExecContext(ctx, "DELETE FROM "+c.table("okx_risk_rule_version")+" WHERE version_hash=?", []byte(key)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.LoadDiscoveryCapital(ctx, p.InstrumentID, time.Now().Add(time.Millisecond)); err == nil {
		t.Fatal("missing version silently fell back")
	}
}
