package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/across"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	keeper "github.com/vphoenix/crypto-market-info/internal/justlendkeeper"
	"github.com/vphoenix/crypto-market-info/internal/lst"
	"github.com/vphoenix/crypto-market-info/internal/reserve"
	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

func TestMySQLRuntimeAcrossExactIdentityPagesAndRestoreBinding(t *testing.T) {
	c := familyClient(t, "crypto_market_info_across")
	ctx := context.Background()
	var server string
	if err := c.db.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&server); err != nil || server != "a93e84b2-c7c3-11f1-bf43-7ab521822a4d" {
		t.Fatal("runtime integration requires isolated target", err)
	}
	b := acrossFixture(t)
	if err := c.WriteAcrossBatch(ctx, b); err != nil {
		t.Fatal(err)
	}
	if exists, err := c.AcrossCaptureExists(ctx, b.Capture); err != nil || !exists {
		t.Fatal("existing finite-import key", err)
	}
	absent := b.Capture
	absent.CaptureId = across.ID("absent")
	if exists, err := c.AcrossCaptureExists(ctx, absent); err != nil || exists {
		t.Fatal("absent finite-import key", err)
	}

	got, err := c.AcrossCapture(ctx, b.Capture)
	if err != nil || !reflect.DeepEqual(got, b.Capture) {
		t.Fatal("exact capture lookup", err)
	}
	wrong := b.Capture
	wrong.ChainId++
	if _, err = c.AcrossCapture(ctx, wrong); err == nil {
		t.Fatal("cross-chain key accepted")
	}
	// Metadata-only fixtures exercise opaque physical-key paging. They are not
	// passed off as verified fact batches or sent through runtime conversion.
	expected := map[string]bool{b.Capture.CaptureId: true}
	var headers []across.Capture
	for i := 0; i < 35; i++ {
		cap := b.Capture
		cap.CaptureId = across.ID(fmt.Sprint("runtime-page-", i))
		headers = append(headers, cap)
		expected[cap.CaptureId] = true
	}
	insertCaptureFixtures(t, c, "across_capture", headers)
	var cursor []byte
	seen := map[string]bool{}
	for {
		page, next, err := c.AcrossCapturePage(ctx, b.Capture.ManifestHash, cursor, 7)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		if len(next) != 32 || len(cursor) > 0 && bytes.Compare(next, cursor) <= 0 {
			t.Fatal("unstable physical cursor")
		}
		for _, cap := range page {
			if seen[cap.CaptureId] || !expected[cap.CaptureId] {
				t.Fatal("duplicate or unknown page member")
			}
			seen[cap.CaptureId] = true
		}
		cursor = next
	}
	if !reflect.DeepEqual(seen, expected) {
		t.Fatal("incomplete page traversal", len(seen), len(expected))
	}
	epochPath := filepath.Join(t.TempDir(), "recovery-epoch")
	epoch, err := runtimecheckpoint.RotateEpoch(epochPath, server+"/"+c.database, "", "isolated runtime test")
	if err != nil {
		t.Fatal(err)
	}
	binding, err := c.RuntimeBinding(ctx, across.Hex(b.Capture.ManifestHash), epochPath, 1)
	if err != nil {
		t.Fatal(err)
	}
	if binding.ProducerGeneration != c.producer.Generation || binding.Holder != c.producer.Holder.String() || binding.RecoveryEpoch != epoch.ID || binding.SchemaHash == "" {
		t.Fatal(binding)
	}
	if _, err = runtimecheckpoint.RotateEpoch(epochPath, server+"/"+c.database, epoch.ID, "simulated database restore"); err != nil {
		t.Fatal(err)
	}
	changed, err := c.RuntimeBinding(ctx, binding.ScopeHash, epochPath, 1)
	if err != nil || changed.RecoveryEpoch == binding.RecoveryEpoch {
		t.Fatal("restore epoch was ignored", err)
	}
	if _, err = ActivateProducer(ctx, c.db, c.producer.Service, c.producer.Generation, uuid.New()); err != nil {
		t.Fatal(err)
	}
	if _, err = c.RuntimeBinding(ctx, binding.ScopeHash, epochPath, 1); !errors.Is(err, contracts.ErrFenced) {
		t.Fatal("old generation accepted", err)
	}
}

func TestMySQLRuntimeReserveCompositeIdentityAndPages(t *testing.T) {
	c := familyClient(t, "crypto_market_info_reserve")
	ctx := context.Background()
	var server string
	if err := c.db.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&server); err != nil || server != "a93e84b2-c7c3-11f1-bf43-7ab521822a4d" {
		t.Fatal("requires isolated target", err)
	}
	b := reserveFixture(20)
	if err := c.WriteReserveBatch(ctx, b); err != nil {
		t.Fatal(err)
	}
	if exists, err := c.ReserveCaptureExists(ctx, b.Capture); err != nil || !exists {
		t.Fatal("existing finite-import key", err)
	}
	absent := b.Capture
	absent.CaptureId = reserve.ID("absent")
	if exists, err := c.ReserveCaptureExists(ctx, absent); err != nil || exists {
		t.Fatal("absent finite-import key", err)
	}

	got, err := c.ReserveCapture(ctx, b.Capture)
	if err != nil || !reflect.DeepEqual(got, b.Capture) {
		t.Fatal("composite identity", err)
	}
	for _, change := range []func(*reserve.Capture){func(v *reserve.Capture) { v.ManifestHash = reserve.ID("other") }, func(v *reserve.Capture) { v.BatchId = reserve.ID("other") }, func(v *reserve.Capture) { v.ChainId++ }} {
		wrong := b.Capture
		change(&wrong)
		if _, err = c.ReserveCapture(ctx, wrong); err == nil {
			t.Fatal("cross-scope key accepted")
		}
	}
	expected := map[string]bool{b.Capture.CaptureId + b.Capture.BatchId: true}
	var rows []reserve.Capture
	for i := 0; i < 35; i++ {
		cap := b.Capture
		cap.CaptureId = reserve.ID(fmt.Sprint("capture-", i/5))
		cap.BatchId = reserve.ID(fmt.Sprint("attempt-", i))
		expected[cap.CaptureId+cap.BatchId] = true
		rows = append(rows, cap)
	}
	insertCaptureFixtures(t, c, "reserve_capture", rows)
	seen := map[string]bool{}
	var after []byte
	for {
		page, next, e := c.ReserveCapturePage(ctx, b.Capture.ManifestHash, after, 7)
		if e != nil {
			t.Fatal(e)
		}
		if len(page) == 0 {
			break
		}
		if len(next) != 64 || len(after) > 0 && bytes.Compare(next, after) <= 0 {
			t.Fatal("unstable composite cursor")
		}
		for _, cap := range page {
			key := cap.CaptureId + cap.BatchId
			if seen[key] || !expected[key] {
				t.Fatal("duplicate or unknown attempt")
			}
			seen[key] = true
		}
		after = next
	}
	if !reflect.DeepEqual(seen, expected) {
		t.Fatal("incomplete conversion", len(seen), len(expected))
	}
}

func TestMySQLRuntimeLSTExactRevisionAndCompletePaging(t *testing.T) {
	c := familyClient(t, "crypto_market_info_lst")
	ctx := context.Background()
	var server string
	if err := c.db.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&server); err != nil || server != "a93e84b2-c7c3-11f1-bf43-7ab521822a4d" {
		t.Fatal("runtime integration requires isolated target", err)
	}
	// Registration-only rows verify this narrow interface, not fact completeness.
	b := lstStorageMarket()
	if err := b.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := c.WriteLST(ctx, b); err != nil {
		t.Fatal(err)
	}
	if exists, err := c.LSTCaptureExists(ctx, b.Capture); err != nil || !exists {
		t.Fatal("existing finite-import key", err)
	}
	absent := b.Capture
	absent.CaptureId = uuid.New()
	if exists, err := c.LSTCaptureExists(ctx, absent); err != nil || exists {
		t.Fatal("absent finite-import key", err)
	}

	base := b.Capture
	headers := []lst.Capture{}
	expected := map[uuid.UUID]bool{base.CaptureId: true}
	for i := 0; i < 35; i++ {
		cap := base
		cap.CaptureId = uuid.New()
		headers = append(headers, cap)
		expected[cap.CaptureId] = true
	}
	insertCaptureFixtures(t, c, "lst_capture", headers)
	actual, err := c.LSTCapture(ctx, base)
	if err != nil || lst.CanonicalHash(actual) != lst.CanonicalHash(base) {
		t.Fatal(actual, err)
	}
	wrong := base
	wrong.ManifestHash = string(bytes.Repeat([]byte{8}, 32))
	if _, err = c.LSTCapture(ctx, wrong); err == nil {
		t.Fatal("wrong identity accepted")
	}
	seen := map[uuid.UUID]bool{}
	for after := []byte(nil); ; {
		rows, next, e := c.LSTCapturePage(ctx, base.ManifestHash, after, 7)
		if e != nil {
			t.Fatal(e)
		}
		if len(rows) == 0 {
			break
		}
		if len(next) != 16 || bytes.Compare(next, after) <= 0 {
			t.Fatal("invalid cursor")
		}
		for _, cap := range rows {
			if seen[cap.CaptureId] || !expected[cap.CaptureId] {
				t.Fatal("page duplicate")
			}
			seen[cap.CaptureId] = true
		}
		after = next
	}
	if !reflect.DeepEqual(seen, expected) {
		t.Fatal("incomplete traversal", len(seen))
	}
}

func TestMySQLRuntimeKeeperCompletePaging(t *testing.T) {
	c := familyClient(t, "crypto_market_info_justlend_keeper")
	ctx := context.Background()
	var server string
	if err := c.db.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&server); err != nil || server != "a93e84b2-c7c3-11f1-bf43-7ab521822a4d" {
		t.Fatal("requires isolated target", err)
	}
	b := keeperFixture(t)
	if err := c.WriteKeeperBatch(ctx, b); err != nil {
		t.Fatal(err)
	}
	if exists, err := c.KeeperCaptureExists(ctx, b.Capture); err != nil || !exists {
		t.Fatal("existing finite-import key", err)
	}
	absent := b.Capture
	absent.CaptureId = uuid.New()
	if exists, err := c.KeeperCaptureExists(ctx, absent); err != nil || exists {
		t.Fatal("absent finite-import key", err)
	}

	headers := make([]keeper.Capture, 520)
	expected := map[uuid.UUID]bool{b.Capture.CaptureId: true}
	for i := range headers {
		headers[i] = b.Capture
		headers[i].CaptureId = uuid.New()
		expected[headers[i].CaptureId] = true
	}
	insertCaptureFixtures(t, c, "jl_keeper_capture", headers)
	seen := map[uuid.UUID]bool{}
	for after := []byte(nil); ; {
		rows, next, err := c.KeeperCapturePage(ctx, b.Capture.ConfigHash, after, 256)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) == 0 {
			break
		}
		if len(next) != 16 || bytes.Compare(next, after) <= 0 {
			t.Fatal("unstable conversion cursor")
		}
		for _, cap := range rows {
			if seen[cap.CaptureId] || !expected[cap.CaptureId] {
				t.Fatal("duplicate or wrong capture")
			}
			seen[cap.CaptureId] = true
		}
		after = next
	}
	if !reflect.DeepEqual(seen, expected) {
		t.Fatal("truncated Keeper history", len(seen), len(expected))
	}
	if rows, _, err := c.KeeperCapturePage(ctx, string(bytes.Repeat([]byte{9}, 32)), nil, 500); err != nil || len(rows) != 0 {
		t.Fatal("configuration filter lost", err)
	}
	if _, _, err := c.KeeperCapturePage(ctx, b.Capture.ConfigHash, []byte{1}, 500); err == nil {
		t.Fatal("malformed cursor accepted")
	}
}

func TestMySQLRuntimeDEXExactRevisionAndCompositePaging(t *testing.T) {
	if os.Getenv("MYSQL_INTEGRATION") != "1" {
		t.Skip("isolated MySQL required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	db, err := Connect(ctx, Config{Socket: os.Getenv("CMI_MYSQL_TEST_SOCKET"), Username: os.Getenv("CMI_MYSQL_TEST_USER"), PasswordFile: os.Getenv("CMI_MYSQL_TEST_PASSWORD_FILE"), MaxConnections: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var server string
	if err = db.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&server); err != nil || server != "a93e84b2-c7c3-11f1-bf43-7ab521822a4d" {
		t.Fatal("requires isolated target", err)
	}
	target := "cmi_test_runtime_dex_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = db.ExecContext(ctx, "CREATE DATABASE `"+target+"`"); err != nil {
		t.Fatal(err)
	}
	defer db.ExecContext(context.Background(), "DROP DATABASE `"+target+"`")
	for _, statement := range strings.Split(controlDDL, ";") {
		if strings.Contains(statement, "CREATE DATABASE") || strings.Contains(statement, "CREATE TABLE IF NOT EXISTS cmi_storage.producer_generation (") {
			if _, err = db.ExecContext(ctx, statement); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, name := range []string{"dex_block", "dex_route_quote", "dex_sky_state", "dex_log", "dex_tx_receipt"} {
		m, e := Mapping("crypto_market_info", name)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = db.ExecContext(ctx, strings.Replace(m.DDL, "`crypto_market_info`", "`"+target+"`", 1)); e != nil {
			t.Fatal(e)
		}
	}
	holder := uuid.New()
	gen, err := ActivateProducer(ctx, db, target, 0, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer db.ExecContext(context.Background(), "DELETE FROM cmi_storage.producer_generation WHERE service=?", target)
	c := &Client{db: db, database: target, family: "crypto_market_info", producer: Producer{Service: target, Generation: gen, Holder: holder}}
	batch := dexTestBatch(100)
	if err = c.WriteDEXBatch(ctx, batch); err != nil {
		t.Fatal(err)
	}
	actual, ok, err := c.DEXRuntimeBlock(ctx, batch.Block)
	if err != nil || !ok || !reflect.DeepEqual(actual, batch.Block) {
		t.Fatal("exact actual block", actual, ok, err)
	}
	orphan := batch.Block
	orphan.Revision += 100
	orphan.Canonical = false
	orphan.Finality = "orphaned"
	if err = c.WriteDEXBlock(ctx, orphan); err != nil {
		t.Fatal(err)
	}
	if err = c.WriteDEXBlock(ctx, batch.Block); err != nil {
		t.Fatal(err)
	}
	actual, ok, err = c.DEXRuntimeBlock(ctx, batch.Block)
	if err != nil || !ok || actual.Canonical || actual.Revision != orphan.Revision {
		t.Fatal("stale result revived old revision", err)
	}
	missing := batch.Block
	missing.Hash = dex.ObjectHash("missing")
	if _, ok, err = c.DEXRuntimeBlock(ctx, missing); err != nil || ok {
		t.Fatal("wrong composite key", err)
	}
	mapping, err := c.mapping("dex_block")
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string]bool{fmt.Sprintf("%020d/%s", orphan.Number, orphan.Hash): true}
	// Registration-only fixtures exercise height/hash pagination, including
	// duplicate heights and the upper half of UInt64. They are not fact proofs.
	err = c.transaction(ctx, func(tx *sql.Tx) error {
		for i := 0; i < 520; i++ {
			b := batch.Block
			b.Number = uint64(i/2) + (uint64(1) << 63)
			b.Hash = dex.ObjectHash(fmt.Sprint("runtime-page-", i))
			r, e := namedRecord(dexBlockColumns, blockValues(b))
			if e != nil {
				return e
			}
			if e = insertRecord(ctx, tx, c.table("dex_block"), mapping, r); e != nil {
				return e
			}
			expected[fmt.Sprintf("%020d/%s", b.Number, b.Hash)] = true
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for after := []byte(nil); ; {
		rows, next, e := c.DEXRuntimePage(ctx, batch.Block.Manifest, after, 256)
		if e != nil {
			t.Fatal(e)
		}
		if len(rows) == 0 {
			break
		}
		if len(next) != 40 || bytes.Compare(next, after) <= 0 {
			t.Fatal("invalid composite cursor")
		}
		for _, b := range rows {
			key := fmt.Sprintf("%020d/%s", b.Number, b.Hash)
			if seen[key] || !expected[key] {
				t.Fatal("wrong page member")
			}
			seen[key] = true
		}
		after = next
	}
	if !reflect.DeepEqual(seen, expected) {
		t.Fatal("partial conversion page traversal", len(seen), len(expected))
	}
}
