package clickhouse

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/lst"
)

func lstPtr[T any](v T) *T { return &v }
func lstIntegration(t *testing.T) *Client {
	t.Helper()
	if os.Getenv("CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set CLICKHOUSE_INTEGRATION=1")
	}
	ctx := context.Background()
	cfg := Config{Database: fmt.Sprintf("crypto_lst_it_%d", time.Now().UnixNano()), MaxAttempts: 1}
	if addr := os.Getenv("LST_TEST_CLICKHOUSE_ADDRESS"); addr != "" {
		cfg.Addresses = []string{addr}
	}
	c, e := Open(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = c.conn.Exec(context.Background(), "DROP DATABASE `"+cfg.Database+"` SYNC"); _ = c.Close() })
	for i := 0; i < 2; i++ {
		if e = c.InitLSTSchema(ctx); e != nil {
			t.Fatal(e)
		}
	}
	var count uint64
	if e = c.conn.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database=?", cfg.Database).Scan(&count); e != nil || count != 8 {
		t.Fatalf("wanted exactly 8 research tables: %d %v", count, e)
	}
	return c
}
func lstStorageMarket() lst.Batch {
	now := time.Now().UTC().Truncate(time.Microsecond)
	h := string([]byte{0xff, 0x80}) + strings.Repeat("h", 30)
	c := lst.Capture{CaptureId: uuid.New(), ManifestHash: h, CaptureKind: "market", CaptureMode: "live", SourceId: "fixture", StartedAt: now, AvailableAt: now, Finality: "head", Canonical: true, Status: "partial", ExpectedProtocolRows: 1, ExpectedQuoteRows: 1}
	p := lst.ProtocolState{CaptureId: c.CaptureId, ChainId: 1, ObservedAt: now, AvailableAt: now, QueueAddress: strings.Repeat("q", 20), StateStatus: "unknown", StethTotalSharesRaw: new(big.Int).Lsh(big.NewInt(1), 220), PayloadHashes: []string{h}}
	q := lst.Quote{CaptureId: c.CaptureId, QuoteId: h, ObservedAt: now, AvailableAt: now, QuoteRole: "entry", RouteId: "A", QuoteAssetAddress: strings.Repeat("u", 20), LstAddress: strings.Repeat("l", 20), PurchaseBudgetUsdtRaw: big.NewInt(100000000000), HedgeInstrumentId: 1, BuyStatus: "unknown", ConversionStatus: "unknown", ExitStatus: "unknown", HedgeStatus: "unknown", TimingStatus: "unknown", IndicatedFundingRate: lstPtr(decimal.RequireFromString("-0.000000000000000001")), MarkPayloadHash: &h}
	q.MarkSourceTime = lstPtr(now)
	q.MarkRequestedAt = lstPtr(now)
	q.MarkReceivedAt = lstPtr(now)
	q.MarkAvailableAt = lstPtr(now)
	return lst.Batch{Capture: c, Protocols: []lst.ProtocolState{p}, Quotes: []lst.Quote{q}}
}
func TestLSTSchemaIsExplicitlyScoped(t *testing.T) {
	ss, e := LSTSchemaStatements("scratch")
	if e != nil || len(ss) != 8 {
		t.Fatal(ss, e)
	}
	for _, s := range ss {
		if !strings.Contains(s, "`scratch`.`lst_") && !strings.Contains(s, "`scratch`.instrument\n") {
			t.Fatal("unrelated schema", s)
		}
	}
	if _, e = LSTSchemaStatements("unsafe;drop"); e == nil {
		t.Fatal("unsafe database accepted")
	}
}
func TestLSTIntegrationRetryBinaryHashNullableUInt256AndRevision(t *testing.T) {
	c := lstIntegration(t)
	ctx := context.Background()
	b := lstStorageMarket()
	if e := b.Seal(); e != nil {
		t.Fatal(e)
	}
	if e := c.dexInsert(ctx, "lst_quote_observation", lstColumns(reflect.TypeOf(lst.Quote{})), lstRows(b.Quotes)); e != nil {
		t.Fatal(e)
	}
	captures, e := c.LSTCaptures(ctx, b.Capture.ManifestHash)
	if e != nil || len(captures) != 0 {
		t.Fatal("partial capture visible", captures, e)
	}
	for i := 0; i < 2; i++ {
		if e = c.WriteLST(ctx, b); e != nil {
			t.Fatal(e)
		}
	}
	captures, e = c.LSTCaptures(ctx, b.Capture.ManifestHash)
	if e != nil || len(captures) != 1 {
		t.Fatal(captures, e)
	}
	got, e := c.LSTBatch(ctx, captures[0])
	if e != nil {
		t.Fatal(e)
	}
	if got.Protocols[0].StethTotalSharesRaw.Cmp(b.Protocols[0].StethTotalSharesRaw) != 0 || got.Protocols[0].BlockHash != nil || got.Quotes[0].ChainRequestedAt != nil || got.Quotes[0].QuoteId != b.Quotes[0].QuoteId {
		t.Fatal("UInt256/binary/NULL roundtrip failed")
	}
	orphan := b.Capture
	orphan.Canonical = false
	orphan.Finality = "orphaned"
	orphan.Revision++
	if e = c.WriteLSTRevision(ctx, orphan); e != nil {
		t.Fatal(e)
	}
	if e = c.WriteLST(ctx, b); e != nil {
		t.Fatal("old retry should not revive canonical", e)
	}
	captures, e = c.LSTCaptures(ctx, b.Capture.ManifestHash)
	if e != nil || len(captures) != 1 || captures[0].Canonical {
		t.Fatal("old revision revived", captures, e)
	}
	changed := b
	changed.Quotes = append([]lst.Quote(nil), b.Quotes...)
	changed.Quotes[0].Reason = "new network response"
	if e = changed.Seal(); e != nil {
		t.Fatal(e)
	}
	if e = c.WriteLST(ctx, changed); e == nil {
		t.Fatal("changed retry accepted")
	}
	// Tamper with the stored child while retaining a committed capture: reads fail.
	changed.Quotes[0].Reason = "corrupt stored row"
	if e = c.dexInsert(ctx, "lst_quote_observation", lstColumns(reflect.TypeOf(lst.Quote{})), lstRows(changed.Quotes)); e != nil {
		t.Fatal(e)
	}
	if _, e = c.LSTBatch(ctx, captures[0]); e == nil {
		t.Fatal("corrupt row read accepted")
	}
}
func lstStorageLogs() lst.Batch {
	b := lstStorageMarket()
	b.Protocols = nil
	b.Quotes = nil
	c := &b.Capture
	c.ExpectedProtocolRows = 0
	c.ExpectedQuoteRows = 0
	c.CaptureKind = "logs"
	c.Status = "complete"
	c.Finality = "finalized"
	c.ChainId = lstPtr(uint64(1))
	c.FromBlock = lstPtr(uint64(42))
	c.ToBlock = lstPtr(uint64(42))
	c.FromBlockHash = lstPtr(c.ManifestHash)
	c.ToBlockHash = lstPtr(c.ManifestHash)
	c.FromBlockTime = lstPtr(c.StartedAt)
	c.ToBlockTime = lstPtr(c.StartedAt)
	r := lst.WithdrawalRequest{CaptureId: c.CaptureId, ChainId: 1, QueueAddress: strings.Repeat("q", 20), BlockNumber: 42, BlockHash: c.ManifestHash, BlockTime: c.StartedAt, TransactionHash: lst.CanonicalHash("tx"), AbiVersion: "fixture-v1", RequestId: big.NewInt(3), Sender: strings.Repeat("s", 20), InitialOwner: strings.Repeat("o", 20), AmountStethWei: new(big.Int).Lsh(big.NewInt(1), 201), AmountSharesRaw: big.NewInt(10), EventPayloadHash: c.ManifestHash, AvailableAt: c.StartedAt, GasSampleClass: "missing"}
	b.Requests = []lst.WithdrawalRequest{r}
	b.Finalizations = []lst.WithdrawalFinalization{{CaptureId: c.CaptureId, ChainId: 1, QueueAddress: r.QueueAddress, BlockNumber: 42, BlockHash: c.ManifestHash, BlockTime: c.StartedAt, TransactionHash: r.TransactionHash, LogIndex: 1, AbiVersion: r.AbiVersion, FromRequestId: big.NewInt(3), ToRequestId: big.NewInt(3), EthLockedWei: big.NewInt(100), SharesToBurnRaw: big.NewInt(10), EventTimestamp: c.StartedAt, EventPayloadHash: c.ManifestHash, AvailableAt: c.StartedAt}}
	b.Claims = []lst.WithdrawalClaim{{CaptureId: c.CaptureId, ChainId: 1, QueueAddress: r.QueueAddress, BlockNumber: 42, BlockHash: c.ManifestHash, BlockTime: c.StartedAt, TransactionHash: r.TransactionHash, LogIndex: 2, AbiVersion: r.AbiVersion, RequestId: big.NewInt(3), Owner: r.InitialOwner, Receiver: r.InitialOwner, AmountEthWei: big.NewInt(100), EventPayloadHash: c.ManifestHash, AvailableAt: c.StartedAt, GasSampleClass: "missing"}}
	return b
}
func TestLSTIntegrationAllEventsAndReceiptEnrichment(t *testing.T) {
	c := lstIntegration(t)
	ctx := context.Background()
	b := lstStorageLogs()
	if e := b.Seal(); e != nil {
		t.Fatal(e)
	}
	if e := c.WriteLST(ctx, b); e != nil {
		t.Fatal(e)
	}
	got, e := c.LSTBatch(ctx, b.Capture)
	if e != nil {
		t.Fatal(e)
	}
	if got.Requests[0].AmountStethWei.Cmp(b.Requests[0].AmountStethWei) != 0 || len(got.Finalizations) != 1 || len(got.Claims) != 1 {
		t.Fatal("required UInt256 roundtrip failed")
	}
	next := lstStorageLogs()
	next.Capture.StartedAt = b.Capture.StartedAt
	next.Capture.AvailableAt = b.Capture.AvailableAt
	next.Capture.FromBlockTime = b.Capture.FromBlockTime
	next.Capture.ToBlockTime = b.Capture.ToBlockTime
	next.Requests = append([]lst.WithdrawalRequest(nil), b.Requests...)
	next.Finalizations = nil
	next.Claims = nil
	next.Requests[0].CaptureId = next.Capture.CaptureId
	next.Requests[0].GasUsed = lstPtr(uint64(123456))
	next.Requests[0].EffectiveGasPriceWei = big.NewInt(9999999999)
	next.Requests[0].ReceiptPayloadHash = lstPtr(lst.CanonicalHash("receipt"))
	next.Requests[0].ReceiptAvailableAt = lstPtr(next.Capture.AvailableAt)
	next.Requests[0].GasSampleClass = "direct_single"
	if e = next.Seal(); e != nil {
		t.Fatal(e)
	}
	if e = c.WriteLST(ctx, next); e != nil {
		t.Fatal("receipt enrichment rejected", e)
	}
	old, e := c.LSTBatch(ctx, b.Capture)
	if e != nil || old.Requests[0].GasUsed != nil {
		t.Fatal("old evidence mutated", e)
	}
	next.Capture.CaptureId = uuid.New()
	next.Requests[0].CaptureId = next.Capture.CaptureId
	next.Requests[0].AmountStethWei = big.NewInt(12)
	if e = next.Seal(); e != nil {
		t.Fatal(e)
	}
	if e = c.WriteLST(ctx, next); e == nil {
		t.Fatal("conflicting event accepted")
	}
}
func TestLSTIntegrationFundingExactAndConflict(t *testing.T) {
	c := lstIntegration(t)
	ctx := context.Background()
	b := lstStorageMarket()
	b.Protocols = nil
	b.Quotes = nil
	cap := &b.Capture
	cap.CaptureKind = "funding"
	cap.Finality = "not_applicable"
	cap.ExpectedProtocolRows = 0
	cap.ExpectedQuoteRows = 0
	cap.Status = "complete"
	cap.WindowFromAt = lstPtr(cap.StartedAt.Add(-time.Hour))
	cap.WindowToAt = lstPtr(cap.StartedAt)
	b.Funding = []lst.FundingSettlement{{CaptureId: cap.CaptureId, InstrumentId: 1, FundingTime: cap.StartedAt, FundingRate: decimal.RequireFromString("-0.000000000000000001"), SettlementMarkPriceTickE8: lstPtr(int64(312312312312)), SourceId: cap.SourceId, RequestedAt: cap.StartedAt, ReceivedAt: cap.StartedAt, AvailableAt: cap.StartedAt, SourcePayloadHash: cap.ManifestHash}}
	if e := b.Seal(); e != nil {
		t.Fatal(e)
	}
	if e := c.WriteLST(ctx, b); e != nil {
		t.Fatal(e)
	}
	got, e := c.LSTBatch(ctx, b.Capture)
	if e != nil || !got.Funding[0].FundingRate.Equal(b.Funding[0].FundingRate) {
		t.Fatal("funding roundtrip", e)
	}
	cap.CaptureId = uuid.New()
	b.Funding[0].CaptureId = cap.CaptureId
	b.Funding[0].FundingRate = decimal.Zero
	if e = b.Seal(); e != nil {
		t.Fatal(e)
	}
	if e = c.WriteLST(ctx, b); e == nil {
		t.Fatal("funding conflict accepted")
	}
}

func TestLSTWriteGuardsRejectReadOnlyAndZeroAttempts(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		client  *Client
		message string
	}{
		{nil, "lst_client_not_connected"},
		{&Client{readOnly: true, maxAttempts: 1}, "lst_client_read_only"},
		{&Client{maxAttempts: 0}, "lst_write_attempts_not_positive"},
	}
	for _, tc := range cases {
		if e := tc.client.WriteLST(ctx, lst.Batch{}); e == nil || e.Error() != tc.message {
			t.Fatalf("write guard: want %s got %v", tc.message, e)
		}
		if e := tc.client.WriteLSTRevision(ctx, lst.Capture{}); e == nil || e.Error() != tc.message {
			t.Fatalf("revision guard: want %s got %v", tc.message, e)
		}
	}
}

func TestLSTIntegrationExistingWriterAndReadOnlyConstructors(t *testing.T) {
	bootstrap := lstIntegration(t)
	ctx := context.Background()
	cfg := Config{Database: bootstrap.database}
	if addr := os.Getenv("LST_TEST_CLICKHOUSE_ADDRESS"); addr != "" {
		cfg.Addresses = []string{addr}
	}
	writer, e := OpenLSTWriter(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer writer.Close()
	if writer.maxAttempts != 3 || writer.writeTimeout != 10*time.Second || writer.retryDelay != 250*time.Millisecond {
		t.Fatal("writer defaults missing")
	}
	b := lstStorageMarket()
	if e = b.Seal(); e != nil {
		t.Fatal(e)
	}
	if e = writer.WriteLST(ctx, b); e != nil {
		t.Fatal(e)
	}
	got, e := writer.LSTBatch(ctx, b.Capture)
	if e != nil || got.Capture.FactDigest != b.Capture.FactDigest {
		t.Fatal("new constructor silently skipped write", e)
	}
	for name, open := range map[string]func(context.Context, Config) (*Client, error){"dex": OpenDEXReader, "general": OpenReadOnly} {
		reader, e := open(ctx, cfg)
		if e != nil {
			t.Fatal(name, e)
		}
		if _, e = reader.LSTBatch(ctx, b.Capture); e != nil {
			reader.Close()
			t.Fatal(name, "read failed", e)
		}
		if e = reader.WriteLST(ctx, b); e == nil || e.Error() != "lst_client_read_only" {
			reader.Close()
			t.Fatal(name, "readonly write accepted", e)
		}
		if e = reader.WriteLSTRevision(ctx, b.Capture); e == nil || e.Error() != "lst_client_read_only" {
			reader.Close()
			t.Fatal(name, "readonly revision accepted", e)
		}
		reader.Close()
	}
	writer.maxAttempts = 0
	if e = writer.WriteLST(ctx, b); e == nil || e.Error() != "lst_write_attempts_not_positive" {
		t.Fatal("zero retry count silently accepted write", e)
	}
	if e = writer.WriteLSTRevision(ctx, b.Capture); e == nil || e.Error() != "lst_write_attempts_not_positive" {
		t.Fatal("zero retry count silently accepted revision", e)
	}
	missing := cfg
	missing.Database = bootstrap.database + "_missing"
	unexpected, e := OpenLSTWriter(ctx, missing)
	if e == nil {
		unexpected.Close()
		t.Fatal("writer created missing database")
	}
	var count uint64
	if e = bootstrap.conn.QueryRow(ctx, "SELECT count() FROM system.databases WHERE name=?", missing.Database).Scan(&count); e != nil || count != 0 {
		t.Fatal("writer caused database DDL", count, e)
	}
	if e = bootstrap.conn.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database=?", cfg.Database).Scan(&count); e != nil || count != 8 {
		t.Fatal("writer caused table DDL", count, e)
	}
}
