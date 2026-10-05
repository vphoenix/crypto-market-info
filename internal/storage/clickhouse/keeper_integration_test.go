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
	keeper "github.com/vphoenix/crypto-market-info/internal/justlendkeeper"
)

func TestKeeperSevenTableRoundTripAndFrozenRetry(t *testing.T) {
	if os.Getenv("KEEPER_DB_TEST") != "1" {
		t.Skip("set KEEPER_DB_TEST=1 for isolated local database")
	}
	ctx := context.Background()
	db := "crypto_market_info_justlend_keeper_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	c, e := Open(ctx, Config{Database: db})
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	defer c.conn.Exec(ctx, "DROP DATABASE `"+db+"`")
	if e = c.InitKeeperSchema(ctx); e != nil {
		t.Fatal(e)
	}
	var n uint64
	if e = c.conn.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database=?", db).Scan(&n); e != nil || n != 7 {
		t.Fatal(e, n)
	}
	cfg := keeper.DefaultConfig()
	capTime := time.Date(2026, 1, 31, 23, 59, 59, 900000000, time.UTC)
	id := uuid.New()
	addr, _ := keeper.HexAddress(keeper.ContractHex)
	renter, _ := keeper.HexAddress(cfg.Callers[1])
	caller, _ := keeper.HexAddress(cfg.Callers[0])
	hash := keeper.Hash([]byte("binary-hash\xff"))
	amount := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
	raw := keeper.Archive{Dir: t.TempDir()}
	mh, e := raw.Put(keeper.JSONEvidence(keeper.Manifest{Version: "keeper-v1", DigestEncoding: "jl-keeper-fact-v1", Capture: id.String()}))
	if e != nil {
		t.Fatal(e)
	}
	b := keeper.Batch{Capture: keeper.Capture{CaptureId: id, CaptureStartedAt: capTime, ConfigHash: cfg.Hash(), Network: keeper.Network, ContractAddress: addr, CaptureKind: "events", CaptureMode: "backfill", SourceId: "trongrid", CoverageScope: "returned_liquidations", AvailableAt: capTime, Status: "complete", EvidenceManifestHash: mh, Committed: true}}
	b.Events = []keeper.RentalEvent{{CaptureId: id, CaptureStartedAt: capTime, ContractAddress: addr, BlockNumber: 1, BlockHash: hash, BlockTime: capTime, Finality: "solid", TxId: hash, TransactionIndex: keeper.Ptr(uint32(2)), ReceiptLogIndex: keeper.Ptr(uint32(3)), PositionStatus: "receipt_verified", EventKind: "liquidate", Renter: renter, Receiver: renter, ResourceType: 1, Liquidator: &caller, AmountSun: amount, RewardSun: big.NewInt(20000000), SendBackSun: big.NewInt(0), UsageRentalSun: big.NewInt(1), RequestStartedAt: capTime, AvailableAt: capTime, PayloadHash: hash}}
	b.Events = append(b.Events, keeper.RentalEvent{CaptureId: id, CaptureStartedAt: capTime, ContractAddress: addr, BlockNumber: 1, BlockHash: hash, BlockTime: capTime, Finality: "solid", TxId: hash, TransactionIndex: keeper.Ptr(uint32(2)), ProviderEventIndex: 1, ReceiptLogIndex: keeper.Ptr(uint32(4)), PositionStatus: "receipt_verified", AbiRevision: "energy-market-events-v2", EventKind: "rent", Renter: renter, Receiver: renter, ResourceType: 1, AmountSun: big.NewInt(1000000), AddedAmountSun: big.NewInt(1000000), AddedDepositSun: big.NewInt(20000000), SecurityDepositSun: amount, RentIndex: big.NewInt(1051630712545733418), RequestStartedAt: capTime, AvailableAt: capTime, PayloadHash: hash})
	b.Receipts = []keeper.TxReceipt{{CaptureId: id, CaptureStartedAt: capTime, BlockNumber: 1, BlockHash: hash, BlockTime: capTime, Finality: "solid", TxId: hash, Sender: &caller, ExecutionResult: "SUCCESS", BodyComplete: true, ReceiptComplete: true, EnergyUsageTotal: keeper.Ptr(uint64(10000)), NativeTransferStatus: "present", NativeTransfers: []keeper.NativeTransfer{{InternalIndex: 1, ValueIndex: 0, Sender: addr, Recipient: caller, AmountSun: *amount, Rejected: false}}, RentalLiquidationLogCount: 1, CallClass: "direct_liquidate", RequestStartedAt: capTime, AvailableAt: capTime, ReceiptPayloadHash: hash}}
	b.Probes = []keeper.Probe{{CaptureId: id, CaptureStartedAt: capTime, ContractAddress: addr, Renter: renter, Receiver: renter, ResourceType: 1, CohortId: uuid.New(), CallerAddress: caller, ScheduledAt: capTime, AvailableAt: capTime, EndpointView: "wallet_latest", StateBinding: "node_latest_unpinned", IdentityStatus: "unknown", Status: "timeout", RewardConsistency: "incomplete"}}
	d := decimal.RequireFromString("0.123456789012345678")
	b.Costs = []keeper.CostObservation{{CaptureId: id, CaptureStartedAt: capTime, ObservationKind: "trx_usdt_bbo", SourceId: "binance", RequestStartedAt: capTime, ReceivedAt: &capTime, AvailableAt: capTime, SourceTime: &capTime, SourceTimeKind: "received", StateBinding: "offchain", Symbol: "TRXUSDT", BidPriceUsdt: &d, BidQtyTrx: &d, AskPriceUsdt: &d, AskQtyTrx: &d, Status: "ok", PayloadHash: &hash}}
	keeper.Seal(&b)
	// Simulate facts committed while final capture marker was lost.
	if e = c.dexInsert(ctx, "jl_keeper_rental_event", keeperColumns(reflect.TypeOf(keeper.RentalEvent{})), keeperValues(b.Events)); e != nil {
		t.Fatal(e)
	}
	if e = c.WriteKeeperBatch(ctx, b); e != nil {
		t.Fatal(e)
	}
	if e = c.WriteKeeperBatch(ctx, b); e != nil {
		t.Fatal(e)
	}
	got, e := c.KeeperBatch(ctx, b.Capture)
	if e != nil {
		t.Fatal(e)
	}
	empty := keeper.Batch{Capture: b.Capture}
	empty.Capture.CaptureId = uuid.New()
	empty.Capture.ConfigHash = keeper.Hash([]byte("foreign-test-config"))
	keeper.Seal(&empty)
	if e = c.WriteKeeperBatch(ctx, empty); e != nil {
		t.Fatal(e)
	}
	bulk, e := c.KeeperBatches(ctx, []keeper.Capture{b.Capture, empty.Capture})
	if e != nil || len(bulk) != 2 || len(bulk[b.Capture.CaptureId].Events) != 2 || len(bulk[empty.Capture.CaptureId].Costs) != 0 {
		t.Fatal("bulk typed roundtrip", e)
	}
	// A table with expected count zero must still be read and authenticated.
	extra := b.Costs[0]
	extra.CaptureId = empty.Capture.CaptureId
	if e = c.dexInsert(ctx, "jl_keeper_cost_observation", keeperColumns(reflect.TypeOf(extra)), keeperValues([]keeper.CostObservation{extra})); e != nil {
		t.Fatal(e)
	}
	if _, e = c.KeeperBatches(ctx, []keeper.Capture{empty.Capture}); e == nil {
		t.Fatal("unexpected empty-table member accepted")
	}
	if got.Events[0].AmountSun.Cmp(amount) != 0 || got.Receipts[0].FeeSun != nil || got.Receipts[0].NativeTransfers[0].AmountSun.Cmp(amount) != 0 || !got.Costs[0].BidPriceUsdt.Equal(d) {
		t.Fatal("roundtrip changed uint256/null/tuple/decimal")
	}
	if got.Events[1].SecurityDepositSun == nil || got.Events[1].SecurityDepositSun.Cmp(amount) != 0 || got.Events[1].RentIndex == nil || got.Events[1].RentIndex.Cmp(b.Events[1].RentIndex) != 0 || got.Events[0].SecurityDepositSun != nil {
		t.Fatal("extended event uint256/null changed")
	}
	a, _ := keeper.Freeze(b)
	z, _ := keeper.Freeze(got)
	if string(a) != string(z) {
		t.Fatal("typed batch did not roundtrip exactly")
	}
	var rows uint64
	if e = c.conn.QueryRow(ctx, "SELECT count() FROM `"+db+"`.jl_keeper_rental_event FINAL").Scan(&rows); e != nil || rows != 2 {
		t.Fatal(e, rows)
	}
	mutated := b
	mutated.Capture.AvailableAt = capTime.Add(time.Hour)
	if e = c.WriteKeeperBatch(ctx, mutated); e == nil {
		t.Fatal("mutable cross-month retry accepted")
	}
	reader, e := OpenDEXReader(ctx, Config{Database: db})
	if e != nil {
		t.Fatal(e)
	}
	defer reader.Close()
	if e = reader.WriteKeeperBatch(ctx, b); e == nil {
		t.Fatal("read-only writer allowed")
	}
	if e = keeper.Report(ctx, reader, raw, cfg, capTime.Add(-time.Hour), capTime.Add(time.Hour), capTime.Add(2*time.Hour), t.TempDir()); e != nil {
		t.Fatal(e)
	}
	quoteRaw, e := os.ReadFile("../../../research/2026-10-03-keeper-implementation/quote.raw")
	if e != nil {
		t.Fatal(e)
	}
	quotes := [][]byte{
		[]byte(`{"symbol":"TRXUSDT","bidPrice":"0.1","bidQty":"2.3","askPrice":"0.2","askQty":"4.5"}`),
		quoteRaw,
		[]byte(`{"symbol":"TRXUSDT","bidPrice":"0.123456789012345678","bidQty":"2.123456789012345678","askPrice":"0.223456789012345678","askQty":"4.123456789012345678"}`),
	}
	for i, body := range quotes {
		at := capTime.Add(time.Duration(i+1) * time.Minute)
		h, er := raw.Put(body)
		if er != nil {
			t.Fatal(er)
		}
		q, er := keeper.ParseQuote(body, keeper.Evidence{Started: at, Received: &at, Available: at, ResponseHash: keeper.Hex(h)})
		if er != nil {
			t.Fatal(er)
		}
		quoteCap := keeper.Capture{CaptureId: uuid.New(), CaptureStartedAt: at, AvailableAt: at, ConfigHash: cfg.Hash(), Network: keeper.Network, ContractAddress: addr, CaptureKind: "costs", CaptureMode: "live", SourceId: "binance", Status: "complete"}
		quoteCap.EvidenceManifestHash, er = raw.Put(keeper.JSONEvidence(keeper.Manifest{Version: "keeper-v1", DigestEncoding: "jl-keeper-fact-v1", Capture: quoteCap.CaptureId.String()}))
		if er != nil {
			t.Fatal(er)
		}
		q.CaptureId = quoteCap.CaptureId
		q.CaptureStartedAt = at
		qb := keeper.Batch{Capture: quoteCap, Costs: []keeper.CostObservation{q}}
		keeper.Seal(&qb)
		if er = c.WriteKeeperBatch(ctx, qb); er != nil {
			t.Fatal(er)
		}
		readBack, er := c.KeeperBatch(ctx, qb.Capture)
		if er != nil {
			t.Fatal(er)
		}
		if readBack.Costs[0].BidPriceUsdt.Exponent() != -18 || !readBack.Costs[0].BidPriceUsdt.Equal(*q.BidPriceUsdt) {
			t.Fatal("source Decimal did not roundtrip", i)
		}
		if i == 1 {
			// Equivalent eight-place values keep the same canonical digest
			// when ClickHouse reads them with eighteen places.
			legacy := qb
			legacy.Capture.CaptureId = uuid.New()
			legacy.Costs = append([]keeper.CostObservation(nil), qb.Costs...)
			legacy.Costs[0].CaptureId = legacy.Capture.CaptureId
			r := &legacy.Costs[0]
			for _, field := range []**decimal.Decimal{&r.BidPriceUsdt, &r.BidQtyTrx, &r.AskPriceUsdt, &r.AskQtyTrx} {
				d := decimal.NewFromBigInt((*field).Shift(8).BigInt(), -8)
				*field = &d
			}
			legacy.Capture.EvidenceManifestHash, er = raw.Put(keeper.JSONEvidence(keeper.Manifest{Version: "keeper-v1", DigestEncoding: "jl-keeper-fact-v1", Capture: legacy.Capture.CaptureId.String()}))
			if er != nil {
				t.Fatal(er)
			}
			keeper.Seal(&legacy)
			if er = c.WriteKeeperBatch(ctx, legacy); er != nil {
				t.Fatal(er)
			}
			got, er := c.KeeperBatch(ctx, legacy.Capture)
			if er != nil {
				t.Fatal(er)
			}
			if !got.Costs[0].BidPriceUsdt.Equal(*legacy.Costs[0].BidPriceUsdt) {
				t.Fatal("equivalent-scale digest roundtrip")
			}
			if er = c.WriteKeeperBatch(ctx, legacy); er != nil {
				t.Fatal("committed legacy retry", er)
			}
		}
	}
	if e = keeper.Report(ctx, reader, raw, cfg, capTime.Add(-time.Hour), capTime.Add(time.Hour), capTime.Add(2*time.Hour), t.TempDir()); e != nil {
		t.Fatal("source-scale/legacy report", e)
	}
}

func TestKeeperIndexFrontierRetryAndLegacyDefaults(t *testing.T) {
	if os.Getenv("KEEPER_DB_TEST") != "1" {
		t.Skip("set KEEPER_DB_TEST=1")
	}
	ctx := context.Background()
	db := "crypto_market_info_justlend_keeper_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	c, err := Open(ctx, Config{Database: db})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	defer c.conn.Exec(ctx, "DROP DATABASE `"+db+"`")
	if err = c.InitKeeperSchema(ctx); err != nil {
		t.Fatal(err)
	}
	// Simulate an actual old table row, with no knowledge of the three new fields.
	raw, err := os.ReadFile("../../../research/2026-10-03-keeper-collection-split/fixtures/legacy-frozen-state.gob")
	if err != nil {
		t.Fatal(err)
	}
	var old keeper.State
	if err = keeper.Thaw(raw, &old); err != nil {
		t.Fatal(err)
	}
	legacy := old.Ops[0].Batch
	for _, col := range []string{"indexed_rows", "indexed_digest", "parent_capture_id"} {
		if err = c.conn.Exec(ctx, "ALTER TABLE "+c.table("jl_keeper_capture")+" DROP COLUMN "+col); err != nil {
			t.Fatal(err)
		}
	}
	columns := strings.Split(keeperColumns(reflect.TypeOf(legacy.Capture)), ",")
	values := keeperValues([]keeper.Capture{legacy.Capture})
	if err = c.dexInsert(ctx, "jl_keeper_capture", strings.Join(columns[:len(columns)-3], ","), [][]any{values[0][:len(values[0])-3]}); err != nil {
		t.Fatal(err)
	}
	if err = c.InitKeeperSchema(ctx); err != nil {
		t.Fatal(err)
	}
	if err = c.WriteKeeperBatch(ctx, legacy); err != nil {
		t.Fatal("old frozen retry after defaults migration", err)
	}
	readLegacy, err := c.KeeperCaptures(ctx, legacy.Capture.CaptureStartedAt, legacy.Capture.CaptureStartedAt.Add(time.Second))
	if err != nil || len(readLegacy) != 1 || readLegacy[0].IndexedDigest != nil || readLegacy[0].ParentCaptureId != nil {
		t.Fatal("legacy NULL defaults changed", err)
	}
	cfg := keeper.DefaultConfig()
	addr, _ := keeper.HexAddress(keeper.ContractHex)
	renter, _ := keeper.HexAddress(cfg.Callers[1])
	at := legacy.Capture.CaptureStartedAt.Add(time.Hour)
	archive := keeper.Archive{Dir: t.TempDir()}
	amount := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
	parent := func(from, to time.Time, in, out string, rows bool) keeper.Batch {
		cap := keeper.Capture{CaptureId: uuid.New(), CaptureStartedAt: at, AvailableAt: at, ConfigHash: cfg.Hash(), Network: keeper.Network, ContractAddress: addr, CaptureKind: "event_index", CaptureMode: "live", SourceId: "trongrid", EventKind: "RentResource", CoverageScope: "indexed_energy_events", RequestedFrom: &from, RequestedTo: &to, Status: "complete", PageCount: 1, PaginationExhausted: out == ""}
		cap.EvidenceManifestHash, _ = archive.Put(keeper.JSONEvidence(keeper.Manifest{Version: "keeper-v1", DigestEncoding: "jl-keeper-fact-v1", Capture: cap.CaptureId.String(), Kind: "index_page", ScanID: "watch:v2:" + from.String(), FingerprintIn: in, FingerprintOut: out}))
		b := keeper.Batch{Capture: cap, IndexPage: &keeper.IndexPage{CaptureId: cap.CaptureId, CaptureStartedAt: at, ScanID: "watch:v2:" + from.String(), FingerprintIn: in, FingerprintOut: out, RequestStartedAt: at, AvailableAt: at, PayloadHash: keeper.Hash([]byte("source"))}}
		if rows {
			for i := 0; i < 2; i++ {
				b.IndexedEvents = append(b.IndexedEvents, keeper.IndexedEvent{CaptureId: cap.CaptureId, CaptureStartedAt: at, Ordinal: uint32(i), ContractAddress: addr, BlockNumber: 1, BlockTime: from, Finality: "provider_claimed_confirmed", TxId: keeper.Hash([]byte("tx")), ProviderEventIndex: 0, PositionStatus: "indexed_only", AbiRevision: "energy-market-events-v2", EventKind: "rent", Renter: renter, Receiver: renter, ResourceType: 1, AmountSun: amount, AddedAmountSun: amount, AddedDepositSun: big.NewInt(0), SecurityDepositSun: amount, RentIndex: amount, RequestStartedAt: at, AvailableAt: at, PayloadHash: keeper.Hash([]byte("source"))})
			}
		}
		b.Capture.DiscoveredCandidates = uint32(len(b.IndexedEvents))
		keeper.Seal(&b)
		if err = c.WriteKeeperBatch(ctx, b); err != nil {
			t.Fatal(err)
		}
		return b
	}
	p1 := parent(at, at.Add(30*time.Second), "", "next", true)
	p2 := parent(at, at.Add(30*time.Second), "next", "", false)
	p3 := parent(at.Add(30*time.Second), at.Add(time.Minute), "", "", false)
	round, err := c.KeeperBatch(ctx, p1.Capture)
	if err != nil || len(round.IndexedEvents) != 2 || round.IndexedEvents[1].AmountSun.Cmp(amount) != 0 || round.IndexedEvents[0].BlockHash != nil {
		t.Fatal("indexed UInt256/NULL/duplicate ordinal roundtrip", err)
	}
	if err = c.WriteKeeperBatch(ctx, p1); err != nil {
		t.Fatal("indexed frozen retry", err)
	}
	child := func(p keeper.Batch, status string, available time.Time) {
		cap := keeper.Capture{CaptureId: uuid.New(), CaptureStartedAt: available, AvailableAt: available, ConfigHash: cfg.Hash(), Network: keeper.Network, ContractAddress: addr, CaptureKind: "enrichment", CaptureMode: "live", SourceId: "publicnode", EventKind: "RentResource", CoverageScope: "sampled_rental_events", RequestedFrom: p.Capture.RequestedFrom, RequestedTo: p.Capture.RequestedTo, ParentCaptureId: &p.Capture.CaptureId, Status: status}
		cap.DiscoveredCandidates = p.Capture.IndexedRows
		cap.EvidenceManifestHash = keeper.Hash([]byte("summary"))
		b := keeper.Batch{Capture: cap}
		keeper.Seal(&b)
		if err = c.WriteKeeperBatch(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	check := func(caps []keeper.Capture) (bool, error) {
		pages, err := c.KeeperIndexPages(ctx, caps)
		if err != nil {
			return false, err
		}
		tokens := map[string]string{}
		for _, cap := range caps {
			p, ok := pages[cap.CaptureId]
			if !ok {
				return false, nil
			}
			if _, ok := tokens[p.FingerprintIn]; ok {
				return false, nil
			}
			tokens[p.FingerprintIn] = p.FingerprintOut
		}
		seen := map[string]bool{}
		token := ""
		for !seen[token] {
			next, ok := tokens[token]
			if !ok {
				return false, nil
			}
			seen[token] = true
			if next == "" {
				return len(seen) == len(caps), nil
			}
			token = next
		}
		return false, nil
	}
	if err := os.RemoveAll(archive.Dir); err != nil {
		t.Fatal(err)
	}
	child(p3, "complete", at)
	child(p2, "complete", at)
	child(p1, "error", at)
	frontier, err := c.KeeperEvidenceFrontier(ctx, cfg.Hash(), "RentResource", at, at.Add(time.Minute), check)
	if err != nil || !frontier.Equal(at) {
		t.Fatal("later window concealed missing earlier proof", frontier, err)
	}
	pending, err := c.KeeperNextEnrichment(ctx, cfg.Hash(), at.Add(time.Minute))
	if err != nil || pending != nil {
		t.Fatal("failed child skipped retry delay", pending, err)
	}
	pending, err = c.KeeperNextEnrichment(ctx, cfg.Hash(), at.Add(31*time.Minute))
	if err != nil || pending == nil || pending.CaptureId != p1.Capture.CaptureId {
		t.Fatal("failed child not eligible after retry delay", pending, err)
	}
	child(p1, "complete", at.Add(31*time.Minute))
	frontier, err = c.KeeperEvidenceFrontier(ctx, cfg.Hash(), "RentResource", at, at.Add(time.Minute), check)
	if err == nil {
		frontier, err = c.KeeperEvidenceFrontier(ctx, cfg.Hash(), "RentResource", frontier, at.Add(time.Minute), check)
	}
	if err != nil || !frontier.Equal(at.Add(time.Minute)) {
		t.Fatal("late gap repair did not cross already-completed later window", frontier, err)
	}
	frontier, err = c.KeeperEvidenceFrontier(ctx, cfg.Hash(), "RentResource", at, at.Add(time.Minute), func([]keeper.Capture) (bool, error) { return false, nil })
	if err != nil || !frontier.Equal(at) {
		t.Fatal("frontier ignored pagination-chain rejection", err)
	}
	oldWithoutPage := p2
	oldWithoutPage.Capture.CaptureId = uuid.New()
	oldWithoutPage.IndexPage = nil
	if err = c.WriteKeeperBatch(ctx, oldWithoutPage); err != nil {
		t.Fatal(err)
	}
	wrong := oldWithoutPage
	page := *p2.IndexPage
	page.CaptureId = wrong.Capture.CaptureId
	wrong.IndexPage = &page
	wrong.Capture.Reason = "mutated retry"
	if err = c.WriteKeeperBatch(ctx, wrong); err == nil {
		t.Fatal("mutated capture retry accepted")
	}
	progress, err := c.KeeperIndexPages(ctx, []keeper.Capture{oldWithoutPage.Capture})
	if err != nil || len(progress) != 0 {
		t.Fatal("rejected capture retry wrote pagination metadata", err)
	}
	// Read all member tables even when a legacy capture expects no index members.
	injected := p1.IndexedEvents[0]
	injected.CaptureId = legacy.Capture.CaptureId
	injected.CaptureStartedAt = legacy.Capture.CaptureStartedAt
	if err = c.dexInsert(ctx, "jl_keeper_indexed_event", keeperColumns(reflect.TypeOf(injected)), keeperValues([]keeper.IndexedEvent{injected})); err != nil {
		t.Fatal(err)
	}
	if _, err = c.KeeperBatches(ctx, []keeper.Capture{legacy.Capture}); err == nil {
		t.Fatal("unexpected legacy index member was ignored")
	}
	// A single pagination chain longer than the old 256-row SQL cap remains
	// certifiable; read the entire current window, rather than truncating it.
	largeFrom, largeTo := at.Add(2*time.Minute), at.Add(3*time.Minute)
	all := make([]keeper.Capture, 0, 514)
	var allPages []keeper.IndexPage
	for i := 0; i < 257; i++ {
		cap := p2.Capture
		cap.CaptureId = uuid.New()
		cap.CaptureStartedAt = largeFrom
		cap.AvailableAt = largeFrom
		cap.RequestedFrom = &largeFrom
		cap.RequestedTo = &largeTo
		cap.PaginationExhausted = i == 256
		in, out := "", fmt.Sprint(i+1)
		if i > 0 {
			in = fmt.Sprint(i)
		}
		if i == 256 {
			out = ""
		}
		cap.EvidenceManifestHash = keeper.Hash([]byte("summary"))
		allPages = append(allPages, keeper.IndexPage{CaptureId: cap.CaptureId, CaptureStartedAt: largeFrom, ScanID: "watch:v2:long-window", FingerprintIn: in, FingerprintOut: out, RequestStartedAt: largeFrom, AvailableAt: largeFrom, PayloadHash: keeper.Hash([]byte("source"))})
		b := keeper.Batch{Capture: cap}
		keeper.Seal(&b)
		all = append(all, b.Capture)
		ch := cap
		ch.CaptureId = uuid.New()
		ch.CaptureKind = "enrichment"
		ch.IndexedDigest = nil
		ch.ParentCaptureId = keeper.Ptr(cap.CaptureId)
		ch.SourceId = "publicnode"
		ch.CoverageScope = "sampled_rental_events"
		ch.EvidenceManifestHash = keeper.Hash([]byte("summary"))
		b = keeper.Batch{Capture: ch}
		keeper.Seal(&b)
		all = append(all, b.Capture)
	}
	for i := 0; i < len(allPages); i += 256 {
		if err = c.WriteKeeperIndexPages(ctx, allPages[i:min(i+256, len(allPages))]); err != nil {
			t.Fatal(err)
		}
	}
	if err = c.dexInsert(ctx, "jl_keeper_capture", keeperColumns(reflect.TypeOf(keeper.Capture{})), keeperValues(all)); err != nil {
		t.Fatal(err)
	}
	frontier, err = c.KeeperEvidenceFrontier(ctx, cfg.Hash(), "RentResource", largeFrom, largeTo, func(caps []keeper.Capture) (bool, error) {
		if len(caps) != 257 {
			return false, fmt.Errorf("window truncated at %d", len(caps))
		}
		return check(caps)
	})
	if err != nil || !frontier.Equal(largeTo) {
		t.Fatal("large source window became permanently stuck", frontier, err)
	}
}
