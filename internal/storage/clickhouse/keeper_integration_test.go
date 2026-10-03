package clickhouse

import (
	"context"
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

func TestKeeperFiveTableRoundTripAndFrozenRetry(t *testing.T) {
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
	if e = c.conn.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database=?", db).Scan(&n); e != nil || n != 5 {
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
