package mysql

import (
	"context"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/across"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	keeper "github.com/vphoenix/crypto-market-info/internal/justlendkeeper"
	"github.com/vphoenix/crypto-market-info/internal/lst"
	"github.com/vphoenix/crypto-market-info/internal/reserve"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func lstPtr[T any](v T) *T { return &v }
func acrossFixture(t *testing.T) across.Batch {
	t.Helper()
	now := across.Now()
	id := across.ID("capture")
	h := across.ID("block")
	num := new(big.Int).Lsh(big.NewInt(1), 220)
	initRow := func(out any) {
		x := reflect.ValueOf(out).Elem()
		for i := 0; i < x.NumField(); i++ {
			v := x.Field(i)
			tag := x.Type().Field(i).Tag.Get("dbtype")
			if strings.HasPrefix(tag, "FixedString(") {
				n, _ := strconv.Atoi(tag[12 : len(tag)-1])
				v.SetString(strings.Repeat("x", n))
			}
			if strings.HasPrefix(tag, "DateTime64") {
				v.Set(reflect.ValueOf(now))
			}
			if tag == "UInt256" {
				v.Set(reflect.ValueOf(new(big.Int).Set(num)))
			}
		}
		x.FieldByName("CaptureId").SetString(id)
		x.FieldByName("ChainId").SetUint(8453)
		if x.FieldByName("BlockNumber").Kind() != reflect.Pointer {
			x.FieldByName("BlockNumber").SetUint(7)
			x.FieldByName("BlockHash").SetString(h)
		}
	}
	b := across.Batch{Capture: across.Capture{ManifestHash: across.ID("manifest"), CaptureId: id, ChainId: 8453, CaptureKind: "logs", CaptureMode: "live", FromBlock: across.Ptr(uint64(7)), ToBlock: across.Ptr(uint64(7)), FromHash: &h, ToHash: &h, StartedAt: now, AvailableAt: now, SourceId: "fixture", Status: "complete", EvidenceHash: across.ID("evidence"), Canonical: true, Finality: "head"}}
	var d across.Deposit
	initRow(&d)
	d.DestinationChainId = 42161
	d.MessageHash = strings.Repeat("\x00", 32)
	d.LiveReceivedAt = &now
	b.Deposits = []across.Deposit{d}
	var u across.DepositUpdate
	initRow(&u)
	u.DepositorSignature = "public fixture"
	b.Updates = []across.DepositUpdate{u}
	var f across.Fill
	initRow(&f)
	f.OriginChainId = 42161
	f.FillType = 1
	b.Fills = []across.Fill{f}
	var r across.Refund
	initRow(&r)
	r.EventKind = "leaf_execution"
	r.RootBundleId = across.Ptr(uint32(1))
	r.LeafId = across.Ptr(uint32(2))
	r.RefundAddresses = []string{strings.Repeat("a", 20), strings.Repeat("b", 20)}
	r.RefundAmountsRaw = []*big.Int{new(big.Int).Set(num), big.NewInt(0)}
	r.DeferredRefunds = across.Ptr(true)
	b.Refunds = []across.Refund{r}
	var tx across.TxReceipt
	initRow(&tx)
	tx.Success = true
	tx.FeeComplete = false
	tx.FeeRule = "unknown"
	tx.Recipient = across.Ptr(strings.Repeat("t", 20))
	b.Receipts = []across.TxReceipt{tx}
	var p across.OrderProbe
	initRow(&p)
	p.OriginChainId = 42161
	p.ProbeStatus = "skipped_budget"
	p.ObservationOrigin = "live"
	p.TargetDelayMs = across.Ptr(uint32(2000))
	p.EthUsdtAsk = across.Ptr(decimal.RequireFromString("2001.000000000000000001"))
	b.Probes = []across.OrderProbe{p}
	across.Seal(&b)
	if e := across.Validate(b); e != nil {
		t.Fatal(e)
	}
	return b
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
func TestMySQLAcrossTypedBatchAndRevision(t *testing.T) {
	c := familyClient(t, "crypto_market_info_across")
	ctx := context.Background()
	b := acrossFixture(t)
	for range 2 {
		if e := c.WriteAcrossBatch(ctx, b); e != nil {
			t.Fatal(e)
		}
	}
	caps, e := c.AcrossCaptures(ctx, b.Capture.ManifestHash)
	if e != nil || len(caps) != 1 {
		t.Fatal(e)
	}
	got, e := c.AcrossBatch(ctx, caps[0])
	if e != nil {
		t.Fatal(e)
	}
	if across.ID(got) != across.ID(b) {
		t.Fatal("UInt256, NULL, decimal or binary changed")
	}
	orphan := b.Capture
	orphan.Canonical = false
	orphan.Finality = "orphaned"
	orphan.Revision++
	if e = c.WriteAcrossRevision(ctx, orphan); e != nil {
		t.Fatal(e)
	}
	if e = c.WriteAcrossRevision(ctx, b.Capture); e != nil {
		t.Fatal(e)
	}
	caps, e = c.AcrossCaptures(ctx, b.Capture.ManifestHash)
	if e != nil || len(caps) != 1 || caps[0].Canonical {
		t.Fatal("old revision revived")
	}
	if _, e = c.db.ExecContext(ctx, "DELETE FROM "+c.table("across_deposit")+" WHERE capture_id=?", []byte(b.Capture.CaptureId)); e != nil {
		t.Fatal(e)
	}
	if _, e = c.AcrossBatch(ctx, caps[0]); e == nil {
		t.Fatal("missing committed member accepted")
	}
}
func TestMySQLLSTTypedBatchAndRevision(t *testing.T) {
	c := familyClient(t, "crypto_market_info_lst")
	ctx := context.Background()
	b := lstStorageMarket()
	if e := b.Seal(); e != nil {
		t.Fatal(e)
	}
	for range 2 {
		if e := c.WriteLST(ctx, b); e != nil {
			t.Fatal(e)
		}
	}
	caps, e := c.LSTCaptures(ctx, b.Capture.ManifestHash)
	if e != nil || len(caps) != 1 {
		t.Fatal(e)
	}
	got, e := c.LSTBatch(ctx, caps[0])
	if e != nil {
		t.Fatal(e)
	}
	if lst.CanonicalHash(got) != lst.CanonicalHash(b) {
		t.Fatal("typed members changed")
	}
	orphan := b.Capture
	orphan.Canonical = false
	orphan.Finality = "orphaned"
	orphan.Revision++
	if e = c.WriteLSTRevision(ctx, orphan); e != nil {
		t.Fatal(e)
	}
	if e = c.WriteLST(ctx, b); e != nil {
		t.Fatal(e)
	}
	caps, e = c.LSTCaptures(ctx, b.Capture.ManifestHash)
	if e != nil || len(caps) != 1 || caps[0].Canonical {
		t.Fatal("old revision revived")
	}
	if _, e = c.db.ExecContext(ctx, "DELETE FROM "+c.table("lst_quote_observation")+" WHERE capture_id=?", b.Capture.CaptureId[:]); e != nil {
		t.Fatal(e)
	}
	if _, e = c.LSTBatch(ctx, caps[0]); e == nil {
		t.Fatal("missing committed member accepted")
	}
}

func reserveFixture(n uint64) reserve.Batch {
	now := dex.Now()
	h := dex.ObjectHash(n)
	m := dex.Digest([]byte("m"))
	batch := dex.ObjectHash(n + 100)
	cap := reserve.Capture{ChainId: 1, ManifestHash: reserve.Hash(m), CaptureId: reserve.Hash(h), BatchId: reserve.Hash(batch), CaptureKind: "snapshot", CaptureMode: "live", FromBlock: n, ToBlock: n, FromHash: reserve.Hash(h), ToHash: reserve.Hash(h), FromTime: now, ToTime: now, ReceivedAt: now, Canonical: true, Finality: "head", LogCoverage: "not_requested", StateCoverage: "partial", QuoteCoverage: "partial", ReceiptCoverage: "not_requested", PayloadHash: reserve.Hash(m), PlanHash: reserve.Hash(h)}
	num := new(big.Int).Lsh(big.NewInt(1), 200)
	token := reserve.Address(reserve.USDC)
	s := reserve.State{ChainId: 1, ManifestHash: cap.ManifestHash, CaptureId: cap.CaptureId, BatchId: cap.BatchId, BlockNumber: n, BlockHash: cap.ToHash, BlockTime: now, AvailableAt: now, PayloadHash: cap.PayloadHash, Folio: token, TotalSupplyRaw: new(big.Int).Set(num), Reason: "intentional_partial", Basket: []reserve.Asset{{Token: token, AmountRaw: new(big.Int).Set(num)}, {Token: token, AmountRaw: nil}}}
	q := reserve.Quote{ChainId: 1, ManifestHash: cap.ManifestHash, CaptureId: cap.CaptureId, BatchId: cap.BatchId, BlockNumber: n, BlockHash: cap.ToHash, BlockTime: now, AvailableAt: now, PayloadHash: cap.PayloadHash, Folio: token, RouteId: reserve.ID("r"), QuoteId: reserve.ID("q"), RouteKind: "mint", BudgetToken: token, RequestedBudgetRaw: num, TokenIn: token, TokenOut: token, Quality: "incomplete", Status: "failed", BasketAmounts: []reserve.Amount{{Token: token, AmountRaw: new(big.Int).Set(num)}}, DexLegs: []reserve.Leg{{LegIndex: 0, Side: "entry", QuoteMode: "exact_output", TokenIn: token, TokenOut: token, RequestedRaw: new(big.Int).Set(num), AmountInRaw: nil, AmountOutRaw: new(big.Int).Set(num), PathTokens: []string{}, PathPools: []string{}, PoolFees: []uint32{}, AvailableAt: now, PayloadHash: cap.PayloadHash, Status: "failed"}}, SharedPools: []string{}}
	cap.ExpectedStates = 1
	cap.ExpectedQuotes = 1
	b := reserve.Batch{Capture: cap, States: []reserve.State{s}, Quotes: []reserve.Quote{q}}
	b.Seal()
	return b
}

func dexTestBatch(n uint64) dex.Batch {
	now := dex.Now()
	a := dex.Anchor{ChainID: 1, Number: n, Hash: dex.ObjectHash(n), Manifest: dex.Digest([]byte("manifest")), Batch: dex.ObjectHash(n + 1000), Payload: dex.Digest([]byte("payload")), Time: now.Add(-time.Hour)}
	amount := new(big.Int).Lsh(big.NewInt(1), 200)
	q := dex.Quote{Anchor: a, Role: "strategy", Route: "r", Mode: "exact_in", Requested: amount, AmountIn: dex.Copy(amount), AmountOut: new(big.Int).Add(amount, big.NewInt(123)), DustDAI: new(big.Int), DustUSDS: new(big.Int), V3In: dex.Copy(amount), V3Out: dex.Copy(amount), SqrtAfter: big.NewInt(2), GasEstimate: big.NewInt(99), Status: "ok", AvailableAt: now}
	q.SetID()
	b := dex.Batch{Block: dex.Block{Anchor: a, Parent: dex.ObjectHash(n - 1), BaseFee: big.NewInt(123), ReceivedAt: now, Canonical: true, Finality: "head", Capture: "live", ExpectedQuotes: 1, QuoteCoverage: "complete", LogCoverage: "complete", ReceiptCoverage: "complete"}, Sky: &dex.Sky{Anchor: a, Module: "sky", Reason: "intentional_missing_state"}, Quotes: []dex.Quote{q}}
	b.Seal()
	return b
}

func keeperFixture(t *testing.T) keeper.Batch {
	t.Helper()
	var e error
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
	return b
}

func TestMySQLReserveTupleRevisionAndMissingMember(t *testing.T) {
	c := familyClient(t, "crypto_market_info_reserve")
	ctx := context.Background()
	b := reserveFixture(7)
	for n := 0; n < 2; n++ {
		if e := c.WriteReserveBatch(ctx, b); e != nil {
			t.Fatal(e)
		}
	}
	got, e := c.ReserveBatch(ctx, b.Capture)
	if e != nil || reserve.StateMembers(got.States) != b.Capture.StateMembers || got.States[0].Basket[1].AmountRaw != nil {
		t.Fatal("typed Reserve roundtrip", e)
	}
	revision := b.Capture
	revision.Revision++
	revision.Canonical = false
	if e = c.WriteReserveRevision(ctx, revision); e != nil {
		t.Fatal(e)
	}
	if e = c.WriteReserveRevision(ctx, b.Capture); e != nil {
		t.Fatal(e)
	}
	caps, e := c.ReserveCaptures(ctx, b.Capture.ManifestHash)
	if e != nil || len(caps) != 1 || caps[0].Canonical {
		t.Fatal("old revision overwrote orphan", e)
	}
	if _, e = c.db.ExecContext(ctx, "DELETE FROM "+c.table("reserve_route_quote")+" WHERE batch_id=?", []byte(b.Capture.BatchId)); e != nil {
		t.Fatal(e)
	}
	if _, e = c.ReserveBatch(ctx, revision); e == nil {
		t.Fatal("missing quote visible")
	}
}
func TestMySQLKeeperSevenTablesBinaryTupleAndFrozenRetry(t *testing.T) {
	c := familyClient(t, "crypto_market_info_justlend_keeper")
	ctx := context.Background()
	b := keeperFixture(t)
	for n := 0; n < 2; n++ {
		if e := c.WriteKeeperBatch(ctx, b); e != nil {
			t.Fatal(e)
		}
	}
	got, e := c.KeeperBatch(ctx, b.Capture)
	if e != nil || got.Events[0].AmountSun.Cmp(b.Events[0].AmountSun) != 0 || got.Receipts[0].NativeTransfers[0].AmountSun.Cmp(&b.Receipts[0].NativeTransfers[0].AmountSun) != 0 {
		t.Fatal("keeper typed roundtrip", e)
	}
	bad := b
	bad.Capture.AvailableAt = bad.Capture.AvailableAt.Add(time.Microsecond)
	if e = c.WriteKeeperBatch(ctx, bad); e == nil {
		t.Fatal("mutable keeper capture accepted")
	}
	if _, e = c.db.ExecContext(ctx, "DELETE FROM "+c.table("jl_keeper_cost_observation")+" WHERE capture_id=?", b.Capture.CaptureId[:]); e != nil {
		t.Fatal(e)
	}
	if _, e = c.KeeperBatch(ctx, b.Capture); e == nil {
		t.Fatal("missing cost visible")
	}
}
func TestMySQLDEXUInt256ReorgAndMemberProof(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	b := dexTestBatch(7)
	if e := c.WriteDEXBlock(ctx, b.Block); e == nil {
		t.Fatal("premature block marker accepted")
	}
	for n := 0; n < 2; n++ {
		if e := c.WriteDEXBatch(ctx, b); e != nil {
			t.Fatal(e)
		}
	}
	blocks, e := c.DEXBlocks(ctx, b.Block.Time.Add(-time.Minute), dex.Now())
	if e != nil || len(blocks) != 1 {
		t.Fatal("DEX block", e)
	}
	quotes, e := c.DEXQuotes(ctx, blocks)
	if e != nil || quotes[b.Block.Batch][0].AmountIn.Cmp(b.Quotes[0].AmountIn) != 0 {
		t.Fatal("DEX UInt256", e)
	}
	orphan := b.Block
	orphan.Canonical = false
	orphan.Finality = "orphaned"
	orphan.Revision++
	if e = c.WriteDEXBlock(ctx, orphan); e != nil {
		t.Fatal(e)
	}
	if e = c.WriteDEXBlock(ctx, b.Block); e != nil {
		t.Fatal(e)
	}
	last, e := c.DEXLastBlock(ctx, b.Block.Manifest, false)
	if e != nil || last != nil {
		t.Fatal("orphan revived", e)
	}
}
