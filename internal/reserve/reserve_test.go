package reserve

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
)

var testToken = dex.MustAddress("0x1111111111111111111111111111111111111111")
var testShare = dex.MustAddress("0x2222222222222222222222222222222222222222")

func testReader(t *testing.T, handler func(string, []json.RawMessage) (any, error)) *Reader {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var calls []struct {
			ID     uint64
			Method string
			Params []json.RawMessage
		}
		if json.NewDecoder(req.Body).Decode(&calls) != nil {
			t.Error("bad request")
		}
		out := []map[string]any{}
		for _, c := range calls {
			v, e := handler(c.Method, c.Params)
			row := map[string]any{"id": c.ID, "jsonrpc": "2.0"}
			if e != nil {
				row["error"] = map[string]any{"code": -32000, "message": e.Error()}
			} else {
				row["result"] = v
			}
			out = append(out, row)
		}
		_ = json.NewEncoder(w).Encode(out)
	}))
	t.Cleanup(srv.Close)
	rpc, e := ethereum.NewClient(srv.URL, t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	return NewReader(rpc, Manifest{})
}
func abiResult(name string, values ...any) string {
	b, e := ABI.Methods[name].Outputs.Pack(values...)
	if e != nil {
		panic(e)
	}
	return "0x" + common.Bytes2Hex(b)
}
func decodeCall(params []json.RawMessage) (string, []any) {
	var c struct{ To, Data string }
	if json.Unmarshal(params[0], &c) != nil {
		panic("bad call")
	}
	raw, e := ethereum.Bytes(c.Data)
	if e != nil {
		panic(e)
	}
	m, e := ABI.MethodById(raw[:4])
	if e != nil {
		panic(e)
	}
	v, e := m.Inputs.Unpack(raw[4:])
	if e != nil {
		panic(e)
	}
	return m.Name, v
}
func TestMintFeeCheckedArithmeticAndFloor(t *testing.T) {
	n, e := MintFee(Uint(10_000), Uint(0), Uint(1), Uint(2), Uint(0))
	if e != nil || n.Cmp(Uint(3)) != 0 {
		t.Fatal(n, e)
	}
	n, e = MintFee(Uint(10_001), Uint(1_000_000_000_000_000), Uint(1), Uint(2), Uint(0))
	if e != nil || n.Cmp(Uint(11)) != 0 {
		t.Fatal(n, e)
	}
	max := new(big.Int).Sub(new(big.Int).Lsh(Uint(1), 256), Uint(1))
	if _, e = MintFee(max, Uint(2), Uint(1), Uint(2), Uint(0)); e == nil {
		t.Fatal("checked mul overflow accepted")
	}
	if _, e = MulCeil(max, Uint(1), Uint(2)); e == nil {
		t.Fatal("checked ceil add overflow accepted")
	}
	if _, e = MintFee(Uint(1), Uint(1), Uint(1), Uint(0), Uint(0)); e == nil {
		t.Fatal("zero denominator accepted")
	}
}
func TestCanonicalBinaryIdentityAndGasBuckets(t *testing.T) {
	if ID("\xff") == ID("\xfe") {
		t.Fatal("invalid UTF8 identity collision")
	}
	quotes := []Quote{}
	for _, n := range []int64{100, 1000} {
		quotes = append(quotes, Quote{RouteKind: "cost_reference", TokenIn: Address(USDC), TokenOut: Address(WETH), Status: "ok", AmountInRaw: Uint(n * 2), AmountOutRaw: Uint(n)})
	}
	if cost, ok := GasBucket(quotes, Uint(101)); !ok || cost.Cmp(Uint(2000)) != 0 {
		t.Fatal("not upper bucket", cost, ok)
	}
	if _, ok := GasBucket(quotes, Uint(1001)); ok {
		t.Fatal("extrapolated absent bucket")
	}
}
func TestBatchedBasketFullVectorZeroLegFeeAndSameHash(t *testing.T) {
	h := dex.Digest([]byte("fixed block"))
	unit := Uint(1_000_000_000_000)
	r := testReader(t, func(method string, p []json.RawMessage) (any, error) {
		if method != "eth_call" {
			return nil, fmt.Errorf("unexpected %s", method)
		}
		var ref struct {
			BlockHash        string
			RequireCanonical bool
		}
		_ = json.Unmarshal(p[1], &ref)
		if ref.BlockHash != h.String() || !ref.RequireCanonical {
			t.Error("unpinned eth_call")
		}
		name, v := decodeCall(p)
		if name == "toAssets" {
			shares := v[0].(*big.Int)
			d := new(big.Int).Mul(unit, Uint(2))
			a := new(big.Int).Div(Copy(shares), d)
			if v[1].(uint8) == 1 {
				a = new(big.Int).Div(new(big.Int).Add(Copy(shares), new(big.Int).Sub(d, Uint(1))), d)
			}
			return abiResult(name, []common.Address{Eth(USDC), Eth(testToken), Eth(WETH)}, []*big.Int{a, Copy(shares), Uint(0)}), nil
		}
		path, amount := v[0].([]byte), v[1].(*big.Int)
		in, out := dex.Address(common.BytesToAddress(path[:20])), dex.Address(common.BytesToAddress(path[len(path)-20:]))
		n := Copy(amount)
		if name == "quoteExactInput" {
			if in == USDC && out == testShare {
				n.Mul(n, unit).Mul(n, Uint(100)).Div(n, Uint(101))
			} else if in == testShare && out == USDC {
				n.Mul(n, Uint(101)).Div(n, new(big.Int).Mul(unit, Uint(100)))
			} else {
				n.Div(n, new(big.Int).Mul(unit, Uint(2)))
			}
		} else {
			n.Add(n, new(big.Int).Sub(new(big.Int).Mul(unit, Uint(2)), Uint(1))).Div(n, new(big.Int).Mul(unit, Uint(2)))
		}
		return abiResult(name, n, []*big.Int{Uint(1)}, []uint32{0}, Uint(123)), nil
	})
	r.Manifest.Paths = []Path{{Tokens: []dex.Address{USDC, testToken}, Pools: []dex.Address{testToken}, Fees: []uint32{3000}}, {Tokens: []dex.Address{USDC, testShare}, Pools: []dex.Address{testShare}, Fees: []uint32{3000}}}
	s := State{ChainId: 1, ManifestHash: Hash(h), CaptureId: Hash(h), BatchId: Hash(h), BlockHash: Hash(h), BlockNumber: 99, BlockTime: dex.Now(), Folio: Address(testShare), IdentityOk: true, StateComplete: true, TotalSupplyRaw: Uint(1), Deprecated: Ptr(false), SyncStateChangeActive: Ptr(false), AsyncStateChangeActive: Ptr(false), MintFeeD18: Uint(0), DaoFeeNumerator: Uint(1), DaoFeeDenominator: Uint(2), DaoFeeFloorD18: Uint(0), Basket: []Asset{{Token: Address(USDC)}, {Token: Address(testToken)}, {Token: Address(WETH)}}}
	qs := r.BasketQuotes(context.Background(), s, []*big.Int{Uint(5_000_000_000), Uint(20_000_000_000), Uint(50_000_000_000)})
	if len(qs) != 6 {
		t.Fatal(len(qs))
	}
	for i := range qs {
		q := &qs[i]
		if e := r.Finish(q, q.Reason); e != nil {
			t.Fatal(e)
		}
		if q.Quality != "quoted_complete" || len(q.BasketAmounts) != 3 || len(q.DexLegs) != 3 || q.ExpectedLegs != 3 {
			t.Fatalf("bad vector/quality %+v", q)
		}
		if q.RouteKind == "mint" {
			expected, e := MintFee(q.GrossSharesRaw, s.MintFeeD18, s.DaoFeeNumerator, s.DaoFeeDenominator, s.DaoFeeFloorD18)
			if e != nil || q.FeeSharesRaw.Cmp(expected) != 0 {
				t.Fatal("bad mint fee")
			}
			if q.AmountInRaw.Cmp(q.RequestedBudgetRaw) > 0 {
				t.Fatal("mint over budget")
			}
		}
	}
	s.Deprecated = Ptr(true)
	if !Usable(s) || MintAllowed(s) {
		t.Fatal("deprecated permissions")
	}
	s.AsyncStateChangeActive = Ptr(true)
	if Usable(s) {
		t.Fatal("trusted fill state accepted")
	}
}
func TestBestCandidateSurvivesZeroAlternativeAndOverlap(t *testing.T) {
	r := testReader(t, func(_ string, p []json.RawMessage) (any, error) {
		name, v := decodeCall(p)
		path := v[0].([]byte)
		n := Uint(123)
		if path[22] == 2 {
			n = Uint(0)
		}
		return abiResult(name, n, []*big.Int{Uint(1)}, []uint32{0}, Uint(10)), nil
	})
	r.Manifest.Paths = []Path{{Tokens: []dex.Address{USDC, testToken}, Pools: []dex.Address{testShare}, Fees: []uint32{1}}, {Tokens: []dex.Address{USDC, testToken}, Pools: []dex.Address{WETH}, Fees: []uint32{2}}}
	legs := r.BestLegs(context.Background(), dex.Digest([]byte("b")), []Leg{NewLeg(0, "entry", "exact_input", Address(USDC), Address(testToken), Uint(10))})
	if legs[0].Status != "ok" || legs[0].AmountOutRaw.Cmp(Uint(123)) != 0 {
		t.Fatal("successful candidate lost", legs)
	}
	q := Quote{BatchId: Hash(dex.Digest([]byte("batch"))), RouteId: ID("r"), RequestedBudgetRaw: Uint(10), AmountInRaw: Uint(10), AmountOutRaw: Uint(20), ExpectedLegs: 2, DexLegs: []Leg{legs[0], legs[0]}}
	if e := r.Finish(&q, ""); e != nil || q.Quality != "indicative_overlap" {
		t.Fatal(q.Quality, e)
	}
}

type memoryStore struct {
	caps      []Capture
	revisions []Capture
}
type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func (s *memoryStore) ReserveCaptures(context.Context, string) ([]Capture, error) { return s.caps, nil }
func (s *memoryStore) WriteReserveRevision(_ context.Context, c Capture) error {
	s.revisions = append(s.revisions, c)
	return nil
}
func (s *memoryStore) WriteReserveBatch(context.Context, Batch) error       { return nil }
func (s *memoryStore) ReserveBatch(context.Context, Capture) (Batch, error) { return Batch{}, nil }
func (s *memoryStore) ReserveReceipt(context.Context, dex.Anchor, dex.Hash) (dex.Receipt, bool, error) {
	return dex.Receipt{}, false, nil
}
func TestReorgInvalidatesAllAttemptsAndRewindsCursor(t *testing.T) {
	old, newHash := dex.Digest([]byte("old")), dex.Digest([]byte("new"))
	r := testReader(t, func(_ string, _ []json.RawMessage) (any, error) {
		return map[string]any{"number": "0x7", "hash": newHash.String(), "parentHash": old.String(), "timestamp": "0x64", "baseFeePerGas": "0x1", "miner": testToken.String()}, nil
	})
	store := &memoryStore{}
	for i := 0; i < 3; i++ {
		store.caps = append(store.caps, Capture{FromBlock: 7, ToBlock: 7, FromHash: Hash(old), ToHash: Hash(old), Canonical: true, Finality: "head", Revision: uint64(i + 1), Committed: i != 2})
	}
	c := Collector{RPC: r.RPC, Store: store}
	if e := c.Reconcile(context.Background(), dex.Block{Anchor: dex.Anchor{Number: 5}}, dex.Block{Anchor: dex.Anchor{Number: 6}}); e != nil {
		t.Fatal(e)
	}
	if len(store.revisions) != 3 || c.Rewind != 7 {
		t.Fatal("missed attempt or cursor", len(store.revisions), c.Rewind)
	}
	for _, v := range store.revisions {
		if v.Canonical {
			t.Fatal("old branch revived")
		}
	}
	store.caps[0].Finality = "finalized"
	if e := c.Reconcile(context.Background(), dex.Block{Anchor: dex.Anchor{Number: 8}}, dex.Block{Anchor: dex.Anchor{Number: 8}}); e == nil {
		t.Fatal("finalized hash conflict ignored")
	}
}
func TestAuctionRunningPastRebalanceDeadline(t *testing.T) {
	now := time.Now().UTC()
	s := State{IdentityOk: true, StateComplete: true, Basket: []Asset{{Token: Address(testToken)}}, TotalSupplyRaw: Uint(1), SyncStateChangeActive: Ptr(false), AsyncStateChangeActive: Ptr(false), Deprecated: Ptr(false), BlockTime: now, AuctionId: Uint(1), AuctionStartTime: Ptr(uint64(now.Unix() - 60)), AuctionEndTime: Ptr(uint64(now.Unix() + 60)), RebalanceAvailableUntil: Ptr(uint64(now.Unix() - 1)), RebalanceBidsEnabled: Ptr(true), RebalanceNonce: Uint(1), AuctionRebalanceNonce: Uint(1)}
	if !AuctionActive(s) {
		t.Fatal("ended rebalance cut live auction")
	}
	s.RebalanceBidsEnabled = Ptr(false)
	if AuctionActive(s) {
		t.Fatal("permissionless disabled ignored")
	}
}

func TestBatchedAuctionSixPairsCapacityAndUnknownRPC(t *testing.T) {
	unit := Uint(1_000_000_000_000)
	s := State{ChainId: 1, ManifestHash: ID("m"), CaptureId: ID("c"), BatchId: ID("b"), BlockHash: ID("h"), BlockNumber: 101, BlockTime: dex.Now(), Folio: Address(testShare), IdentityOk: true, StateComplete: true, TotalSupplyRaw: Uint(1), Deprecated: Ptr(false), SyncStateChangeActive: Ptr(false), AsyncStateChangeActive: Ptr(false), AuctionId: Uint(1), AuctionStartTime: Ptr(uint64(1)), AuctionEndTime: Ptr(uint64(time.Now().Unix() + 1000)), RebalanceBidsEnabled: Ptr(true), RebalanceNonce: Uint(1), AuctionRebalanceNonce: Uint(1)}
	r := testReader(t, func(_ string, p []json.RawMessage) (any, error) {
		name, v := decodeCall(p)
		if name == "getBid" {
			sell, buy := Addr(v[1].(common.Address)), Addr(v[2].(common.Address))
			requested := v[3].(*big.Int)
			su, bu := Copy(unit), Copy(unit)
			if sell == USDC {
				su = Uint(1)
			}
			if buy == USDC {
				bu = Uint(1)
			}
			capacity := new(big.Int).Mul(Uint(100_000_000), su)
			if requested.Cmp(capacity) < 0 {
				capacity = Copy(requested)
			}
			bid := new(big.Int).Mul(capacity, bu)
			bid.Div(bid, new(big.Int).Mul(su, Uint(2)))
			return abiResult(name, capacity, bid, Uint(1)), nil
		}
		path, n := v[0].([]byte), Copy(v[1].(*big.Int))
		in, out := dex.Address(common.BytesToAddress(path[:20])), dex.Address(common.BytesToAddress(path[len(path)-20:]))
		if name == "quoteExactInput" && in == USDC {
			n.Mul(n, unit)
		} else if name == "quoteExactInput" && out == USDC {
			n.Div(n, unit)
		} else {
			n.Div(n, unit)
		}
		return abiResult(name, n, []*big.Int{Uint(1)}, []uint32{0}, Uint(1)), nil
	})
	s.Basket = append(s.Basket, Asset{Token: Address(USDC)})
	for i := byte(1); i <= 5; i++ {
		var a dex.Address
		a[19] = i
		s.Basket = append(s.Basket, Asset{Token: Address(a)})
		r.Manifest.Paths = append(r.Manifest.Paths, Path{Tokens: []dex.Address{USDC, a}, Pools: []dex.Address{a}, Fees: []uint32{3000}})
	}
	quotes, skipped := r.AuctionQuotes(context.Background(), s, []*big.Int{Uint(5_000_000_000), Uint(20_000_000_000), Uint(50_000_000_000)})
	if len(quotes) != 18 || skipped != 72 {
		t.Fatal("unbounded pair expansion", len(quotes), skipped)
	}
	for i := range quotes {
		q := &quotes[i]
		if e := r.Finish(q, q.Reason); e != nil {
			t.Fatal(e)
		}
		if q.Quality != "quoted_complete" || q.AmountInRaw.Cmp(q.RequestedBudgetRaw) > 0 || q.RequestedMaxSellRaw == nil || q.AuctionSellRaw.Sign() == 0 {
			t.Fatal("bad capped auction", q.Reason, q.Quality)
		}
	}
	bad := testReader(t, func(_ string, _ []json.RawMessage) (any, error) { return nil, fmt.Errorf("upstream unavailable") })
	s.Basket = s.Basket[:2]
	qs, _ := bad.AuctionQuotes(context.Background(), s, []*big.Int{Uint(1)})
	if len(qs) != 2 {
		t.Fatal("node errors became zero pairs", len(qs))
	}
	for _, q := range qs {
		if q.Reason == "" {
			t.Fatal("missing unknown reason")
		}
	}
}

func TestMixedRPCErrorGroupRemainsUnknown(t *testing.T) {
	rpc, e := ethereum.NewClient("https://example.invalid", t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	raw := []byte(`[{"id":1,"error":{"code":3,"message":"execution reverted"}},{"id":2,"error":{"code":-32000,"message":"upstream unavailable"}}]`)
	h, e := rpc.Archive.PutObject(struct{ Response []byte }{raw})
	if e != nil {
		t.Fatal(e)
	}
	if SourceError(rpc, ethereum.Result{Err: errors.New("rpc_method_error"), Payload: h, Raw: raw}) == "contract_revert" {
		t.Fatal("upstream failure misclassified as pair zero")
	}
}

func TestEventsTriggerAndPendingAuctionMonitoring(t *testing.T) {
	if !SnapshotEvent([]dex.Log{{Event: "AuctionOpened"}}) || !SnapshotEvent([]dex.Log{{Event: "unknown_upgrade_block"}}) || SnapshotEvent([]dex.Log{{Event: "Transfer"}}) {
		t.Fatal("incorrect event trigger")
	}
	now := time.Now()
	s := State{IdentityOk: true, StateComplete: true, Basket: []Asset{{Token: Address(testToken)}}, TotalSupplyRaw: Uint(1), SyncStateChangeActive: Ptr(false), AsyncStateChangeActive: Ptr(false), Deprecated: Ptr(false), BlockTime: now, AuctionId: Uint(1), AuctionStartTime: Ptr(uint64(now.Unix() + 30)), AuctionEndTime: Ptr(uint64(now.Unix() + 100)), RebalanceBidsEnabled: Ptr(true), RebalanceNonce: Uint(1), AuctionRebalanceNonce: Uint(1)}
	if !AuctionMonitor(s) || AuctionActive(s) {
		t.Fatal("pending auction not monitored or quoted prematurely")
	}
}

func TestTwoFoliosKeepDistinctQuoteIdentity(t *testing.T) {
	rpc, e := ethereum.NewClient("https://example.invalid", t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	r := NewReader(rpc, Manifest{})
	a := Quote{BatchId: ID("batch"), Folio: Address(testToken), RouteId: ID("mint"), RequestedBudgetRaw: Uint(1)}
	b := a
	b.Folio = Address(testShare)
	if e = r.Finish(&a, "missing"); e != nil {
		t.Fatal(e)
	}
	if e = r.Finish(&b, "missing"); e != nil {
		t.Fatal(e)
	}
	if a.QuoteId == b.QuoteId {
		t.Fatal("cross-folio quote collision")
	}
}

func TestHeaderFailureCannotReviseFinalityOrCursor(t *testing.T) {
	r := testReader(t, func(_ string, _ []json.RawMessage) (any, error) { return nil, fmt.Errorf("temporarily unavailable") })
	store := &memoryStore{caps: []Capture{{Canonical: true, Finality: "head", FromBlock: 7, ToBlock: 7}}}
	r.RPC.HTTP.Transport = transportFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("temporary network failure") })
	c := Collector{RPC: r.RPC, Store: store}
	e := c.Reconcile(context.Background(), dex.Block{Anchor: dex.Anchor{Number: 10}}, dex.Block{Anchor: dex.Anchor{Number: 10}})
	if e == nil || !TransientRPC(e) || len(store.revisions) > 0 || c.Rewind != 0 {
		t.Fatal("failed header revised facts or cursor", e, store.revisions)
	}
	if TransientRPC(errors.New("finalized_hash_conflict")) {
		t.Fatal("finalized hash conflict treated as transport retry")
	}
	for _, s := range []string{"rpc_batch_invalid_ids_or_envelope", "evidence_write_failed", "rpc_http_403", "rpc_method_error"} {
		if TransientRPC(errors.New(s)) {
			t.Fatal("persistent or integrity error retried", s)
		}
	}
}
