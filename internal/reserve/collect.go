package reserve

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
)

type Store interface {
	WriteReserveBatch(context.Context, Batch) error
	WriteReserveRevision(context.Context, Capture) error
	ReserveCaptures(context.Context, string) ([]Capture, error)
	ReserveBatch(context.Context, Capture) (Batch, error)
	ReserveReceipt(context.Context, dex.Anchor, dex.Hash) (dex.Receipt, bool, error)
}
type Collector struct {
	RPC              *ethereum.Client
	Manifest         Manifest
	Store            Store
	Rewind           uint64
	StartBlock       uint64
	RelatedManifests []dex.Hash
	SnapshotBudget   time.Duration
	SimulateEvery    time.Duration
}

func (c *Collector) Header(ctx context.Context, tag string) (dex.Block, error) {
	b, e := c.RPC.Header(ctx, tag)
	b.Manifest = c.Manifest.Hash
	return b, e
}
func (c *Collector) capture(from, to dex.Block, kind, mode, finality string) Capture {
	var nonce [32]byte
	if _, e := rand.Read(nonce[:]); e != nil {
		panic(e)
	}
	return Capture{ChainId: 1, ManifestHash: Hash(c.Manifest.Hash), CaptureId: ID(struct {
		Manifest   dex.Hash
		Kind, Mode string
		From, To   uint64
		FH, TH     dex.Hash
	}{c.Manifest.Hash, kind, mode, from.Number, to.Number, from.Hash, to.Hash}), BatchId: Hash(dex.Digest(nonce[:])), CaptureKind: kind, CaptureMode: mode, FromBlock: from.Number, FromHash: Hash(from.Hash), ToBlock: to.Number, ToHash: Hash(to.Hash), FromTime: from.Time, ToTime: to.Time, ReceivedAt: minTime(from.ReceivedAt, to.ReceivedAt), Canonical: true, Finality: finality, LogCoverage: "not_requested", StateCoverage: "not_requested", QuoteCoverage: "not_requested", ReceiptCoverage: "not_requested"}
}
func (c *Collector) Snapshot(ctx context.Context, b dex.Block, mode string) (Batch, error) {
	cap := c.capture(b, b, "snapshot", mode, "head")
	out := Batch{Capture: cap}
	r := NewReader(c.RPC, c.Manifest)
	r.Members = append(r.Members, b.Payload)
	budget := c.SnapshotBudget
	if budget == 0 {
		budget = 35 * time.Second
	}
	work, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	out.Capture.ExpectedStates = uint32(len(c.Manifest.Folios))
	out.Capture.StateCoverage = "complete"
	out.Capture.QuoteCoverage = "complete"
	for _, f := range c.Manifest.Folios {
		s := r.State(work, b, cap, f.Address)
		if s.Reason == "evidence_write_failed" {
			return out, errors.New(s.Reason)
		}
		if !s.StateComplete {
			out.Capture.StateCoverage = "partial"
		}
		out.States = append(out.States, s)
	}
	// Gas replacement is USDC -> WETH exact-output. USDT conversions are both
	// directions; their notional is a separate finite cost-reference bucket.
	ref := stateBase(cap, dex.Address{}, b)
	legs := []Leg{}
	names := []string{}
	for _, n := range []int64{100_000_000_000, 1_000_000_000_000} {
		for _, pair := range [][2]dex.Address{{USDT, USDC}, {USDC, USDT}} {
			legs = append(legs, NewLeg(uint16(len(legs)), "entry", "exact_input", Address(pair[0]), Address(pair[1]), Uint(n)))
			names = append(names, "conversion:"+pair[0].String()+":"+pair[1].String())
		}
	}
	for _, n := range []int64{1_000_000_000_000_000, 10_000_000_000_000_000, 100_000_000_000_000_000} {
		legs = append(legs, NewLeg(uint16(len(legs)), "entry", "exact_output", Address(USDC), Address(WETH), Uint(n)))
		names = append(names, fmt.Sprintf("gas_replacement:%d", n))
	}
	legs = r.BestLegs(work, b.Hash, legs)
	for i, l := range legs {
		q := quoteBase(ref, "cost_reference", l.RequestedRaw, names[i])
		q.Folio = string(make([]byte, 20))
		q.TokenIn = l.TokenIn
		q.TokenOut = l.TokenOut
		q.BudgetToken = l.TokenIn
		q.ExpectedLegs = 1
		AddLegs(&q, []Leg{l})
		reason := ""
		if !CompleteLegs([]Leg{l}) {
			reason = "reference_failed"
		} else {
			q.AmountInRaw = Copy(l.AmountInRaw)
			q.AmountOutRaw = Copy(l.AmountOutRaw)
			if l.QuoteMode == "exact_output" {
				q.RequestedBudgetRaw = Copy(l.AmountInRaw)
			}
		}
		if e := r.Finish(&q, reason); e != nil {
			return out, e
		}
		out.Quotes = append(out.Quotes, q)
	}
	budgets := []*big.Int{}
	for _, s := range c.Manifest.Budgets {
		n, _ := new(big.Int).SetString(s, 10)
		budgets = append(budgets, n)
	}
	pairsRemaining := 6
	for _, s := range out.States {
		basket := r.BasketQuotes(work, s, budgets)
		for i := range basket {
			q := &basket[i]
			if e := r.Finish(q, q.Reason); e != nil {
				return out, e
			}
			out.Quotes = append(out.Quotes, *q)
		}
		auctions, skipped := r.AuctionQuotes(work, s, budgets, pairsRemaining)
		pairsRemaining -= len(auctions) / len(budgets)
		out.Capture.SkippedRoutes += skipped
		for i := range auctions {
			q := &auctions[i]
			if e := r.Finish(q, q.Reason); e != nil {
				return out, e
			}
			out.Quotes = append(out.Quotes, *q)
		}
	}
	out.Capture.ExpectedQuotes = uint32(len(out.Quotes)) + out.Capture.SkippedRoutes
	if out.Capture.SkippedRoutes > 0 {
		out.Capture.QuoteCoverage = "partial"
	}
	for _, q := range out.Quotes {
		if q.Quality == "incomplete" {
			out.Capture.QuoteCoverage = "partial"
		}
	}
	out.Capture.PlanHash = ID(struct {
		Folios   []Folio
		Budgets  []string
		Expected uint32
	}{c.Manifest.Folios, c.Manifest.Budgets, out.Capture.ExpectedQuotes})
	h, e := r.Proof(struct {
		Block dex.Hash
		Mode  string
		Used  int
	}{b.Hash, mode, r.Used})
	if e != nil {
		return out, e
	}
	out.Capture.PayloadHash = h
	if e = c.RPC.Canonical(ctx, b); e != nil {
		return out, e
	}
	out.Seal()
	return out, Validate(out)
}

type rawLog struct {
	BlockNumber, BlockHash, TransactionHash, TransactionIndex, LogIndex, Address, Data string
	Topics                                                                             []string
	Removed                                                                            *bool
}

var events = func() map[dex.Hash]string {
	m := map[dex.Hash]string{}
	for name, sig := range map[string]string{"AuctionBid": "AuctionBid(uint256,address,address,uint256,uint256)", "AuctionClosed": "AuctionClosed(uint256)", "RebalanceEnded": "RebalanceEnded(uint256)", "BidsEnabledSet": "BidsEnabledSet(bool)", "Transfer": "Transfer(address,address,uint256)", "BasketTokenAdded": "BasketTokenAdded(address)", "BasketTokenRemoved": "BasketTokenRemoved(address)", "MintFeeSet": "MintFeeSet(uint256)", "FolioDeprecated": "FolioDeprecated()", "Upgraded": "Upgraded(address)", "AuctionOpened": "AuctionOpened(uint256,uint256,address[],(uint256,uint256,uint256)[],(uint256,uint256)[],(uint256,uint256,uint256),uint256,uint256)"} {
		m[dex.Hash(crypto.Keccak256Hash([]byte(sig)))] = name
	}
	return m
}()

func parseLog(v rawLog, a dex.Anchor, allowed map[dex.Address]bool) (dex.Log, error) {
	l := dex.Log{Anchor: a, Event: "unknown"}
	n, e := ethereum.Quantity(v.BlockNumber, 64)
	if e != nil || n.Uint64() != a.Number {
		return l, errors.New("log_height")
	}
	h, e := dex.ParseHash(v.BlockHash)
	if e != nil || h != a.Hash {
		return l, errors.New("log_hash")
	}
	l.TxHash, e = dex.ParseHash(v.TransactionHash)
	if e != nil {
		return l, e
	}
	tx, e := ethereum.Quantity(v.TransactionIndex, 32)
	if e != nil {
		return l, e
	}
	ix, e := ethereum.Quantity(v.LogIndex, 32)
	if e != nil {
		return l, e
	}
	l.TxIndex = uint32(tx.Uint64())
	l.Index = uint32(ix.Uint64())
	l.Emitter, e = dex.ParseAddress(v.Address)
	if e != nil || !allowed[l.Emitter] {
		return l, errors.New("log_emitter")
	}
	if v.Removed == nil || *v.Removed {
		return l, errors.New("removed_or_unknown_log")
	}
	l.Data, e = ethereum.Bytes(v.Data)
	if e != nil {
		return l, e
	}
	if len(v.Topics) > 4 {
		return l, errors.New("log_topics")
	}
	for _, s := range v.Topics {
		h, e := dex.ParseHash(s)
		if e != nil {
			return l, e
		}
		l.Topics = append(l.Topics, h)
	}
	if len(l.Topics) > 0 {
		if name, ok := events[l.Topics[0]]; ok {
			l.Event = name
		}
	}
	return l, nil
}
func (c *Collector) Logs(ctx context.Context, from, to dex.Block, mode, finality string) (Batch, error) {
	out := Batch{Capture: c.capture(from, to, "logs", mode, finality)}
	r := NewReader(c.RPC, c.Manifest)
	r.Limit = 10000
	r.Members = append(r.Members, from.Payload, to.Payload)
	out.Capture.LogCoverage = "missing"
	out.Capture.ReceiptCoverage = "missing"
	allowed := map[dex.Address]bool{}
	addresses := []string{}
	for _, f := range c.Manifest.Folios {
		allowed[f.Address] = true
		addresses = append(addresses, f.Address.String())
	}
	result := r.Batch(ctx, []ethereum.Call{{Method: "eth_getLogs", Params: []any{map[string]any{"fromBlock": ethereum.Height(from.Number), "toBlock": ethereum.Height(to.Number), "address": addresses}}}})[0]
	var rows []rawLog
	var failure error
	if result.Err != nil {
		failure = errors.New(SourceError(c.RPC, result))
	} else if string(result.Raw) == "null" || json.Unmarshal(result.Raw, &rows) != nil {
		failure = errors.New("logs_encoding")
	}
	if failure == nil {
		nums := []uint64{}
		seen := map[uint64]bool{}
		for _, v := range rows {
			n, e := ethereum.Quantity(v.BlockNumber, 64)
			if e != nil || n.Uint64() < from.Number || n.Uint64() > to.Number {
				return out, errors.New("logs_outside_range")
			}
			if !seen[n.Uint64()] {
				nums = append(nums, n.Uint64())
				seen[n.Uint64()] = true
			}
		}
		headers, e := c.Headers(ctx, nums)
		if e != nil {
			return out, e
		}
		duplicates := map[string]bool{}
		for _, v := range rows {
			n, _ := ethereum.Quantity(v.BlockNumber, 64)
			b := headers[n.Uint64()]
			r.Members = append(r.Members, b.Payload)
			a := b.Anchor
			a.Manifest = c.Manifest.Hash
			copy(a.Batch[:], out.Capture.BatchId)
			l, e := parseLog(v, a, allowed)
			if e != nil {
				return out, e
			}
			key := l.Hash.String() + l.TxHash.String() + fmt.Sprint(l.Index)
			if duplicates[key] {
				return out, errors.New("duplicate_log")
			}
			duplicates[key] = true
			l.Payload, e = c.RPC.Archive.PutObject(v)
			if e != nil {
				return out, e
			}
			out.Logs = append(out.Logs, l)
		}
		sort.Slice(out.Logs, func(i, j int) bool {
			if out.Logs[i].Number != out.Logs[j].Number {
				return out.Logs[i].Number < out.Logs[j].Number
			}
			return out.Logs[i].Index < out.Logs[j].Index
		})
		out.Capture.LogCoverage = "complete"
		// A block-end version read cannot determine intra-block upgrade ordering.
		// Such blocks retain raw logs but get an explicit unknown event label.
		type key struct {
			Number uint64
			Folio  dex.Address
		}
		keys := []key{}
		seenKey := map[key]bool{}
		upgraded := map[key]bool{}
		for _, l := range out.Logs {
			k := key{l.Number, l.Emitter}
			if !seenKey[k] {
				keys = append(keys, k)
				seenKey[k] = true
			}
			if l.Event == "Upgraded" {
				upgraded[k] = true
			}
		}
		calls := []ethereum.Call{}
		for _, k := range keys {
			bh := headers[k.Number].Hash
			calls = append(calls, ethereum.Call{Method: "eth_getStorageAt", Params: []any{k.Folio.String(), ImplementationSlot, ethereum.BlockRef(bh)}}, Call(k.Folio, bh, "version"))
		}
		versions := r.Batch(ctx, calls)
		known := map[key]bool{}
		for i, k := range keys {
			slot, e := ethereum.HexResult(versions[2*i])
			v, ve := Decode(versions[2*i+1], "version")
			known[k] = e == nil && len(slot) == 32 && string(slot[12:]) == Address(c.Manifest.Implementation) && ve == nil && v[0].(string) == "5.0.0" && !upgraded[k]
		}
		for i := range out.Logs {
			k := key{out.Logs[i].Number, out.Logs[i].Emitter}
			if upgraded[k] {
				out.Logs[i].Event = "unknown_upgrade_block"
			} else if !known[k] {
				out.Logs[i].Event = "unknown_version"
			}
		}
		// Fetch/validate full receipts in memory, then retain typed logs/calldata.
		dataStore, ok := c.Store.(ReceiptDataStore)
		if !ok && len(out.Logs) > 0 {
			return out, errors.New("receipt_data_store_unavailable")
		}
		seenTx := map[dex.Hash]bool{}
		byTx := map[dex.Hash][]dex.Log{}
		for _, l := range out.Logs {
			byTx[l.TxHash] = append(byTx[l.TxHash], l)
		}
		for _, l := range out.Logs {
			if seenTx[l.TxHash] {
				continue
			}
			seenTx[l.TxHash] = true
			out.Capture.ExpectedReceipts++
			existing, ok, e := c.Store.ReserveReceipt(ctx, l.Anchor, l.TxHash)
			if e != nil {
				return out, e
			}
			if ok {
				data, present, e := dataStore.ReserveReceiptData(ctx, existing.Anchor, existing.TxHash)
				if e != nil {
					return out, e
				}
				if !present {
					return out, errors.New("receipt_data_migration_required")
				}
				if e = ReceiptDataContains(data, existing, byTx[l.TxHash]); e != nil {
					return out, e
				}
				out.Receipts = append(out.Receipts, existing)
				out.ReceiptData = append(out.ReceiptData, data)
				continue
			}
			response := r.Batch(ctx, []ethereum.Call{{Method: "eth_getTransactionByHash", Params: []any{l.TxHash.String()}}, {Method: "eth_getTransactionReceipt", Params: []any{l.TxHash.String()}}})
			receipt, e := c.RPC.DecodeReceipt(l.Anchor, l.TxHash, l.TxIndex, response[0], response[1])
			if e != nil {
				failure = e
				continue
			}
			var transaction struct{ Input string }
			if json.Unmarshal(response[0].Raw, &transaction) != nil {
				return out, errors.New("receipt_transaction_encoding")
			}
			calldata, e := ethereum.Bytes(transaction.Input)
			if e != nil {
				return out, e
			}
			data, e := DecodeReceiptData(receipt, response[1].Raw, calldata)
			if e != nil {
				return out, e
			}
			if e = ReceiptDataContains(data, receipt, byTx[l.TxHash]); e != nil {
				return out, e
			}
			out.Receipts = append(out.Receipts, receipt)
			out.ReceiptData = append(out.ReceiptData, data)
		}
		out.Capture.ReceiptCoverage = "complete"
		if len(out.Receipts) != int(out.Capture.ExpectedReceipts) {
			out.Capture.ReceiptCoverage = "partial"
		}
	}
	for _, l := range out.Logs {
		r.Members = append(r.Members, l.Payload)
	}
	for _, receipt := range out.Receipts {
		r.Members = append(r.Members, receipt.Payload, receipt.ReceiptHash, receipt.CalldataHash)
	}
	if failure != nil {
		out.Capture.Reason = failure.Error()
	}
	proof, e := r.Proof(struct {
		From, To dex.Hash
		Reason   string
	}{from.Hash, to.Hash, out.Capture.Reason})
	if e != nil {
		return out, e
	}
	out.Capture.PayloadHash = proof
	out.Capture.PlanHash = ID(addresses)
	if e = c.RPC.Canonical(ctx, from); e != nil {
		return out, e
	}
	if to.Number != from.Number {
		if e = c.RPC.Canonical(ctx, to); e != nil {
			return out, e
		}
	}
	out.Seal()
	return out, Validate(out)
}

func minTime(a, b time.Time) time.Time {
	if a.IsZero() {
		return b
	}
	if b.IsZero() || a.Before(b) {
		return a
	}
	return b
}
