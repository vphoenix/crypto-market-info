package ethereum

import (
	"context"
	"errors"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/dexarb"
	"math/big"
	"sync"
	"time"
)

func (c *Client) Collect(ctx context.Context, header dex.Block, live bool) (dex.Batch, error) {
	mode := "backfill"
	if live {
		mode = "live"
	}
	return c.CollectMode(ctx, header, mode)
}
func (c *Client) CollectMode(ctx context.Context, header dex.Block, mode string) (dex.Batch, error) {
	if mode != "live" && mode != "backfill" && mode != "research" {
		return dex.Batch{}, errors.New("invalid capture mode")
	}
	live := mode != "backfill"
	b := dex.Batch{Block: header}
	b.Block.Capture = mode
	b.Block.Batch = dex.ObjectHash(struct {
		Hash, Manifest dex.Hash
		At             time.Time
		Mode           string
	}{header.Hash, header.Manifest, header.ReceivedAt, b.Block.Capture})
	a := b.Block.Anchor
	work, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	m := DefaultManifest()
	var logsHash dex.Hash
	var logErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); b.Logs, logsHash, logErr = c.Logs(work, a) }()
	if live {
		sky := c.ReadSky(work, a)
		b.Sky = &sky
		b.Quotes = dexarb.Scan(work, c, a, sky, m.Routes(), m.Sizes(), m.Addresses["USDC"])
		reqs := []dex.SwapRequest{{TokenIn: m.Addresses["WETH"], TokenOut: m.Addresses["USDC"], Fee: 500, Amount: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)}, {TokenIn: m.Addresses["USDT"], TokenOut: m.Addresses["USDC"], Fee: 100, Amount: big.NewInt(1_000_000_000_000)}}
		var rr []dex.SwapResult
		if sky.IdentityOK {
			rr = c.Quotes(work, a.Hash, reqs)
		}
		for i, req := range reqs {
			role := "gas_reference"
			if i == 1 {
				role = "capital_entry"
			}
			q := dex.Quote{Anchor: a, Role: role, Route: role, Mode: "exact_in", TokenIn: req.TokenIn, TokenOut: req.TokenOut, Requested: req.Amount, V3InToken: req.TokenIn, V3OutToken: req.TokenOut, V3In: req.Amount, Status: "unknown", Reason: "identity_or_rpc_missing", AvailableAt: dex.Now()}
			q.Payload = sky.Payload
			q.SetID()
			if i < len(rr) {
				v := rr[i]
				if v.Payload != (dex.Hash{}) {
					q.Payload = v.Payload
				}
				q.AvailableAt = v.At
				if v.Err != nil {
					q.Reason = v.Err.Error()
				} else {
					q.Status = "ok"
					q.Reason = ""
					q.AmountIn = dex.Copy(req.Amount)
					q.AmountOut = v.Amount
					q.V3Out = v.Amount
					q.SqrtAfter = v.SqrtAfter
					q.GasEstimate = v.Gas
					q.TicksCrossed = v.Ticks
				}
			}
			b.Quotes = append(b.Quotes, q)
		}
		b.Block.ExpectedQuotes = 58
		b.Block.QuoteCoverage = "complete"
		for _, q := range b.Quotes {
			if q.Status != "ok" {
				b.Block.QuoteCoverage = "partial"
				break
			}
		}
	}
	wg.Wait()
	if b.Sky != nil && b.Sky.Payload == (dex.Hash{}) {
		return b, errors.New("sky_evidence_unavailable")
	}
	if logErr == nil {
		b.Block.LogCoverage = "complete"
		var e error
		b.Receipts, e = c.Receipts(work, a, b.Logs)
		if e == nil {
			b.Block.ReceiptCoverage = "complete"
		} else {
			b.Block.ReceiptCoverage = "partial"
		}
	}
	// An unavailable RPC member still has a reproducible failure observation;
	// it must not borrow an unrelated successful response as its payload.
	for i := range b.Quotes {
		q := &b.Quotes[i]
		if q.Payload == (dex.Hash{}) {
			h, e := c.Archive.PutObject(struct {
				Anchor                dex.Anchor
				Route, Status, Reason string
				At                    time.Time
			}{q.Anchor, q.Route, q.Status, q.Reason, q.AvailableAt})
			if e != nil {
				return b, e
			}
			q.Payload = h
		}
	}
	proofs := []dex.Hash{header.Payload, logsHash}
	if b.Sky != nil {
		proofs = append(proofs, b.Sky.Payload)
	}
	for _, q := range b.Quotes {
		proofs = append(proofs, q.Payload)
	}
	for _, r := range b.Receipts {
		proofs = append(proofs, r.Payload)
	}
	h, e := c.Archive.PutObject(struct {
		Block     dex.Anchor
		Capture   string
		Members   []dex.Hash
		LogsError string
	}{a, b.Block.Capture, proofs, errorCode(logErr)})
	if e != nil {
		return b, e
	}
	b.Block.Payload = h
	// Validation after the 10-second collection budget gets its own RPC timeout;
	// available_at includes this delay. It never fetches replacement quotes.
	if e = c.Canonical(ctx, b.Block); e != nil {
		return b, e
	}
	b.Seal()
	return b, nil
}
func errorCode(e error) string {
	if e == nil {
		return ""
	}
	if errors.Is(e, context.DeadlineExceeded) {
		return "deadline_exceeded"
	}
	return e.Error()
}
