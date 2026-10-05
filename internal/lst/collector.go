package lst

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
)

type Collector struct {
	RPC                     *RPC
	LogRPC                  *RPC // optional events and historical queue identity; primary headers anchor the chain
	LogMode                 string
	PauseLiveLogs           bool   // explicit pause; preserves coverage and the original cursor
	LiveLogsFromBlock       uint64 // explicit watch segment; original cursor and older gaps retained
	CEX                     *CEX
	Manifest                Manifest
	Metadata                CEXMetadata
	Store                   Store
	Archive                 ethereum.Archive
	StateDir                string
	IdentityResponses       []Response
	MarketInterval          time.Duration
	EntryRoutesPerRound     int // zero preserves the original A/B pair
	PruneCommittedResponses bool
}

func (c *Collector) newCapture(kind, mode string) Capture {
	return Capture{CaptureId: uuid.New(), ManifestHash: c.Manifest.Hash, CaptureKind: kind, CaptureMode: mode, SourceId: "ethereum:1+lido+binance:ETHUSDT", StartedAt: Now(), AvailableAt: Now(), Finality: "head", Status: "partial", Revision: 1}
}
func setAnchor(cap *Capture, from, to Block) {
	cap.ChainId = Ptr(uint64(1))
	cap.FromBlock = &from.Number
	cap.ToBlock = &to.Number
	cap.FromBlockHash = &from.Hash
	cap.ToBlockHash = &to.Hash
	cap.FromBlockTime = &from.Time
	cap.ToBlockTime = &to.Time
}
func (c *Collector) seal(b *Batch, responses []Response) error {
	hashes := []string{}
	seen := map[string]bool{}
	for _, r := range responses {
		h := r.PayloadHash
		if len(h) == 66 {
			v, e := ParseHex(h, 32)
			if e != nil {
				return e
			}
			h = v
		}
		if len(h) == 32 && !seen[h] {
			hashes = append(hashes, Hex(h))
			seen[h] = true
		}
		if !r.ReceivedAt.IsZero() && !r.ReceivedAt.Before(b.Capture.StartedAt) && (b.Capture.ReceivedAt == nil || r.ReceivedAt.After(*b.Capture.ReceivedAt)) {
			b.Capture.ReceivedAt = Ptr(r.ReceivedAt)
		}
	}
	sort.Strings(hashes)
	proof, err := c.Archive.PutObject(struct {
		Version, Manifest string
		CaptureId         uuid.UUID
		Responses         []string
	}{CollectorVersion, Hex(c.Manifest.Hash), b.Capture.CaptureId, hashes})
	if err != nil {
		return err
	}
	b.Capture.EvidenceRootHash = string(proof[:])
	b.Capture.AvailableAt = Now()
	if c.PruneCommittedResponses {
		b.RawEvidenceHashes = append(c.takeRawEvidence(), proof.String())
	}
	return b.Seal()
}
func (c *Collector) blankQuote(cap Capture, route string, budget *big.Int) Quote {
	lst := c.Manifest.Address("steth")
	if route == "B" {
		lst = c.Manifest.Address("wsteth")
	}
	q := Quote{CaptureId: cap.CaptureId, ObservedAt: cap.StartedAt, QuoteRole: "entry", RouteId: route, QuoteAssetAddress: c.Manifest.Address("usdt"), LstAddress: lst, PurchaseBudgetUsdtRaw: clone(budget), HedgeInstrumentId: c.Metadata.Instrument.ID, BuyStatus: "unknown", ConversionStatus: "unknown", ExitStatus: "unknown", HedgeStatus: "unknown", TimingStatus: "unknown", AvailableAt: Now()}
	q.QuoteId = CanonicalHash(struct {
		Id                  uuid.UUID
		Role, Route, Budget string
	}{cap.CaptureId, "entry", route, budget.String()})
	return q
}
func (c *Collector) chainEntry(ctx context.Context, b Block, p ProtocolState, q *Quote, weth *big.Int, buyResponse Response, buyGas uint64) (responses []Response) {
	if p.StateStatus != "ok" {
		q.Reason = "protocol_unknown"
		return
	}
	if p.QueuePaused == nil || *p.QueuePaused {
		q.Reason = "withdrawals_paused"
		return
	}
	if weth == nil {
		q.BuyReason = "usdt_weth_unavailable"
		return
	}
	responses = append(responses, buyResponse)
	q.BuyWethOutWei = clone(weth)
	q.QuoterInternalGas = append(q.QuoterInternalGas, buyGas)
	var amount *big.Int
	var rs Response
	var e error
	var gas uint64
	if q.RouteId == "A" {
		amount, rs, e = c.RPC.Uint(ctx, c.Manifest.Address("curve"), b, "get_dy(int128,int128,uint256)", big.NewInt(0), big.NewInt(1), weth)
	} else {
		amount, gas, rs, e = c.RPC.Swap(ctx, c.Manifest, b, c.Manifest.Address("weth"), c.Manifest.Address("wsteth"), 100, weth)
		q.QuoterInternalGas = append(q.QuoterInternalGas, gas)
	}
	responses = append(responses, rs)
	if e != nil {
		q.BuyReason = failureReason("lst_buy", e, rs)
		return
	}
	q.BuyLstOutRaw = amount
	q.BuyStatus = "ok"
	steth := clone(amount)
	if q.RouteId == "B" {
		steth, rs, e = c.RPC.Uint(ctx, c.Manifest.Address("wsteth"), b, "getStETHByWstETH(uint256)", amount)
		responses = append(responses, rs)
		if e != nil {
			q.ConversionReason = failureReason("wsteth_conversion", e, rs)
			return
		}
	}
	parts, e := requestParts(steth, p.MinRequestStethWei, p.MaxRequestStethWei)
	if e != nil {
		q.ConversionReason = e.Error()
		return
	}
	q.RequestParts = &parts
	q.RequestStethWei = steth
	// Splitting wstETH and shares changes integer rounding. Never mark an
	// unsplit conversion as the actual sum of several withdrawal requests.
	if parts != 1 {
		q.ConversionReason = "split_withdrawal_rounding_not_quoted"
		return
	}
	shares, rs, e := c.RPC.Uint(ctx, c.Manifest.Address("steth"), b, "getSharesByPooledEth(uint256)", steth)
	responses = append(responses, rs)
	if e != nil {
		q.ConversionReason = failureReason("steth_shares", e, rs)
		return
	}
	if shares == nil || shares.Sign() == 0 {
		q.ConversionReason = "steth_shares_zero_or_missing"
		return
	}
	q.RequestSharesRaw = shares
	q.NominalRedeemEthWei = clone(steth)
	q.ConversionStatus = "ok"
	q.EthExitInputWei = clone(steth)
	exit, gas, rs, e := c.RPC.Swap(ctx, c.Manifest, b, c.Manifest.Address("weth"), c.Manifest.Address("usdt"), 500, steth)
	responses = append(responses, rs)
	if e != nil {
		q.ExitReason = failureReason("eth_exit", e, rs)
		return
	}
	q.EthExitUsdtOutRaw = exit
	q.QuoterInternalGas = append(q.QuoterInternalGas, gas)
	q.ExitStatus = "ok"
	return
}
func quoteFresh(q Quote, p ProtocolState, b Block, started, ended time.Time) bool {
	return quoteTimingReason(q, p, b, started, ended) == ""
}
func completeQuote(q Quote) bool {
	return q.BuyStatus == "ok" && q.ConversionStatus == "ok" && q.ExitStatus == "ok" && q.HedgeStatus == "ok" && q.TimingStatus == "fresh" && q.MarkPriceTickE8 != nil
}

// Each budget gets a leading turn every four minutes. During the daily seed
// window prioritize 100k; all quotes retain their actual independent timestamps.
func entryBudgetOrder(at time.Time) []int {
	at = at.UTC()
	first := int(at.Unix() / 60 % 4)
	if at.Hour() == 12 && at.Minute() < 5 {
		first = 2
	}
	return []int{first, (first + 1) % 4, (first + 2) % 4, (first + 3) % 4}
}

func (c *Collector) marketInterval() time.Duration {
	if c.MarketInterval > 0 {
		return c.MarketInterval
	}
	return time.Minute
}

func (c *Collector) entryPlan(at time.Time) ([]int, []string, int) {
	at = at.UTC()
	round := at.Unix() / int64(c.marketInterval()/time.Second)
	first := int(round % 4)
	routes := []string{"A", "B"}
	count := 2
	if c.EntryRoutesPerRound == 1 {
		count = 1
		first = int(round / 2 % 4)
		if round%2 != 0 {
			routes = []string{"B", "A"}
		}
	}
	if at.Hour() == 12 && at.Minute() < 5 {
		first = 2
	}
	return []int{first, (first + 1) % 4, (first + 2) % 4, (first + 3) % 4}, routes, count
}

func (c *Collector) validateScheduling() error {
	if c.MarketInterval != 0 && c.MarketInterval != time.Minute && c.MarketInterval != 2*time.Minute {
		return errors.New("invalid_market_interval")
	}
	if c.EntryRoutesPerRound < 0 || c.EntryRoutesPerRound > 2 {
		return errors.New("invalid_entry_routes_per_round")
	}
	return nil
}

// A timed cross-source observation needs a recent chain anchor first. Only
// successfully decoded but old heads are polled again; source failures stop
// immediately. All attempts remain in the observation's archived evidence.
func (c *Collector) marketHead(parent context.Context) (Block, []Response, error) {
	ctx, cancel := context.WithTimeout(parent, 20*time.Second)
	defer cancel()
	var head Block
	var responses []Response
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			if err := c.RPC.Transport.cfg.Clock.Sleep(ctx, 3*time.Second); err != nil {
				return head, responses, err
			}
		}
		var err error
		head, err = c.RPC.Header(ctx, "latest")
		responses = append(responses, head.Response)
		if err != nil {
			return head, responses, err
		}
		age := Now().Sub(head.Time)
		if age >= -2*time.Second && age <= 10*time.Second {
			return head, responses, nil
		}
	}
	return head, responses, fmt.Errorf("head_admission_not_fresh age_ms=%d attempts=3", Now().Sub(head.Time).Milliseconds())
}

// Market produces the same eight member identities even when a source fails.
// Timed-out members remain unknown; no backlog of stale quotes is replayed.
func (c *Collector) Market(parent context.Context, followups []Quote) (Batch, error) {
	if e := c.validateScheduling(); e != nil {
		return Batch{}, e
	}
	head, headResponses, headErr := c.marketHead(parent)
	cap := c.newCapture("market", "live")
	cap.ExpectedProtocolRows = 1
	cap.ExpectedQuoteRows = uint32(8 + len(followups))
	batch := Batch{Capture: cap}
	order, routes, scheduled := c.entryPlan(cap.StartedAt)
	for _, i := range order {
		for _, route := range routes {
			batch.Quotes = append(batch.Quotes, c.blankQuote(cap, route, c.Manifest.Budget(i)))
		}
	}
	// Keep all eight identities; source quotas determine the planned count.
	for i := scheduled; i < 8; i++ {
		batch.Quotes[i].Reason = "not_scheduled_this_round"
		batch.Quotes[i].TimingStatus = "not_scheduled"
	}
	p := ProtocolState{CaptureId: cap.CaptureId, ObservedAt: cap.StartedAt, ChainId: 1, QueueAddress: c.Manifest.Address("queue"), StateStatus: "unknown", Reason: "head_unavailable", AvailableAt: Now()}
	responses := append([]Response(nil), c.IdentityResponses...)
	responses = append(responses, headResponses...)
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	quoteCtx, quoteCancel := context.WithDeadline(ctx, cap.StartedAt.Add(25*time.Second))
	defer quoteCancel()
	e := headErr
	notRequested := errors.New("cex_not_requested_head_admission_failed")
	market := CEXMarket{DepthErr: notRequested, MarkErr: notRequested}
	if head.Number != 0 && goodHash(head.Hash, 32) && timeValid(head.Time) {
		setAnchor(&batch.Capture, head, head)
	}
	if e == nil {
		marketChannel := make(chan CEXMarket, 1)
		go func() {
			timer := time.NewTimer(max(time.Duration(0), time.Until(cap.StartedAt.Add(12*time.Second))))
			defer timer.Stop()
			select {
			case <-ctx.Done():
				marketChannel <- CEXMarket{DepthErr: ctx.Err(), MarkErr: ctx.Err()}
				return
			case <-timer.C:
			}
			marketChannel <- c.CEX.Market(ctx, c.Metadata)
		}()
		var rr []Response
		p, rr, e = c.RPC.Protocol(quoteCtx, c.Manifest, head)
		p.CaptureId = cap.CaptureId
		p.ObservedAt = cap.StartedAt
		responses = append(responses, rr...)
		if e != nil {
			p.StateStatus = "unknown"
		}
		collectFollowups := func() {
			for _, seed := range followups {
				q := c.blankQuote(cap, seed.RouteId, big.NewInt(0))
				q.QuoteRole = "followup"
				q.PurchaseBudgetUsdtRaw = nil
				q.ReferenceQuoteId = seed.ReferenceQuoteId
				q.TargetDelaySeconds = seed.TargetDelaySeconds
				q.PlannedForAt = seed.PlannedForAt
				q.NominalRedeemEthWei = clone(seed.NominalRedeemEthWei)
				q.EthExitInputWei = clone(seed.NominalRedeemEthWei)
				q.HedgeInstrumentId = seed.HedgeInstrumentId
				q.HedgeQuantityLot = seed.HedgeQuantityLot
				q.HedgeEthWei = clone(seed.HedgeEthWei)
				q.QuoteId = CanonicalHash(struct {
					Id    uuid.UUID
					Ref   string
					Delay uint32
				}{cap.CaptureId, *seed.ReferenceQuoteId, *seed.TargetDelaySeconds})
				if seed.PlannedForAt == nil || Now().Sub(*seed.PlannedForAt) > 2*time.Minute {
					q.TimingStatus = "missed"
					q.Reason = "followup_window_missed"
				} else {
					out, gas, res, err := c.RPC.Swap(quoteCtx, c.Manifest, head, c.Manifest.Address("weth"), c.Manifest.Address("usdt"), 500, q.NominalRedeemEthWei)
					responses = append(responses, res)
					q.ChainRequestedAt, q.ChainReceivedAt, q.ChainAvailableAt, q.ChainPayloadHashes = responseTimes([]Response{res})
					if err != nil {
						q.ExitReason = failureReason("followup_exit", err, res)
					} else {
						q.EthExitUsdtOutRaw = out
						q.QuoterInternalGas = []uint64{gas}
						q.ExitStatus = "ok"
					}
				}
				batch.Quotes = append(batch.Quotes, q)
			}
		}
		// Due same-quantity exits get the first usable quote slots. Rotate the
		// leading entry budget so slow public RPC cannot starve three sizes.
		collectFollowups()
		for k := 0; k < scheduled; k += scheduled {
			if quoteCtx.Err() != nil || p.StateStatus != "ok" {
				for i := k; i < k+scheduled; i++ {
					batch.Quotes[i].Reason = p.Reason
					if quoteCtx.Err() != nil {
						batch.Quotes[i].Reason = "stage=entry cause=round_budget_exhausted http_attempted=false"
					}
				}
				break
			}
			weth, gas, res, err := c.RPC.Swap(quoteCtx, c.Manifest, head, c.Manifest.Address("usdt"), c.Manifest.Address("weth"), 500, batch.Quotes[k].PurchaseBudgetUsdtRaw)
			responses = append(responses, res)
			if err != nil {
				batch.Quotes[k].BuyReason = failureReason("usdt_weth_buy", err, res)
				if scheduled == 2 {
					batch.Quotes[k+1].BuyReason = batch.Quotes[k].BuyReason
				}
				continue
			}
			for i := k; i < k+scheduled; i++ {
				q := &batch.Quotes[i]
				rr := c.chainEntry(quoteCtx, head, p, q, weth, res, gas)
				responses = append(responses, rr...)
				q.ChainRequestedAt, q.ChainReceivedAt, q.ChainAvailableAt, q.ChainPayloadHashes = responseTimes(rr)
			}
		}

		market = <-marketChannel
		responses = append(responses, market.Depth.Response, market.Mark.Response)
		// End check reserves the last 5s of the original 30s capture budget. If it
		// cannot complete, the sample is not presented as canonical/fresh.
		ok, res, err := c.RPC.Canonical(ctx, head)
		responses = append(responses, res)
		batch.Capture.Canonical = err == nil && ok
		if err != nil {
			batch.Capture.Reason = failureReason("canonical_end_check", err, res)
		} else if !ok {
			batch.Capture.Reason = "canonical_end_check_hash_mismatch"
		}
	} else {
		batch.Capture.Reason = failureReason("head", e, head.Response)
		p.Reason = batch.Capture.Reason
		for i := 0; i < scheduled; i++ {
			batch.Quotes[i].Reason = p.Reason
		}
	}
	// A failed head still needs the expected followup rows, with no invented source.
	for len(batch.Quotes) < int(cap.ExpectedQuoteRows) {
		s := followups[len(batch.Quotes)-8]
		q := c.blankQuote(cap, s.RouteId, big.NewInt(0))
		q.QuoteRole = "followup"
		q.ReferenceQuoteId = s.ReferenceQuoteId
		q.TargetDelaySeconds = s.TargetDelaySeconds
		q.PlannedForAt = s.PlannedForAt
		q.PurchaseBudgetUsdtRaw = nil
		q.NominalRedeemEthWei = clone(s.NominalRedeemEthWei)
		q.EthExitInputWei = clone(s.NominalRedeemEthWei)
		q.HedgeInstrumentId = s.HedgeInstrumentId
		q.HedgeQuantityLot = s.HedgeQuantityLot
		q.HedgeEthWei = clone(s.HedgeEthWei)
		q.Reason = p.Reason
		q.QuoteId = CanonicalHash(struct {
			Id    uuid.UUID
			Ref   string
			Delay uint32
		}{cap.CaptureId, *s.ReferenceQuoteId, *s.TargetDelaySeconds})
		batch.Quotes = append(batch.Quotes, q)
	}
	ended := Now()
	for i := range batch.Quotes {
		q := &batch.Quotes[i]
		if q.Reason == "not_scheduled_this_round" {
			q.AvailableAt = ended
			continue
		}
		if q.TimingStatus != "missed" {
			hedgeInput := q.NominalRedeemEthWei
			if q.QuoteRole == "followup" {
				hedgeInput = q.HedgeEthWei
			}
			originalInstrument := q.HedgeInstrumentId
			originalHedge := clone(q.HedgeEthWei)
			_ = ApplyHedge(q, c.Metadata, market, hedgeInput)
			if q.QuoteRole == "followup" {
				if originalInstrument != c.Metadata.Instrument.ID || originalHedge == nil || q.HedgeEthWei == nil || originalHedge.Cmp(q.HedgeEthWei) != 0 {
					q.HedgeStatus = "unknown"
					q.HedgeReason = "original_hedge_no_longer_representable"
					q.HedgeInstrumentId = originalInstrument
				} else {
					q.UnhedgedEthResidualWei = new(big.Int).Sub(q.NominalRedeemEthWei, q.HedgeEthWei)
				}
			}
			timingReason := quoteTimingReason(*q, p, head, cap.StartedAt, ended)
			if !batch.Capture.Canonical {
				timingReason = appendReason(timingReason, "stage=timing cause=canonical_unverified")
			}
			if timingReason == "" {
				q.TimingStatus = "fresh"
			} else {
				q.TimingStatus = "stale"
				q.Reason = appendReason(q.Reason, timingReason)
			}
		}
		q.AvailableAt = ended
	}
	p.AvailableAt = ended
	batch.Protocols = []ProtocolState{p}
	batch.Capture.Status = "complete"
	for _, q := range batch.Quotes {
		if q.Reason == "not_scheduled_this_round" || q.QuoteRole == "entry" && !completeQuote(q) || q.QuoteRole == "followup" && (q.ExitStatus != "ok" || q.HedgeStatus != "ok" || q.TimingStatus != "fresh") {
			batch.Capture.Status = "partial"
		}
	}
	sealErr := c.seal(&batch, responses)
	return batch, sealErr
}

func (c *Collector) Funding(ctx context.Context, from, to time.Time) (Batch, error) {
	b := Batch{Capture: c.newCapture("funding", "live")}
	b.Capture.SourceId = "binance:usdt-perpetual:ETHUSDT:actual"
	b.Capture.Finality = "not_applicable"
	b.Capture.Canonical = true
	b.Capture.WindowFromAt = &from
	b.Capture.WindowToAt = &to
	window, e := c.CEX.Funding(ctx, from, to)
	if e != nil || !window.Complete {
		b.Capture.Status = "failed"
		b.Capture.Reason = "funding_window_incomplete"
		if e != nil {
			var res Response
			if len(window.Responses) > 0 {
				res = window.Responses[len(window.Responses)-1]
			}
			b.Capture.Reason = failureReason("funding", e, res)
		}
	} else {
		b.Capture.Status = "complete"
		for _, p := range window.Points {
			h, err := ParseHex(p.Response.PayloadHash, 32)
			if err != nil {
				return b, err
			}
			b.Funding = append(b.Funding, FundingSettlement{CaptureId: b.Capture.CaptureId, InstrumentId: c.Metadata.Instrument.ID, FundingTime: p.FundingTime, FundingRate: p.Rate, SettlementMarkPriceTickE8: &p.MarkPriceTickE8, SourceId: b.Capture.SourceId, RequestedAt: p.Response.RequestedAt, ReceivedAt: p.Response.ReceivedAt, AvailableAt: p.Response.AvailableAt, SourcePayloadHash: h})
		}
	}
	err := c.seal(&b, window.Responses)
	if err != nil {
		return b, err
	}
	return b, e
}

func (c *Collector) ValidateReady() error {
	if e := c.validateScheduling(); e != nil {
		return e
	}
	if c.RPC == nil || c.CEX == nil || c.Store == nil || !c.Manifest.Pinned() || c.Metadata.Instrument.ID == 0 {
		return errors.New("collector_not_initialized")
	}
	return nil
}
