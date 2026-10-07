package okx

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"sort"
	"strings"
	"time"
)

type PairedCatalog struct {
	Observation model.OKXPairObservation
	Policy      model.OKXLoanPolicy
}

func payloadDigest(payload []byte) string {
	h := sha256.Sum256(payload)
	return hex.EncodeToString(h[:])
}

type loanWire struct {
	ConfigCcyList *[]struct {
		Ccy  string `json:"ccy"`
		Rate string `json:"rate"`
	} `json:"configCcyList"`
	Basic *[]struct {
		Ccy   string `json:"ccy"`
		Rate  string `json:"rate"`
		Quota string `json:"quota"`
	} `json:"basic"`
	Vip     *[]loanLevelWire `json:"vip"`
	Regular *[]loanLevelWire `json:"regular"`
	Config  *[]loanLevelWire `json:"config"`
}
type loanLevelWire struct {
	Ccy         string `json:"ccy"`
	Level       string `json:"level"`
	Quota       string `json:"quota"`
	Discount    string `json:"irDiscount"`
	Coefficient string `json:"loanQuotaCoef"`
	Strategy    string `json:"stgyType"`
}

func ParseLoanPolicy(payload []byte) (model.OKXLoanPolicy, error) {
	var e struct {
		Code string     `json:"code"`
		Data []loanWire `json:"data"`
	}
	var p model.OKXLoanPolicy
	if err := exchange.DecodeStrictJSON(payload, &e); err != nil {
		return p, err
	}
	if e.Code != "0" || len(e.Data) != 1 {
		return p, fmt.Errorf("loan policy requires one complete successful result")
	}
	w := e.Data[0]
	if w.Basic == nil || w.ConfigCcyList == nil || w.Vip == nil || w.Regular == nil || w.Config == nil {
		return p, fmt.Errorf("loan policy missing required arrays")
	}
	number := func(raw string) (decimal.Decimal, error) {
		v, err := model.ParseStrictDecimal(raw, "public loan term")
		if err == nil && !model.ReferenceDecimal(v) {
			err = fmt.Errorf("loan term outside Decimal(38,18)")
		}
		return v, err
	}
	for _, b := range *w.Basic {
		rate, err := number(b.Rate)
		if err != nil {
			return p, err
		}
		quota, err := number(b.Quota)
		if err != nil {
			return p, err
		}
		p.Basic = append(p.Basic, model.OKXBasicLoan{Currency: b.Ccy, DailyRate: rate, PublicQuota: quota})
	}
	for _, r := range *w.ConfigCcyList {
		rate, err := number(r.Rate)
		if err != nil {
			return p, err
		}
		p.Overrides = append(p.Overrides, model.OKXCurrencyRate{Currency: r.Ccy, DailyRate: rate})
	}
	for _, family := range []struct {
		name string
		rows []loanLevelWire
	}{{"vip", *w.Vip}, {"regular", *w.Regular}, {"config", *w.Config}} {
		for _, r := range family.rows {
			l := model.OKXLoanLevel{Family: family.name, Level: r.Level, Currency: r.Ccy, StrategyType: r.Strategy}
			for _, f := range []struct {
				raw string
				out **decimal.Decimal
			}{{r.Quota, &l.Quota}, {r.Discount, &l.InterestDiscount}, {r.Coefficient, &l.QuotaCoefficient}} {
				if f.raw != "" {
					v, err := number(f.raw)
					if err != nil {
						return p, err
					}
					*f.out = &v
				}
			}
			p.Levels = append(p.Levels, l)
		}
	}
	// Validate terms before adding transport provenance, rather than allowing a
	// partial malformed batch to become a successful observation.
	if err := p.ValidateTerms(); err != nil {
		return model.OKXLoanPolicy{}, err
	}
	return p, nil
}
func (c *Client) LoanPolicy(ctx context.Context) (model.OKXLoanPolicy, error) {
	c.wsGateMu.Lock()
	if c.loanGate == nil {
		c.loanGate = exchange.NewRequestGate(1100 * time.Millisecond)
	}
	gate := c.loanGate
	c.wsGateMu.Unlock()
	retry := c.Retry
	before := retry.BeforeRequest
	retry.BeforeRequest = func(ctx context.Context) error {
		if before != nil {
			if err := before(ctx); err != nil {
				return err
			}
		}
		return gate.Wait(ctx)
	}
	requested := time.Now().UTC().Truncate(time.Microsecond)
	endpoint := strings.TrimRight(c.BaseURL, "/") + "/api/v5/public/interest-rate-loan-quota"
	payload, err := exchange.Get(ctx, c.HTTP, endpoint, retry)
	if err != nil {
		return model.OKXLoanPolicy{}, err
	}
	p, err := ParseLoanPolicy(payload)
	if err != nil {
		return p, err
	}
	p.RequestedAt = requested
	p.ID = uuid.New()
	p.ObservedAt = time.Now().UTC().Truncate(time.Microsecond)
	p.SourceURL = endpoint
	p.PayloadHash = payloadDigest(payload)
	return p, p.Validate()
}
func (c *Client) PairedCatalog(ctx context.Context, maxPairs int) (PairedCatalog, error) {
	var result PairedCatalog
	o := model.OKXPairObservation{ID: uuid.New()}
	var catalogs [3][]instrumentWire
	for n, kind := range []string{"SPOT", "SWAP", "MARGIN"} {
		endpoint := strings.TrimRight(c.BaseURL, "/") + "/api/v5/public/instruments?instType=" + kind
		requested := time.Now().UTC().Truncate(time.Microsecond)
		payload, err := exchange.Get(ctx, c.HTTP, endpoint, c.Retry)
		if err != nil {
			return result, err
		}
		received := time.Now().UTC().Truncate(time.Microsecond)
		var e struct {
			Code string            `json:"code"`
			Data *[]instrumentWire `json:"data"`
		}
		if err = exchange.DecodeStrictJSON(payload, &e); err != nil {
			return result, err
		}
		if e.Code != "0" || e.Data == nil || len(*e.Data) == 0 {
			return result, fmt.Errorf("incomplete %s catalog", kind)
		}
		catalogs[n] = *e.Data
		o.SourceURLs = append(o.SourceURLs, endpoint)
		o.SourceHashes = append(o.SourceHashes, payloadDigest(payload))
		o.RequestedAt = append(o.RequestedAt, requested)
		o.ReceivedAt = append(o.ReceivedAt, received)
		o.RawCounts = append(o.RawCounts, uint32(len(*e.Data)))
	}
	policy, err := c.LoanPolicy(ctx)
	if err != nil {
		return result, err
	}
	result.Policy = policy
	o.LoanPolicyID = policy.ID
	o.SourceURLs = append(o.SourceURLs, policy.SourceURL)
	o.SourceHashes = append(o.SourceHashes, policy.PayloadHash)
	o.RequestedAt = append(o.RequestedAt, policy.RequestedAt)
	o.ReceivedAt = append(o.ReceivedAt, policy.ObservedAt)
	o.RawCounts = append(o.RawCounts, 1)
	candidates, err := MatchPairedCatalogs(catalogs[0], catalogs[1], catalogs[2], policy, maxPairs)
	if err != nil {
		return result, err
	}
	o.Pairs = candidates
	o.ObservedAt = time.Now().UTC().Truncate(time.Microsecond)
	result.Observation = o
	return result, nil
}
func MatchPairedCatalogs(spot, swap, margin []instrumentWire, policy model.OKXLoanPolicy, maxPairs int) ([]model.OKXPairMember, error) {
	if err := policy.ValidateTerms(); err != nil {
		return nil, err
	}
	if maxPairs < 1 || maxPairs > 500 {
		return nil, fmt.Errorf("invalid pair capacity")
	}
	groups := []map[string]instrumentWire{{}, {}, {}}
	for n, rows := range [][]instrumentWire{spot, swap, margin} {
		seen := map[string]bool{}
		for _, w := range rows {
			expected := []string{"SPOT", "SWAP", "MARGIN"}[n]
			if w.InstType != expected || w.InstID == "" || strings.TrimSpace(w.InstID) != w.InstID || w.State == "" || strings.TrimSpace(w.State) != w.State || w.Category == "" || w.RuleType == "" || seen[w.InstID] {
				return nil, fmt.Errorf("invalid %s catalog identity", expected)
			}
			seen[w.InstID] = true
			if w.State != "live" {
				continue
			}
			if w.Category == "" || w.RuleType == "" {
				return nil, fmt.Errorf("missing crypto classification")
			}
			if w.Category != "1" || w.RuleType != "normal" {
				continue
			}
			if n == 1 {
				if w.CtType == "" || w.SettleCcy == "" || w.CtValCcy == "" || strings.TrimSpace(w.SettleCcy) != w.SettleCcy {
					return nil, fmt.Errorf("incomplete SWAP selection fields")
				}
				if w.CtType != "linear" || w.SettleCcy != "USDT" {
					continue
				}
			} else {
				if w.QuoteCcy == "" || w.BaseCcy == "" || strings.TrimSpace(w.QuoteCcy) != w.QuoteCcy {
					return nil, fmt.Errorf("incomplete %s selection fields", expected)
				}
				if w.QuoteCcy != "USDT" {
					continue
				}
			}
			base := w.BaseCcy
			if n == 1 {
				base = w.CtValCcy
			}
			if base == "" || strings.TrimSpace(base) != base {
				return nil, fmt.Errorf("invalid base currency")
			}
			if (n == 1 && w.InstID != base+"-USDT-SWAP") || (n != 1 && w.InstID != base+"-USDT") {
				return nil, fmt.Errorf("symbol/base mismatch")
			}
			if _, exists := groups[n][base]; exists {
				return nil, fmt.Errorf("duplicate canonical base")
			}
			groups[n][base] = w
		}
	}
	quota := map[string]decimal.Decimal{}
	for _, b := range policy.Basic {
		quota[b.Currency] = b.PublicQuota
	}
	var bases []string
	for base := range groups[0] {
		if _, ok := groups[1][base]; !ok {
			continue
		}
		if _, ok := groups[2][base]; ok && quota[base].IsPositive() {
			bases = append(bases, base)
		}
	}
	sort.Strings(bases)
	if len(bases) == 0 || len(bases) > maxPairs {
		return nil, fmt.Errorf("paired universe %d outside capacity 1..%d", len(bases), maxPairs)
	}
	var out []model.OKXPairMember
	for _, base := range bases {
		s, err := mapInstrument(groups[0][base], model.MarketSpot)
		if err != nil {
			return nil, err
		}
		p, err := mapInstrument(groups[1][base], model.MarketPerpetual)
		if err != nil {
			return nil, err
		}
		sm, err := model.ParsePositiveDecimal(groups[0][base].MinSz, "spot minSz")
		if err != nil {
			return nil, err
		}
		pm, err := model.ParsePositiveDecimal(groups[1][base].MinSz, "swap minSz")
		if err != nil {
			return nil, err
		}
		if !model.ReferenceDecimal(sm) || !model.ReferenceDecimal(pm) {
			return nil, fmt.Errorf("minimum size outside storage precision")
		}
		out = append(out, model.OKXPairMember{Base: base, Spot: s, Perpetual: p, SpotMinSize: sm, PerpetualMinSize: pm})
	}
	return out, nil
}
