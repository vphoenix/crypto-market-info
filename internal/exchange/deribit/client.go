package deribit

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/options"
)

type Client struct {
	RESTURL, WSURL        string
	HTTP                  *http.Client
	RESTGate, ControlGate *exchange.RequestGate
}

func NewClient(rest, ws string) *Client {
	return &Client{RESTURL: strings.TrimRight(rest, "/"), WSURL: ws, HTTP: &http.Client{Timeout: 15 * time.Second}, RESTGate: exchange.NewRequestGate(2 * time.Second), ControlGate: exchange.NewRequestGate(time.Second)}
}

type Scope struct{ Currency, Kind string }

func (s Scope) String() string { return s.Currency + ":" + s.Kind }

type ScopeResult struct {
	RequestID                              uuid.UUID
	RawCount, AcceptedCount, ExcludedCount uint32
	Scope                                  Scope
	URL, PayloadHash, Status               string
	RequestedAt, ObservedAt                time.Time
	Instruments                            []ParsedInstrument
}

func (c *Client) cooldown(d time.Duration) { c.RESTGate.Cooldown(d); c.ControlGate.Cooldown(d) }
func (c *Client) get(ctx context.Context, method string, params url.Values) ([]byte, string, time.Time, time.Time, error) {
	u := c.RESTURL + "/api/v2/public/" + method + "?" + params.Encode()
	if err := c.RESTGate.Wait(ctx); err != nil {
		return nil, u, time.Time{}, time.Time{}, err
	}
	start := time.Now().UTC().Truncate(time.Microsecond)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, u, start, start, err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, u, start, time.Now().UTC().Truncate(time.Microsecond), err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, (8<<20)+1))
	at := time.Now().UTC().Truncate(time.Microsecond)
	if err != nil {
		return nil, u, start, at, err
	}
	if len(raw) > 8<<20 {
		return nil, u, start, at, fmt.Errorf("Deribit response exceeds 8 MiB")
	}
	if resp.StatusCode == 429 {
		delay := time.Minute
		if n, e := strconv.ParseInt(resp.Header.Get("Retry-After"), 10, 32); e == nil && n >= 0 {
			delay = time.Duration(n) * time.Second
		} else if t, e := http.ParseTime(resp.Header.Get("Retry-After")); e == nil && t.After(at) {
			delay = t.Sub(at)
		}
		c.cooldown(delay)
	}
	if resp.StatusCode != http.StatusOK {
		return raw, u, start, at, fmt.Errorf("Deribit HTTP %d", resp.StatusCode)
	}
	var frame struct {
		Error *struct {
			Code int `json:"code"`
		} `json:"error"`
	}
	if err := decode(raw, &frame); err != nil {
		return raw, u, start, at, err
	}
	if frame.Error != nil {
		if frame.Error.Code == 10028 {
			c.cooldown(time.Minute)
		}
		return raw, u, start, at, fmt.Errorf("Deribit RPC error %d", frame.Error.Code)
	}
	return raw, u, start, at, nil
}
func (c *Client) FetchScope(ctx context.Context, s Scope) (ScopeResult, error) {
	raw, u, start, at, err := c.get(ctx, "get_instruments", url.Values{"currency": {s.Currency}, "kind": {s.Kind}, "expired": {"false"}})
	r := ScopeResult{RequestID: uuid.New(), Scope: s, URL: u, RequestedAt: start, ObservedAt: at, Status: "request_error"}
	if len(raw) > 0 {
		r.PayloadHash = options.PayloadHash(raw)
	}
	if err != nil {
		return r, err
	}
	var excluded []ExcludedInstrument
	r.Instruments, excluded, err = DecodeInstruments(raw, s.Currency, s.Kind, at)
	if err != nil {
		r.Status = "parse_error"
		return r, err
	}
	r.ObservedAt = time.Now().UTC().Truncate(time.Microsecond)
	r.AcceptedCount = uint32(len(r.Instruments))
	r.ExcludedCount = uint32(len(excluded))
	r.RawCount = r.AcceptedCount + r.ExcludedCount
	for n := range r.Instruments {
		rule := &r.Instruments[n].Rule
		rule.ObservedAt = r.ObservedAt
		rule.KnownFrom = r.ObservedAt
		rule.EffectiveFrom = r.ObservedAt
		rule.SourceURL = u
	}
	r.Status = "complete"
	return r, nil
}
func (c *Client) Reference(ctx context.Context, symbol string) (options.SelectionReference, error) {
	raw, _, _, at, err := c.get(ctx, "get_order_book", url.Values{"instrument_name": {symbol}, "depth": {"1"}})
	if err != nil {
		return options.SelectionReference{}, err
	}
	// Known protocol fields use exact spelling, including nested price arrays.
	var wire struct {
		JSONRPC string `json:"jsonrpc"`
		Result  *struct {
			Symbol    string              `json:"instrument_name"`
			Timestamp *int64              `json:"timestamp"`
			Bids      [][]json.RawMessage `json:"bids"`
			Asks      [][]json.RawMessage `json:"asks"`
		} `json:"result"`
	}
	if err = decode(raw, &wire); err != nil {
		return options.SelectionReference{}, err
	}
	d := wire.Result
	if wire.JSONRPC != "2.0" || d == nil || d.Symbol != symbol || d.Timestamp == nil || *d.Timestamp <= 0 || len(d.Bids) != 1 || len(d.Asks) != 1 || len(d.Bids[0]) != 2 || len(d.Asks[0]) != 2 {
		return options.SelectionReference{}, fmt.Errorf("reference requires complete BBO")
	}
	bid, err := options.Price(string(d.Bids[0][0]), false)
	if err != nil {
		return options.SelectionReference{}, err
	}
	ask, err := options.Price(string(d.Asks[0][0]), false)
	if err != nil {
		return options.SelectionReference{}, err
	}
	for _, row := range [][][]json.RawMessage{d.Bids, d.Asks} {
		q, err := options.Quantity(string(row[0][1]))
		if err != nil || q == 0 {
			return options.SelectionReference{}, fmt.Errorf("reference lacks quantity")
		}
	}
	source := time.UnixMilli(*d.Timestamp).UTC()
	if ask <= bid || source.After(at.Add(time.Second)) || at.Sub(source) > 30*time.Second {
		return options.SelectionReference{}, fmt.Errorf("stale/crossed reference")
	}
	return options.SelectionReference{Symbol: symbol, Bid: bid, Ask: ask, SourceTime: source, ReceivedAt: at, PayloadHash: options.PayloadHash(raw)}, nil
}
