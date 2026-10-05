package deribit

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/options"
)

func DecodeLifecycle(f StreamEvent, at time.Time) (options.LifecycleObservation, error) {
	o := options.LifecycleObservation{ID: uuid.New(), Epoch: f.Epoch, Sequence: f.Sequence, ReceivedAt: at.UTC().Truncate(time.Microsecond), Channel: f.Channel, PayloadHash: options.PayloadHash(f.Raw)}
	var frame struct {
		JSONRPC string          `json:"jsonrpc"`
		Method  string          `json:"method"`
		Result  json.RawMessage `json:"result"`
		Params  struct {
			Channel string          `json:"channel"`
			Data    json.RawMessage `json:"data"`
		} `json:"params"`
	}
	if err := decode(f.Raw, &frame); err != nil {
		return o, err
	}
	if frame.JSONRPC != "2.0" {
		return o, fmt.Errorf("invalid lifecycle RPC")
	}
	if f.Kind == "status" {
		o.Kind = "status"
		o.Channel = "public/status"
		var d struct {
			Locked  string    `json:"locked"`
			Indexes *[]string `json:"locked_indices"`
		}
		if err := decode(frame.Result, &d); err != nil {
			return o, err
		}
		o.LockMode = d.Locked
		if d.Indexes != nil {
			o.LockedIndexes = *d.Indexes
		}
		return o, o.Validate()
	}
	if frame.Method != "subscription" || frame.Params.Channel != f.Channel {
		return o, fmt.Errorf("invalid lifecycle envelope")
	}
	if strings.HasPrefix(f.Channel, "instrument.state.") {
		var d struct {
			Symbol    string `json:"instrument_name"`
			State     string `json:"state"`
			Timestamp *int64 `json:"timestamp"`
		}
		if err := decode(frame.Params.Data, &d); err != nil {
			return o, err
		}
		if d.Timestamp == nil || *d.Timestamp <= 0 {
			return o, fmt.Errorf("missing state timestamp")
		}
		t := time.UnixMilli(*d.Timestamp).UTC()
		o.SourceTime = &t
		o.Kind = "state"
		o.Symbol = d.Symbol
		o.State = d.State
	} else if f.Channel == "platform_state" {
		var d struct {
			Index       string `json:"price_index"`
			Locked      *bool  `json:"locked"`
			Maintenance *bool  `json:"maintenance"`
		}
		if err := decode(frame.Params.Data, &d); err != nil {
			return o, err
		}
		o.Kind = "platform"
		o.IndexID = d.Index
		o.Locked = d.Locked
		o.Maintenance = d.Maintenance
	} else {
		return o, fmt.Errorf("unsupported lifecycle channel")
	}
	return o, o.Validate()
}

func DecodeCreation(f StreamEvent, at time.Time) (ScopeResult, error) {
	r := ScopeResult{RequestID: uuid.New(), RequestedAt: at.UTC().Truncate(time.Microsecond), ObservedAt: at.UTC().Truncate(time.Microsecond), URL: f.Channel, Raw: f.Raw, PayloadHash: options.PayloadHash(f.Raw), Status: "parse_error"}
	p := strings.Split(f.Channel, ".")
	if len(p) != 4 || p[0] != "instrument" || p[1] != "creation" {
		return r, fmt.Errorf("invalid creation channel")
	}
	r.Scope = Scope{Currency: p[3], Kind: p[2]}
	var frame struct {
		JSONRPC string `json:"jsonrpc"`
		Method  string `json:"method"`
		Params  struct {
			Channel string          `json:"channel"`
			Data    json.RawMessage `json:"data"`
		} `json:"params"`
	}
	if err := decode(f.Raw, &frame); err != nil {
		return r, err
	}
	if frame.JSONRPC != "2.0" || frame.Method != "subscription" || frame.Params.Channel != f.Channel {
		return r, fmt.Errorf("invalid creation envelope")
	}
	return decodeSingleResult(r, frame.Params.Data)
}
func (c *Client) FetchInstrument(ctx context.Context, scope Scope, symbol string) (ScopeResult, error) {
	raw, u, start, at, err := c.get(ctx, "get_instrument", url.Values{"instrument_name": {symbol}})
	r := ScopeResult{RequestID: uuid.New(), Scope: scope, URL: u, RequestedAt: start, ObservedAt: at, Raw: raw, Status: "request_error"}
	if len(raw) > 0 {
		r.PayloadHash = options.PayloadHash(raw)
	}
	if err != nil {
		return r, err
	}
	var frame struct {
		JSONRPC string          `json:"jsonrpc"`
		Result  json.RawMessage `json:"result"`
	}
	if err = decode(raw, &frame); err != nil {
		r.Status = "parse_error"
		return r, err
	}
	if frame.JSONRPC != "2.0" {
		r.Status = "parse_error"
		return r, fmt.Errorf("invalid instrument RPC")
	}
	r, err = decodeSingleResult(r, frame.Result)
	if err == nil && len(r.Instruments) == 1 && r.Instruments[0].Spec.Instrument.ExchangeSymbol != symbol {
		r.Status = "parse_error"
		r.Instruments = nil
		r.Excluded = nil
		r.RawCount = 0
		r.AcceptedCount = 0
		r.ExcludedCount = 0
		return r, fmt.Errorf("instrument identity mismatch")
	}
	return r, err
}
func decodeSingleResult(r ScopeResult, data json.RawMessage) (ScopeResult, error) {
	r.Status = "parse_error"
	wrapper := append([]byte(`{"jsonrpc":"2.0","result":[`), data...)
	wrapper = append(wrapper, []byte(`]}`)...)
	var err error
	r.Instruments, r.Excluded, err = DecodeInstruments(wrapper, r.Scope.Currency, r.Scope.Kind, r.ObservedAt)
	if err != nil {
		return r, err
	}
	for n := range r.Instruments {
		p := &r.Instruments[n]
		p.Spec.EvidenceHash = r.PayloadHash
		p.Rule.PayloadHash = r.PayloadHash
		p.Rule.SourceURL = r.URL
	}
	for n := range r.Excluded {
		r.Excluded[n].PayloadHash = r.PayloadHash
	}
	r.AcceptedCount = uint32(len(r.Instruments))
	r.ExcludedCount = uint32(len(r.Excluded))
	r.RawCount = r.AcceptedCount + r.ExcludedCount
	r.Status = "complete"
	return r, nil
}
