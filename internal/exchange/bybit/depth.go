package bybit

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
)

const orderBookDepth = 1000

type DepthUpdate struct {
	Snapshot      bool
	UpdateID      int64
	CrossSequence int64
	SourceTime    time.Time
	Bids          []model.Level
	Asks          []model.Level
}

type depthEnvelope struct {
	Topic     string          `json:"topic"`
	Type      string          `json:"type"`
	Timestamp *int64          `json:"ts"`
	CTS       *int64          `json:"cts"`
	Data      json.RawMessage `json:"data"`
}

type depthWire struct {
	Symbol        string          `json:"s"`
	Bids          json.RawMessage `json:"b"`
	Asks          json.RawMessage `json:"a"`
	UpdateID      *int64          `json:"u"`
	CrossSequence *int64          `json:"seq"`
}

func ParseDepth(payload []byte, instrument model.Instrument) (DepthUpdate, error) {
	var envelope depthEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return DepthUpdate{}, fmt.Errorf("Bybit orderbook JSON: %w", err)
	}
	wantTopic := fmt.Sprintf("orderbook.%d.%s", orderBookDepth, instrument.ExchangeSymbol)
	if envelope.Topic != wantTopic {
		return DepthUpdate{}, fmt.Errorf("Bybit orderbook topic=%q, want %q", envelope.Topic, wantTopic)
	}
	snapshot := envelope.Type == "snapshot"
	if !snapshot && envelope.Type != "delta" {
		return DepthUpdate{}, fmt.Errorf("Bybit orderbook type must be snapshot or delta")
	}
	if envelope.Timestamp == nil || *envelope.Timestamp <= 0 || envelope.CTS == nil || *envelope.CTS <= 0 {
		return DepthUpdate{}, fmt.Errorf("Bybit orderbook ts and cts must be positive milliseconds")
	}
	trimmed := bytes.TrimSpace(envelope.Data)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return DepthUpdate{}, fmt.Errorf("Bybit orderbook data is required")
	}
	var wire depthWire
	if err := json.Unmarshal(trimmed, &wire); err != nil {
		return DepthUpdate{}, fmt.Errorf("Bybit orderbook data: %w", err)
	}
	if wire.Symbol != instrument.ExchangeSymbol {
		return DepthUpdate{}, fmt.Errorf("Bybit orderbook symbol=%q, want %q", wire.Symbol, instrument.ExchangeSymbol)
	}
	if wire.UpdateID == nil || *wire.UpdateID <= 0 || wire.CrossSequence == nil || *wire.CrossSequence <= 0 {
		return DepthUpdate{}, fmt.Errorf("Bybit orderbook u and seq must be positive integers")
	}
	if *wire.UpdateID == 1 && !snapshot {
		return DepthUpdate{}, fmt.Errorf("Bybit orderbook u=1 must be a snapshot")
	}
	bids, err := parseBookRows(wire.Bids, instrument, "bids", !snapshot)
	if err != nil {
		return DepthUpdate{}, err
	}
	asks, err := parseBookRows(wire.Asks, instrument, "asks", !snapshot)
	if err != nil {
		return DepthUpdate{}, err
	}
	if snapshot && (len(bids) == 0 || len(asks) == 0) {
		return DepthUpdate{}, fmt.Errorf("Bybit orderbook snapshot requires both sides")
	}
	return DepthUpdate{Snapshot: snapshot, UpdateID: *wire.UpdateID, CrossSequence: *wire.CrossSequence, SourceTime: time.UnixMilli(*envelope.CTS).UTC(), Bids: bids, Asks: asks}, nil
}

func parseBookRows(raw json.RawMessage, instrument model.Instrument, side string, allowZero bool) ([]model.Level, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, fmt.Errorf("Bybit orderbook %s must be a non-null array", side)
	}
	var rows [][]string
	if err := json.Unmarshal(trimmed, &rows); err != nil {
		return nil, fmt.Errorf("Bybit orderbook %s: %w", side, err)
	}
	levels := make([]model.Level, len(rows))
	seen := make(map[int64]struct{}, len(rows))
	for index, row := range rows {
		if len(row) != 2 {
			return nil, fmt.Errorf("Bybit orderbook %s row %d must contain price and quantity", side, index)
		}
		price, err := model.PriceTick(row[0], instrument.PriceTickSize)
		if err != nil {
			return nil, fmt.Errorf("Bybit orderbook %s row %d: %w", side, index, err)
		}
		quantity, err := model.QuantityLot(row[1], instrument.QuantityStepSize)
		if err != nil {
			return nil, fmt.Errorf("Bybit orderbook %s row %d: %w", side, index, err)
		}
		if quantity == 0 && !allowZero {
			return nil, fmt.Errorf("Bybit orderbook %s snapshot row %d has zero quantity", side, index)
		}
		if _, exists := seen[price]; exists {
			return nil, fmt.Errorf("Bybit orderbook %s contains duplicate price", side)
		}
		seen[price] = struct{}{}
		levels[index] = model.Level{PriceTick: price, QtyLot: quantity}
	}
	return levels, nil
}
