package okx

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
)

type DepthUpdate struct {
	Snapshot      bool
	PreviousSeqID int64
	SeqID         int64
	SourceTime    time.Time
	Bids          []model.Level
	Asks          []model.Level
}

type depthEnvelope struct {
	Arg struct {
		Channel string `json:"channel"`
		InstID  string `json:"instId"`
	} `json:"arg"`
	Action json.RawMessage   `json:"action"`
	Data   []json.RawMessage `json:"data"`
}

type depthWire struct {
	Asks      json.RawMessage `json:"asks"`
	Bids      json.RawMessage `json:"bids"`
	Timestamp json.RawMessage `json:"ts"`
	PrevSeqID json.RawMessage `json:"prevSeqId"`
	SeqID     json.RawMessage `json:"seqId"`
}

func ParseDepth(payload []byte, instrument model.Instrument) (DepthUpdate, error) {
	var envelope depthEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return DepthUpdate{}, fmt.Errorf("OKX books JSON: %w", err)
	}
	if envelope.Arg.Channel != "books" || envelope.Arg.InstID != instrument.ExchangeSymbol {
		return DepthUpdate{}, fmt.Errorf("OKX books identity mismatch")
	}
	var action string
	if len(bytes.TrimSpace(envelope.Action)) == 0 || json.Unmarshal(envelope.Action, &action) != nil || (action != "snapshot" && action != "update") {
		return DepthUpdate{}, fmt.Errorf("OKX books action must be snapshot or update")
	}
	if len(envelope.Data) != 1 {
		return DepthUpdate{}, fmt.Errorf("OKX books data must contain exactly one item")
	}
	var wire depthWire
	if err := json.Unmarshal(envelope.Data[0], &wire); err != nil {
		return DepthUpdate{}, fmt.Errorf("OKX books data: %w", err)
	}
	previous, err := integer(wire.PrevSeqID, "prevSeqId", true)
	if err != nil {
		return DepthUpdate{}, err
	}
	sequence, err := integer(wire.SeqID, "seqId", false)
	if err != nil {
		return DepthUpdate{}, err
	}
	var timestamp string
	if json.Unmarshal(wire.Timestamp, &timestamp) != nil {
		return DepthUpdate{}, fmt.Errorf("OKX books ts must be a positive integer string")
	}
	milliseconds, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil || milliseconds <= 0 {
		return DepthUpdate{}, fmt.Errorf("OKX books ts must be a positive integer string")
	}
	snapshot := action == "snapshot"
	if snapshot && previous != -1 {
		return DepthUpdate{}, fmt.Errorf("OKX books snapshot prevSeqId must be -1")
	}
	if !snapshot && previous < 0 {
		return DepthUpdate{}, fmt.Errorf("OKX books update prevSeqId must be non-negative")
	}
	bids, err := rows(wire.Bids, instrument, "bids", !snapshot)
	if err != nil {
		return DepthUpdate{}, err
	}
	asks, err := rows(wire.Asks, instrument, "asks", !snapshot)
	if err != nil {
		return DepthUpdate{}, err
	}
	if snapshot && (len(bids) == 0 || len(asks) == 0) {
		return DepthUpdate{}, fmt.Errorf("OKX books snapshot requires both sides")
	}
	return DepthUpdate{Snapshot: snapshot, PreviousSeqID: previous, SeqID: sequence, SourceTime: time.UnixMilli(milliseconds).UTC(), Bids: bids, Asks: asks}, nil
}

func rows(raw json.RawMessage, instrument model.Instrument, side string, allowZero bool) ([]model.Level, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, fmt.Errorf("OKX books %s must be a non-null array", side)
	}
	var rows [][]string
	if err := json.Unmarshal(trimmed, &rows); err != nil {
		return nil, fmt.Errorf("OKX books %s: %w", side, err)
	}
	levels := make([]model.Level, len(rows))
	seen := make(map[int64]struct{}, len(rows))
	for index, row := range rows {
		if len(row) != 4 {
			return nil, fmt.Errorf("OKX books %s row %d must contain four fields", side, index)
		}
		if row[2] != "0" {
			return nil, fmt.Errorf("OKX books %s row %d deprecated field must be 0", side, index)
		}
		if _, err := strconv.ParseUint(row[3], 10, 64); err != nil {
			return nil, fmt.Errorf("OKX books %s row %d order count: %w", side, index, err)
		}
		price, err := model.PriceTick(row[0], instrument.PriceTickSize)
		if err != nil {
			return nil, fmt.Errorf("OKX books %s row %d: %w", side, index, err)
		}
		qty, err := model.QuantityLot(row[1], instrument.QuantityStepSize)
		if err != nil {
			return nil, fmt.Errorf("OKX books %s row %d: %w", side, index, err)
		}
		if qty == 0 && !allowZero {
			return nil, fmt.Errorf("OKX books %s snapshot row %d has zero quantity", side, index)
		}
		if _, exists := seen[price]; exists {
			return nil, fmt.Errorf("OKX books %s contains duplicate price", side)
		}
		seen[price] = struct{}{}
		levels[index] = model.Level{PriceTick: price, QtyLot: qty}
	}
	return levels, nil
}

func integer(raw json.RawMessage, field string, negative bool) (int64, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return 0, fmt.Errorf("OKX books %s is required", field)
	}
	value := string(trimmed)
	if trimmed[0] == '"' && json.Unmarshal(trimmed, &value) != nil {
		return 0, fmt.Errorf("OKX books %s must be an integer", field)
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || (!negative && parsed < 0) {
		return 0, fmt.Errorf("OKX books %s must be a valid integer", field)
	}
	return parsed, nil
}
