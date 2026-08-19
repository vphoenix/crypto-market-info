package binance

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
)

type DepthUpdate struct {
	FirstUpdateID    int64
	FinalUpdateID    int64
	PreviousUpdateID int64
	SourceTime       time.Time
	Bids             []model.Level
	Asks             []model.Level
}

type snapshotWire struct {
	LastUpdateID *int64          `json:"lastUpdateId"`
	Transaction  *int64          `json:"T"`
	Bids         json.RawMessage `json:"bids"`
	Asks         json.RawMessage `json:"asks"`
}

func ParseDepthSnapshot(payload []byte, instrument model.Instrument, receiveTime time.Time) (model.BookSnapshot, error) {
	if receiveTime.IsZero() {
		return model.BookSnapshot{}, fmt.Errorf("Binance depth snapshot requires receive time")
	}
	var wire snapshotWire
	if err := json.Unmarshal(payload, &wire); err != nil {
		return model.BookSnapshot{}, fmt.Errorf("Binance depth snapshot JSON: %w", err)
	}
	if wire.LastUpdateID == nil || *wire.LastUpdateID <= 0 {
		return model.BookSnapshot{}, fmt.Errorf("Binance depth snapshot lastUpdateId must be positive")
	}
	bids, err := parseRows(wire.Bids, instrument, "bids", false)
	if err != nil {
		return model.BookSnapshot{}, err
	}
	asks, err := parseRows(wire.Asks, instrument, "asks", false)
	if err != nil {
		return model.BookSnapshot{}, err
	}
	sourceTime := receiveTime.UTC()
	if wire.Transaction != nil {
		if *wire.Transaction <= 0 {
			return model.BookSnapshot{}, fmt.Errorf("Binance depth snapshot T must be positive")
		}
		sourceTime = time.UnixMilli(*wire.Transaction).UTC()
	}
	snapshot := model.BookSnapshot{InstrumentID: instrument.ID, SourceTime: sourceTime, Sequence: *wire.LastUpdateID, Bids: bids, Asks: asks}
	if err := snapshot.Validate(1000); err != nil {
		return model.BookSnapshot{}, fmt.Errorf("Binance depth snapshot: %w", err)
	}
	return snapshot, nil
}

type diffEnvelope struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

type diffWire struct {
	EventType   string          `json:"e"`
	EventTime   *int64          `json:"E"`
	Transaction *int64          `json:"T"`
	Symbol      string          `json:"s"`
	First       *int64          `json:"U"`
	Final       *int64          `json:"u"`
	Previous    *int64          `json:"pu"`
	Bids        json.RawMessage `json:"b"`
	Asks        json.RawMessage `json:"a"`
}

func ParseDepthUpdate(payload []byte, instrument model.Instrument, futures bool, receiveTime time.Time) (DepthUpdate, error) {
	if receiveTime.IsZero() {
		return DepthUpdate{}, fmt.Errorf("Binance diff-depth requires receive time")
	}
	message := json.RawMessage(payload)
	var envelope diffEnvelope
	if json.Unmarshal(payload, &envelope) == nil && len(bytes.TrimSpace(envelope.Data)) > 0 {
		message = envelope.Data
		streamSymbol := strings.Split(envelope.Stream, "@")[0]
		if !strings.EqualFold(streamSymbol, instrument.ExchangeSymbol) {
			return DepthUpdate{}, fmt.Errorf("Binance combined stream symbol mismatch")
		}
	}
	var wire diffWire
	if err := json.Unmarshal(message, &wire); err != nil {
		return DepthUpdate{}, fmt.Errorf("Binance diff-depth JSON: %w", err)
	}
	if wire.EventType != "depthUpdate" || wire.Symbol != instrument.ExchangeSymbol {
		return DepthUpdate{}, fmt.Errorf("Binance diff-depth identity mismatch")
	}
	if wire.First == nil || wire.Final == nil || *wire.First <= 0 || *wire.Final < *wire.First {
		return DepthUpdate{}, fmt.Errorf("Binance diff-depth U/u are invalid")
	}
	previous := int64(0)
	if futures {
		if wire.Previous == nil || *wire.Previous <= 0 {
			return DepthUpdate{}, fmt.Errorf("Binance futures diff-depth pu is invalid")
		}
		previous = *wire.Previous
	}
	bids, err := parseRows(wire.Bids, instrument, "b", true)
	if err != nil {
		return DepthUpdate{}, err
	}
	asks, err := parseRows(wire.Asks, instrument, "a", true)
	if err != nil {
		return DepthUpdate{}, err
	}
	milliseconds := wire.EventTime
	if futures {
		milliseconds = wire.Transaction
	}
	if milliseconds == nil || *milliseconds <= 0 {
		return DepthUpdate{}, fmt.Errorf("Binance diff-depth source timestamp is invalid")
	}
	return DepthUpdate{FirstUpdateID: *wire.First, FinalUpdateID: *wire.Final, PreviousUpdateID: previous,
		SourceTime: time.UnixMilli(*milliseconds).UTC(), Bids: bids, Asks: asks}, nil
}

func parseRows(raw json.RawMessage, instrument model.Instrument, side string, allowZero bool) ([]model.Level, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, fmt.Errorf("Binance depth %s must be a non-null array", side)
	}
	var rows [][]string
	if err := json.Unmarshal(trimmed, &rows); err != nil {
		return nil, fmt.Errorf("Binance depth %s: %w", side, err)
	}
	levels := make([]model.Level, len(rows))
	seen := make(map[int64]struct{}, len(rows))
	for index, row := range rows {
		if len(row) != 2 {
			return nil, fmt.Errorf("Binance depth %s row %d must contain price and quantity", side, index)
		}
		price, err := model.PriceTick(row[0], instrument.PriceTickSize)
		if err != nil {
			return nil, fmt.Errorf("Binance depth %s row %d: %w", side, index, err)
		}
		qty, err := model.QuantityLot(row[1], instrument.QuantityStepSize)
		if err != nil {
			return nil, fmt.Errorf("Binance depth %s row %d: %w", side, index, err)
		}
		if qty == 0 && !allowZero {
			return nil, fmt.Errorf("Binance depth %s snapshot row %d has zero quantity", side, index)
		}
		if _, exists := seen[price]; exists {
			return nil, fmt.Errorf("Binance depth %s contains duplicate price", side)
		}
		seen[price] = struct{}{}
		levels[index] = model.Level{PriceTick: price, QtyLot: qty}
	}
	return levels, nil
}
