package deribit

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/options"
	"github.com/vphoenix/crypto-market-info/internal/orderbook"
)

// DecodeBook accepts ONLY the ungrouped public 100ms full book protocol. Its
// caller must invalidate the affected stream on every returned parse error.
func DecodeBook(raw []byte, instrument model.Instrument, epoch uuid.UUID, received time.Time) (orderbook.DerivativeUpdate, error) {
	var e struct {
		JSONRPC string `json:"jsonrpc"`
		Method  string `json:"method"`
		Params  struct {
			Channel string `json:"channel"`
			Data    struct {
				Type       string               `json:"type"`
				Instrument string               `json:"instrument_name"`
				Timestamp  *int64               `json:"timestamp"`
				ChangeID   *uint64              `json:"change_id"`
				PrevID     *uint64              `json:"prev_change_id"`
				Bids       *[][]json.RawMessage `json:"bids"`
				Asks       *[][]json.RawMessage `json:"asks"`
			} `json:"data"`
		} `json:"params"`
	}
	if err := instrument.Validate(); err != nil {
		return orderbook.DerivativeUpdate{}, err
	}
	if instrument.Exchange != "Deribit" || (instrument.MarketType != model.MarketOption && instrument.MarketType != model.MarketOptionCombo && instrument.MarketType != model.MarketDelivery) || !instrument.PriceTickSize.Equal(options.StorageUnit()) || !instrument.QuantityStepSize.Equal(options.StorageUnit()) {
		return orderbook.DerivativeUpdate{}, fmt.Errorf("unsupported book normalization")
	}
	if epoch == uuid.Nil || received.IsZero() {
		return orderbook.DerivativeUpdate{}, fmt.Errorf("missing ingress provenance")
	}
	if err := decode(raw, &e); err != nil {
		return orderbook.DerivativeUpdate{}, err
	}
	d := e.Params.Data
	if e.JSONRPC != "2.0" || e.Method != "subscription" || e.Params.Channel != "book."+instrument.ExchangeSymbol+".100ms" || d.Instrument != instrument.ExchangeSymbol || d.Timestamp == nil || *d.Timestamp <= 0 || d.ChangeID == nil || d.Bids == nil || d.Asks == nil {
		return orderbook.DerivativeUpdate{}, fmt.Errorf("missing/incorrect full book envelope")
	}
	if d.Type != "snapshot" && d.Type != "change" {
		return orderbook.DerivativeUpdate{}, fmt.Errorf("unknown book message type")
	}
	if d.Type == "change" && d.PrevID == nil {
		return orderbook.DerivativeUpdate{}, fmt.Errorf("change lacks predecessor")
	}
	u := orderbook.DerivativeUpdate{InstrumentID: instrument.ID, Snapshot: d.Type == "snapshot", Epoch: epoch, ChangeID: *d.ChangeID, SourceTime: time.UnixMilli(*d.Timestamp).UTC(), ReceivedAt: received.UTC()}
	if d.PrevID != nil {
		u.PrevChangeID = *d.PrevID
	}
	var err error
	u.Bids, err = decodeLevels(*d.Bids, instrument.MarketType == model.MarketOptionCombo)
	if err != nil {
		return u, err
	}
	u.Asks, err = decodeLevels(*d.Asks, instrument.MarketType == model.MarketOptionCombo)
	return u, err
}

func decodeLevels(rows [][]json.RawMessage, signed bool) ([]orderbook.DerivativeChangeLevel, error) {
	if len(rows) > 40000 {
		return nil, fmt.Errorf("book action count exceeds budget")
	}
	result := make([]orderbook.DerivativeChangeLevel, 0, len(rows))
	for _, r := range rows {
		if len(r) != 3 {
			return nil, fmt.Errorf("full book requires action/price/amount triples")
		}
		var action string
		if err := json.Unmarshal(r[0], &action); err != nil {
			return nil, err
		}
		var a orderbook.DerivativeAction
		switch action {
		case "new":
			a = orderbook.DerivativeNew
		case "change":
			a = orderbook.DerivativeChange
		case "delete":
			a = orderbook.DerivativeDelete
		default:
			return nil, fmt.Errorf("unknown book action")
		}
		p, err := options.Price(string(r[1]), signed)
		if err != nil {
			return nil, err
		}
		q, err := options.Quantity(string(r[2]))
		if err != nil {
			return nil, err
		}
		if (a == orderbook.DerivativeDelete) != (q == 0) {
			return nil, fmt.Errorf("action/amount mismatch")
		}
		result = append(result, orderbook.DerivativeChangeLevel{Action: a, Level: model.Level{PriceTick: p, QtyLot: q}})
	}
	return result, nil
}
