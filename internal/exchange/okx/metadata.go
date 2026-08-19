package okx

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/model"
)

type instrumentsEnvelope struct {
	Code string          `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data"`
}

type instrumentWire struct {
	InstType  string `json:"instType"`
	InstID    string `json:"instId"`
	State     string `json:"state"`
	BaseCcy   string `json:"baseCcy"`
	QuoteCcy  string `json:"quoteCcy"`
	SettleCcy string `json:"settleCcy"`
	CtType    string `json:"ctType"`
	CtVal     string `json:"ctVal"`
	CtMult    string `json:"ctMult"`
	CtValCcy  string `json:"ctValCcy"`
	TickSz    string `json:"tickSz"`
	LotSz     string `json:"lotSz"`
}

func ParseInstruments(payload []byte, marketType model.MarketType) ([]model.Instrument, error) {
	if marketType != model.MarketSpot && marketType != model.MarketPerpetual {
		return nil, fmt.Errorf("OKX first version supports spot and perpetual metadata")
	}
	var envelope instrumentsEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return nil, fmt.Errorf("OKX instruments JSON: %w", err)
	}
	if envelope.Code != "0" {
		return nil, fmt.Errorf("OKX instruments code=%q msg=%q", envelope.Code, envelope.Msg)
	}
	trimmed := bytes.TrimSpace(envelope.Data)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, fmt.Errorf("OKX instruments data must be a non-null array")
	}
	var wires []instrumentWire
	if err := json.Unmarshal(trimmed, &wires); err != nil {
		return nil, fmt.Errorf("OKX instruments data: %w", err)
	}
	out := make([]model.Instrument, 0, len(wires))
	seen := make(map[string]struct{})
	for _, wire := range wires {
		if wire.State != "live" {
			continue
		}
		if marketType == model.MarketSpot && wire.InstType != "SPOT" {
			continue
		}
		if marketType == model.MarketPerpetual && (wire.InstType != "SWAP" || wire.CtType != "linear" || wire.SettleCcy != "USDT") {
			continue
		}
		instrument, err := mapInstrument(wire, marketType)
		if err != nil {
			return nil, err
		}
		if _, exists := seen[instrument.ExchangeSymbol]; exists {
			return nil, fmt.Errorf("OKX duplicate live instId %q", instrument.ExchangeSymbol)
		}
		seen[instrument.ExchangeSymbol] = struct{}{}
		out = append(out, instrument)
	}
	return out, nil
}

func mapInstrument(wire instrumentWire, marketType model.MarketType) (model.Instrument, error) {
	if wire.InstID == "" || strings.TrimSpace(wire.InstID) != wire.InstID {
		return model.Instrument{}, fmt.Errorf("OKX instruments requires exact instId")
	}
	parts := strings.Split(wire.InstID, "-")
	base, quote := wire.BaseCcy, wire.QuoteCcy
	if marketType == model.MarketPerpetual {
		if len(parts) != 3 || parts[2] != "SWAP" || parts[0] == "" || parts[1] == "" {
			return model.Instrument{}, fmt.Errorf("OKX unsupported swap instId %q", wire.InstID)
		}
		if base == "" {
			base = parts[0]
		}
		if quote == "" {
			quote = parts[1]
		}
	} else if len(parts) != 2 || parts[0] != base || parts[1] != quote {
		return model.Instrument{}, fmt.Errorf("OKX spot instId/baseCcy/quoteCcy mismatch for %q", wire.InstID)
	}
	if base == "" || quote == "" || strings.TrimSpace(base) != base || strings.TrimSpace(quote) != quote {
		return model.Instrument{}, fmt.Errorf("OKX %s requires exact base and quote currencies", wire.InstID)
	}
	tick, err := model.ParsePositiveDecimal(wire.TickSz, "tickSz")
	if err != nil {
		return model.Instrument{}, fmt.Errorf("OKX %s: %w", wire.InstID, err)
	}
	step, err := model.ParsePositiveDecimal(wire.LotSz, "lotSz")
	if err != nil {
		return model.Instrument{}, fmt.Errorf("OKX %s: %w", wire.InstID, err)
	}
	instrument := model.Instrument{Exchange: "OKX", MarketType: marketType, ExchangeSymbol: wire.InstID, BaseAsset: base, QuoteAsset: quote,
		ContractMultiplier: decimal.NewFromInt(1), PriceTickSize: tick, QuantityStepSize: step}
	if marketType == model.MarketPerpetual {
		if wire.CtValCcy != base {
			return model.Instrument{}, fmt.Errorf("OKX %s contract value currency %q is not base asset %q", wire.InstID, wire.CtValCcy, base)
		}
		ctVal, err := model.ParsePositiveDecimal(wire.CtVal, "ctVal")
		if err != nil {
			return model.Instrument{}, fmt.Errorf("OKX %s: %w", wire.InstID, err)
		}
		ctMult, err := model.ParsePositiveDecimal(wire.CtMult, "ctMult")
		if err != nil {
			return model.Instrument{}, fmt.Errorf("OKX %s: %w", wire.InstID, err)
		}
		instrument.ContractMultiplier = ctVal.Mul(ctMult)
		settle := wire.SettleCcy
		instrument.SettleAsset = &settle
	}
	if err := instrument.ValidateDefinition(); err != nil {
		return model.Instrument{}, fmt.Errorf("OKX %s: %w", wire.InstID, err)
	}
	return instrument, nil
}
