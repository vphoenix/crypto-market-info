package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/options"
)

func (c *Client) LoadDerivativeTradingRule(ctx context.Context, id string) (options.TradingRule, error) {
	var r options.TradingRule
	if !model.ValidDigest(id) {
		return r, fmt.Errorf("invalid trading rule ID")
	}
	var above, ticks []decimal.Decimal
	err := c.conn.QueryRow(ctx, `SELECT instrument_id,source_tick_size,min_trade_amount,amount_step,band_above_prices,band_tick_sizes,observed_at,known_from,effective_from,effective_time_basis,source_url,payload_hash FROM `+c.table("derivative_trading_rule")+` FINAL WHERE trading_rule_id=?`, id).Scan(&r.InstrumentID, &r.SourceTick, &r.MinAmount, &r.AmountStep, &above, &ticks, &r.ObservedAt, &r.KnownFrom, &r.EffectiveFrom, &r.EffectiveTimeBasis, &r.SourceURL, &r.PayloadHash)
	if errors.Is(err, sql.ErrNoRows) {
		return r, ErrNotFound
	}
	if err != nil {
		return r, err
	}
	if len(above) != len(ticks) {
		return r, fmt.Errorf("incomplete trading rule bands")
	}
	r.ObservedAt, r.KnownFrom, r.EffectiveFrom = r.ObservedAt.UTC(), r.KnownFrom.UTC(), r.EffectiveFrom.UTC()
	for n := range above {
		r.Bands = append(r.Bands, options.TickBand{AbovePrice: above[n], TickSize: ticks[n]})
	}
	if err := r.Validate(); err != nil {
		return r, err
	}
	if r.ID() != id {
		return r, fmt.Errorf("trading rule digest mismatch")
	}
	return r, nil
}
