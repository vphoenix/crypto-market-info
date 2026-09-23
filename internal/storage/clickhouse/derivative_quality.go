package clickhouse

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/model"
)

const derivativeQualityColumns = `instrument_id,minute_time,batch_id,signed,row_hash,sampled_bitmap,stream_valid_bitmap,replay_valid_bitmap,market_known_bitmap,market_open_bitmap,source_times,received_times,captured_times,last_snapshot_times,connection_confirmed_times,rule_published_times,market_state_times,connection_epochs,change_ids,trading_rule_ids,market_state_bases,reasons,bid_level_counts,ask_level_counts`

type derivativeQualityRow struct {
	id                         uint32
	minute                     time.Time
	batch                      string
	signed                     bool
	hash                       string
	bits                       [5]uint64
	times                      [7][]*time.Time
	epochs                     []*uuid.UUID
	changes                    []uint64
	rules                      []*string
	bases, reasons, bids, asks []uint8
}

func derivativeQualityValues(b model.DerivativeMinuteBatch, batch string) []any {
	r := derivativeQualityRow{id: b.InstrumentID, minute: b.MinuteTime.UTC(), batch: batch, signed: b.Signed, hash: model.DerivativeBatchHash(b)}
	for _, q := range b.Quality {
		for i, t := range []time.Time{q.SourceTime, q.ReceivedAt, q.CapturedAt, q.LastSnapshotAt, q.ConnectionConfirmedAt, q.RulePublishedAt, q.MarketStateAt} {
			r.times[i] = append(r.times[i], nullableDerivativeTime(t))
		}
		var epoch *uuid.UUID
		if q.Epoch != uuid.Nil {
			v := q.Epoch
			epoch = &v
		}
		r.epochs = append(r.epochs, epoch)
		r.changes = append(r.changes, q.ChangeID)
		var rule *string
		if q.TradingRuleID != "" {
			v := q.TradingRuleID
			rule = &v
		}
		r.rules = append(r.rules, rule)
		r.bases = append(r.bases, q.MarketStateBasis)
		r.reasons = append(r.reasons, uint8(q.Reason))
		r.bids = append(r.bids, q.BidLevels)
		r.asks = append(r.asks, q.AskLevels)
	}
	for second, q := range b.Quality {
		for i, v := range []bool{q.Sampled, q.StreamValid, q.ReplayValid, q.MarketKnown, q.MarketOpen} {
			if v {
				r.bits[i] |= 1 << second
			}
		}
	}
	v := []any{r.id, r.minute, r.batch, r.signed, r.hash}
	for _, b := range r.bits {
		v = append(v, b)
	}
	for _, ts := range r.times {
		v = append(v, ts)
	}
	return append(v, r.epochs, r.changes, r.rules, r.bases, r.reasons, r.bids, r.asks)
}

func (r *derivativeQualityRow) scanArgs() []any {
	v := []any{&r.id, &r.minute, &r.batch, &r.signed, &r.hash}
	for i := range r.bits {
		v = append(v, &r.bits[i])
	}
	for i := range r.times {
		v = append(v, &r.times[i])
	}
	return append(v, &r.epochs, &r.changes, &r.rules, &r.bases, &r.reasons, &r.bids, &r.asks)
}

func (r derivativeQualityRow) quality() ([60]model.DerivativeQuality, error) {
	var result [60]model.DerivativeQuality
	for _, ts := range r.times {
		if len(ts) != 60 {
			return result, fmt.Errorf("incomplete quality time array")
		}
	}
	for _, n := range []int{len(r.epochs), len(r.changes), len(r.rules), len(r.bases), len(r.reasons), len(r.bids), len(r.asks)} {
		if n != 60 {
			return result, fmt.Errorf("incomplete quality array")
		}
	}
	for _, bits := range r.bits {
		if bits&^model.MinuteMask != 0 {
			return result, fmt.Errorf("quality bitmap exceeds minute")
		}
	}
	for i := range result {
		q := &result[i]
		q.Sampled = r.bits[0]&(1<<i) != 0
		q.StreamValid = r.bits[1]&(1<<i) != 0
		q.ReplayValid = r.bits[2]&(1<<i) != 0
		q.MarketKnown = r.bits[3]&(1<<i) != 0
		q.MarketOpen = r.bits[4]&(1<<i) != 0
		q.SourceTime = derivativeTime(r.times[0][i])
		q.ReceivedAt = derivativeTime(r.times[1][i])
		q.CapturedAt = derivativeTime(r.times[2][i])
		q.LastSnapshotAt = derivativeTime(r.times[3][i])
		q.ConnectionConfirmedAt = derivativeTime(r.times[4][i])
		q.RulePublishedAt = derivativeTime(r.times[5][i])
		q.MarketStateAt = derivativeTime(r.times[6][i])
		if r.epochs[i] != nil {
			q.Epoch = *r.epochs[i]
		}
		q.ChangeID = r.changes[i]
		if r.rules[i] != nil {
			q.TradingRuleID = *r.rules[i]
		}
		q.MarketStateBasis = r.bases[i]
		q.Reason = model.DerivativeReason(r.reasons[i])
		q.BidLevels = r.bids[i]
		q.AskLevels = r.asks[i]
	}
	return result, nil
}
func nullableDerivativeTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	v := t.UTC()
	return &v
}
func derivativeTime(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return t.UTC()
}
