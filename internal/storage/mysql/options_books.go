package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/options"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

const derivativeMinuteColumns = "id,instrument_id,minute_time,batch_id,encoding_version,stored_depth,signed,valid_bitmap,delta_bitmap,bid_prices,bid_qtys,ask_prices,ask_qtys"
const derivativeDeltaColumns = "minute_id,second_offset,batch_id,bid_change_prices,bid_change_qtys,ask_change_prices,ask_change_qtys"
const derivativeCommitColumns = "run_id,minute_time,batch_id,origin,evidence_hash,prepared_at,instrument_ids,member_hashes,anchor_count,delta_count"

func derivativeManifest(e options.BookEnvelope) ([]uint32, []string, uint32, uint32) {
	ids, hashes := []uint32{}, []string{}
	var anchors, deltas uint32
	for _, b := range e.Batches {
		ids = append(ids, b.InstrumentID)
		hashes = append(hashes, model.DerivativeBatchHash(b))
		if b.Minute != nil {
			anchors++
		}
		deltas += uint32(len(b.Deltas))
	}
	return ids, hashes, anchors, deltas
}
func derivativeArrays(ls []model.Level) ([]int64, []uint64) {
	p, q := []int64{}, []uint64{}
	for _, l := range ls {
		p = append(p, l.PriceTick)
		q = append(q, l.QtyLot)
	}
	return p, q
}
func derivativeZip(p []int64, q []uint64) []model.Level {
	out := make([]model.Level, len(p))
	for n := range p {
		out[n] = model.Level{PriceTick: p[n], QtyLot: q[n]}
	}
	return out
}
func (c *Client) validateDerivativeReferences(ctx context.Context, books []model.DerivativeMinuteBatch) error {
	ids := make([]uint32, len(books))
	for n, b := range books {
		ids[n] = b.InstrumentID
	}
	specs, e := c.LoadDerivativeSpecs(ctx, ids)
	if e != nil {
		return e
	}
	rules := map[string]uint32{}
	for n, b := range books {
		s := specs[n]
		i := s.Instrument
		if i.ID != b.InstrumentID || i.Exchange != "Deribit" || (i.MarketType != model.MarketOption && i.MarketType != model.MarketOptionCombo && i.MarketType != model.MarketDelivery) || b.Signed != (i.MarketType == model.MarketOptionCombo) || !i.PriceTickSize.Equal(options.StorageUnit()) || !i.QuantityStepSize.Equal(options.StorageUnit()) || !i.ContractMultiplier.Equal(options.StorageUnit().Shift(8)) || !strings.HasSuffix(i.VenueContractVersion, s.DefinitionHash()) {
			return fmt.Errorf("incompatible derivative identity")
		}
		if b.Minute != nil && i.MarketType != model.MarketDelivery && b.Minute.StoredDepth != model.BookDepth {
			return fmt.Errorf("options require ten levels")
		}
		for _, q := range b.Quality {
			if q.TradingRuleID != "" {
				if old, ok := rules[q.TradingRuleID]; ok && old != b.InstrumentID {
					return fmt.Errorf("rule reused across instruments")
				}
				rules[q.TradingRuleID] = b.InstrumentID
			}
		}
	}
	ruleIDs := make([]string, 0, len(rules))
	for id := range rules {
		ruleIDs = append(ruleIDs, id)
	}
	sort.Strings(ruleIDs)
	loadedRules, e := c.LoadDerivativeTradingRules(ctx, ruleIDs)
	if e != nil {
		return e
	}
	for _, id := range ruleIDs {
		owner := rules[id]
		r := loadedRules[id]
		var e error
		if e != nil {
			return e
		}
		if r.InstrumentID != owner {
			return fmt.Errorf("rule owner mismatch")
		}
		for _, b := range books {
			if b.InstrumentID != owner {
				continue
			}
			for sec, q := range b.Quality {
				if q.TradingRuleID == id && (q.RulePublishedAt.Before(r.KnownFrom) || b.MinuteTime.Add(time.Duration(sec)*time.Second).Before(r.EffectiveFrom)) {
					return fmt.Errorf("future rule reference")
				}
			}
		}
	}
	return nil
}
func (c *Client) insertDerivativeBooks(ctx context.Context, tx *sql.Tx, id string, books []model.DerivativeMinuteBatch) error {

	recordsByTable := map[string][]record{}
	add := func(name, cols string, values []any) error {
		cols, rows, e := compactInsertValues(name, cols, [][]any{values})
		if e != nil {
			return e
		}
		r, e := namedRecord(cols, rows[0])
		if e != nil {
			return e
		}
		recordsByTable[name] = append(recordsByTable[name], r)
		return nil
	}
	for _, book := range books {
		for _, d := range book.Deltas {
			if e := add("derivative_book_second_delta", derivativeDeltaColumns, []any{d.MinuteID, d.SecondOffset, id, d.BidChangePrice, d.BidChangeQty, d.AskChangePrice, d.AskChangeQty}); e != nil {
				return e
			}
		}
		if m := book.Minute; m != nil {
			bp, bq := derivativeArrays(m.Bids)
			ap, aq := derivativeArrays(m.Asks)
			if e := add("derivative_book_minute", derivativeMinuteColumns, []any{m.ID, m.InstrumentID, m.MinuteTime, id, m.EncodingVersion, m.StoredDepth, m.Signed, m.ValidBitmap, m.DeltaBitmap, bp, bq, ap, aq}); e != nil {
				return e
			}
		}
		if e := add("derivative_book_quality_minute", derivativeQualityColumns, derivativeQualityValues(book, id)); e != nil {
			return e
		}
	}
	for _, name := range []string{"derivative_book_second_delta", "derivative_book_minute", "derivative_book_quality_minute"} {
		target := name
		if name == "derivative_book_quality_minute" && len(books) > 0 {
			var err error
			target, err = c.qualityWriteTable(ctx, tx, name, qualityMinuteV2, books[0].MinuteTime, id)
			if err != nil {
				return err
			}
		}
		t, e := c.mapping(target)
		if e != nil {
			return e
		}
		if e = putImmutableRows(ctx, tx, c.table(target), t, recordsByTable[name]); e != nil {
			return e
		}
	}
	return nil
}

func (c *Client) WriteDerivativeBookEnvelope(ctx context.Context, e options.BookEnvelope) error {
	if err := e.Validate(); err != nil {
		return err
	}
	e = e.Clone()
	if err := c.validateDerivativeReferences(ctx, e.Batches); err != nil {
		return err
	}
	return c.transaction(ctx, func(tx *sql.Tx) error {
		if err := c.insertDerivativeBooks(ctx, tx, e.ID(), e.Batches); err != nil {
			return err
		}
		ids, hashes, a, d := derivativeManifest(e)
		return c.putValues(ctx, tx, "derivative_book_foundation_commit", derivativeCommitColumns, []any{e.RunID, e.MinuteTime, e.ID(), e.Origin, e.EvidenceHash, e.PreparedAt, ids, hashes, a, d})
	})
}
func (c *Client) LoadDerivativeBookEnvelope(ctx context.Context, run uuid.UUID, minute time.Time) (options.BookEnvelope, error) {
	var e options.BookEnvelope
	if run == uuid.Nil || minute.IsZero() || !minute.Equal(minute.Truncate(time.Minute)) {
		return e, fmt.Errorf("exact run/minute required")
	}
	rows, err := c.records(ctx, c.db, "derivative_book_foundation_commit", "WHERE run_id=? AND minute_time=?", run[:], minute.UTC())
	if err != nil {
		return e, err
	}
	if len(rows) == 0 {
		return e, contracts.ErrNotFound
	}
	if len(rows) != 1 {
		return e, contracts.ErrConflict
	}
	var id string
	var ids []uint32
	var hashes []string
	var a, d uint32
	if err = scanRecord(rows[0], derivativeCommitColumns, &e.RunID, &e.MinuteTime, &id, &e.Origin, &e.EvidenceHash, &e.PreparedAt, &ids, &hashes, &a, &d); err != nil {
		return e, err
	}
	e, err = c.loadDerivativeBookRows(ctx, e, id, ids, hashes, a, d)
	if err != nil {
		return e, err
	}
	if err = e.Validate(); err != nil {
		return e, err
	}
	if e.ID() != id {
		return e, fmt.Errorf("derivative envelope hash mismatch")
	}
	return e, nil
}
func (c *Client) loadDerivativeBookRows(ctx context.Context, e options.BookEnvelope, id string, ids []uint32, hashes []string, anchorCount, deltaCount uint32) (options.BookEnvelope, error) {
	if len(ids) == 0 || len(ids) > 448 || len(ids) != len(hashes) || !model.ValidDigest(id) {
		return e, fmt.Errorf("invalid derivative manifest")
	}
	index := map[uint32]int{}
	minuteIDs := make([]uint64, len(ids))
	for n, x := range ids {
		if x == 0 || (n > 0 && x <= ids[n-1]) || !model.ValidDigest(hashes[n]) {
			return e, fmt.Errorf("invalid derivative membership")
		}
		index[x] = n
		var err error
		minuteIDs[n], err = model.MinuteID(x, e.MinuteTime)
		if err != nil {
			return e, err
		}
		e.Batches = append(e.Batches, model.DerivativeMinuteBatch{InstrumentID: x, MinuteTime: e.MinuteTime})
	}
	// Scope by batch first, so extra members are detected as well as missing ones.
	rows, err := c.loadDerivativeQuality(ctx, e.MinuteTime, id)
	if err != nil {
		return e, err
	}
	if len(rows) != len(ids) {
		return e, fmt.Errorf("quality member count mismatch")
	}
	for _, v := range rows {
		var r derivativeQualityRow
		if err = scanRecord(v, derivativeQualityColumns, r.scanArgs()...); err != nil {
			return e, err
		}
		n, ok := index[r.id]
		if !ok || r.batch != id || r.hash != hashes[n] {
			return e, fmt.Errorf("quality identity mismatch")
		}
		e.Batches[n].Signed = r.signed
		e.Batches[n].Quality, err = r.quality()
		if err != nil {
			return e, err
		}
	}
	rows, err = c.records(ctx, c.db, "derivative_book_minute", "WHERE minute_time=? AND batch_id=? ORDER BY instrument_id", e.MinuteTime, []byte(id))
	if err != nil {
		return e, err
	}
	if uint32(len(rows)) != anchorCount {
		return e, fmt.Errorf("anchor count mismatch")
	}
	for _, v := range rows {
		var m model.DerivativeMinute
		var batch string
		var bp, ap []int64
		var bq, aq []uint64
		if err = scanRecord(v, derivativeMinuteColumns, &m.ID, &m.InstrumentID, &m.MinuteTime, &batch, &m.EncodingVersion, &m.StoredDepth, &m.Signed, &m.ValidBitmap, &m.DeltaBitmap, &bp, &bq, &ap, &aq); err != nil {
			return e, err
		}
		n, ok := index[m.InstrumentID]
		if !ok || e.Batches[n].Minute != nil || batch != id || len(bp) != len(bq) || len(ap) != len(aq) {
			return e, fmt.Errorf("malformed derivative anchor")
		}
		m.Bids = derivativeZip(bp, bq)
		m.Asks = derivativeZip(ap, aq)
		e.Batches[n].Minute = &m
	}
	args := make([]any, len(minuteIDs))
	for n, x := range minuteIDs {
		args[n] = x
	}
	args = append(args, []byte(id))
	rows, err = c.records(ctx, c.db, "derivative_book_second_delta", "WHERE minute_id IN ("+placeholders(len(ids))+") AND batch_id=? ORDER BY minute_id,second_offset", args...)
	if err != nil {
		return e, err
	}
	if uint32(len(rows)) != deltaCount {
		return e, fmt.Errorf("delta count mismatch")
	}
	for _, v := range rows {
		var d model.BookDelta
		var batch string
		if err = scanRecord(v, derivativeDeltaColumns, &d.MinuteID, &d.SecondOffset, &batch, &d.BidChangePrice, &d.BidChangeQty, &d.AskChangePrice, &d.AskChangeQty); err != nil {
			return e, err
		}
		n, ok := index[uint32(d.MinuteID)]
		if !ok || d.MinuteID != minuteIDs[n] || batch != id {
			return e, fmt.Errorf("delta identity mismatch")
		}
		e.Batches[n].Deltas = append(e.Batches[n].Deltas, d)
	}
	if err = options.ValidateBookBatches(e.MinuteTime, e.Batches); err != nil {
		return e, err
	}
	for n, b := range e.Batches {
		if model.DerivativeBatchHash(b) != hashes[n] {
			return e, fmt.Errorf("derivative member digest mismatch")
		}
	}
	return e, c.validateDerivativeReferences(ctx, e.Batches)
}
