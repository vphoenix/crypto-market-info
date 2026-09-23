package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/options"
)

const derivativeMinuteColumns = `id,instrument_id,minute_time,batch_id,encoding_version,stored_depth,signed,valid_bitmap,delta_bitmap,bid_prices,bid_qtys,ask_prices,ask_qtys`
const derivativeDeltaColumns = `minute_id,second_offset,batch_id,bid_change_prices,bid_change_qtys,ask_change_prices,ask_change_qtys`
const derivativeCommitColumns = `run_id,minute_time,batch_id,origin,evidence_hash,prepared_at,instrument_ids,member_hashes,anchor_count,delta_count`

// WriteDerivativeBookEnvelope writes offline fixture/synthetic books only. It
// is not wired into app.Run. Callers must keep one writer process per database.
func (c *Client) WriteDerivativeBookEnvelope(ctx context.Context, input options.BookEnvelope) error {
	if err := input.Validate(); err != nil {
		return err
	}
	e := input.Clone()
	id := e.ID()
	c.derivativeMu.Lock()
	defer c.derivativeMu.Unlock()
	var existing string
	err := c.conn.QueryRow(ctx, `SELECT batch_id FROM `+c.table("derivative_book_foundation_commit")+` FINAL WHERE run_id=? AND minute_time=?`, e.RunID, e.MinuteTime.UTC()).Scan(&existing)
	if err == nil {
		if existing != id {
			return fmt.Errorf("immutable derivative minute commit conflict")
		}
		_, err := c.LoadDerivativeBookEnvelope(ctx, e.RunID, e.MinuteTime)
		return err
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err := c.validateDerivativeReferences(ctx, e.Batches); err != nil {
		return err
	}
	return c.retryWrite(ctx, func(writeCtx context.Context) error {
		for offset := 0; offset < len(e.Batches); offset += 100 {
			chunk := e.Batches[offset:min(offset+100, len(e.Batches))]
			if err := c.insertDerivativeDeltas(writeCtx, id, chunk); err != nil {
				return err
			}
			if err := c.derivativeInserted("delta"); err != nil {
				return err
			}
			if err := c.insertDerivativeMinutes(writeCtx, id, chunk); err != nil {
				return err
			}
			if err := c.derivativeInserted("minute"); err != nil {
				return err
			}
			if err := c.insertDerivativeQuality(writeCtx, id, chunk); err != nil {
				return err
			}
			if err := c.derivativeInserted("quality"); err != nil {
				return err
			}
		}
		ids, hashes, anchors, deltas := derivativeManifest(e)
		if err := c.insertDerivativeRow(writeCtx, "derivative_book_foundation_commit", derivativeCommitColumns, e.RunID, e.MinuteTime.UTC(), id, e.Origin, e.EvidenceHash, e.PreparedAt.UTC(), ids, hashes, anchors, deltas); err != nil {
			return err
		}
		return c.derivativeInserted("commit")
	})
}

func (c *Client) derivativeInserted(stage string) error {
	if c.derivativeAfterInsert != nil {
		return c.derivativeAfterInsert(stage)
	}
	return nil
}

func derivativeManifest(e options.BookEnvelope) ([]uint32, []string, uint32, uint32) {
	ids, hashes := make([]uint32, 0, len(e.Batches)), make([]string, 0, len(e.Batches))
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
	p, q := make([]int64, 0, len(ls)), make([]uint64, 0, len(ls))
	for _, l := range ls {
		p = append(p, l.PriceTick)
		q = append(q, l.QtyLot)
	}
	return p, q
}

func (c *Client) insertDerivativeDeltas(ctx context.Context, id string, books []model.DerivativeMinuteBatch) error {
	count := 0
	for _, b := range books {
		count += len(b.Deltas)
	}
	if count == 0 {
		return nil
	}
	b, err := c.conn.PrepareBatch(ctx, `INSERT INTO `+c.table("derivative_book_second_delta")+` (`+derivativeDeltaColumns+`)`)
	if err != nil {
		return err
	}
	defer b.Abort()
	for _, book := range books {
		for _, d := range book.Deltas {
			if err := b.Append(d.MinuteID, d.SecondOffset, id, d.BidChangePrice, d.BidChangeQty, d.AskChangePrice, d.AskChangeQty); err != nil {
				return err
			}
		}
	}
	return b.Send()
}

func (c *Client) validateDerivativeReferences(ctx context.Context, books []model.DerivativeMinuteBatch) error {
	// Every reference must resolve BEFORE the first market-data insert.
	instruments, err := c.Instruments(ctx)
	if err != nil {
		return err
	}
	byID := make(map[uint32]model.Instrument, len(instruments))
	for _, i := range instruments {
		byID[i.ID] = i
	}
	rules := map[string]uint32{}
	for _, b := range books {
		i, ok := byID[b.InstrumentID]
		if !ok || i.Exchange != "Deribit" || (i.MarketType != model.MarketOption && i.MarketType != model.MarketOptionCombo && i.MarketType != model.MarketDelivery) || b.Signed != (i.MarketType == model.MarketOptionCombo) || !i.PriceTickSize.Equal(options.StorageUnit()) || !i.QuantityStepSize.Equal(options.StorageUnit()) || !i.ContractMultiplier.Equal(options.StorageUnit().Shift(8)) {
			return fmt.Errorf("unregistered or incompatible derivative %d", b.InstrumentID)
		}
		var definitionHash string
		if err := c.conn.QueryRow(ctx, `SELECT definition_hash FROM `+c.table("derivative_contract_spec")+` FINAL WHERE instrument_id=?`, i.ID).Scan(&definitionHash); err != nil {
			return fmt.Errorf("missing derivative spec: %w", err)
		}
		if len(i.VenueContractVersion) < 65 || i.VenueContractVersion[len(i.VenueContractVersion)-64:] != definitionHash {
			return fmt.Errorf("spec identity mismatch")
		}
		for _, q := range b.Quality {
			if q.TradingRuleID != "" {
				if owner, ok := rules[q.TradingRuleID]; ok && owner != b.InstrumentID {
					return fmt.Errorf("rule reused across instruments")
				}
				rules[q.TradingRuleID] = b.InstrumentID
			}
		}
	}
	for rule, owner := range rules {
		var found uint32
		var known, effective time.Time
		if err := c.conn.QueryRow(ctx, `SELECT instrument_id,known_from,effective_from FROM `+c.table("derivative_trading_rule")+` FINAL WHERE trading_rule_id=?`, rule).Scan(&found, &known, &effective); err != nil {
			return err
		}
		if found != owner {
			return fmt.Errorf("rule reference instrument mismatch")
		}
		for _, b := range books {
			if b.InstrumentID != owner {
				continue
			}
			for second, q := range b.Quality {
				if q.TradingRuleID == rule && (q.RulePublishedAt.Before(known) || b.MinuteTime.Add(time.Duration(second)*time.Second).Before(effective)) {
					return fmt.Errorf("rule reference precedes knowledge/publication or effectiveness")
				}
			}
		}
	}
	return nil
}
func (c *Client) insertDerivativeMinutes(ctx context.Context, id string, books []model.DerivativeMinuteBatch) error {
	count := 0
	for _, b := range books {
		if b.Minute != nil {
			count++
		}
	}
	if count == 0 {
		return nil
	}
	b, err := c.conn.PrepareBatch(ctx, `INSERT INTO `+c.table("derivative_book_minute")+` (`+derivativeMinuteColumns+`)`)
	if err != nil {
		return err
	}
	defer b.Abort()
	for _, book := range books {
		m := book.Minute
		if m == nil {
			continue
		}
		bp, bq := derivativeArrays(m.Bids)
		ap, aq := derivativeArrays(m.Asks)
		if err := b.Append(m.ID, m.InstrumentID, m.MinuteTime.UTC(), id, m.EncodingVersion, m.StoredDepth, m.Signed, m.ValidBitmap, m.DeltaBitmap, bp, bq, ap, aq); err != nil {
			return err
		}
	}
	return b.Send()
}
func (c *Client) insertDerivativeQuality(ctx context.Context, id string, books []model.DerivativeMinuteBatch) error {
	b, err := c.conn.PrepareBatch(ctx, `INSERT INTO `+c.table("derivative_book_quality_minute")+` (`+derivativeQualityColumns+`)`)
	if err != nil {
		return err
	}
	defer b.Abort()
	for _, book := range books {
		if err := b.Append(derivativeQualityValues(book, id)...); err != nil {
			return err
		}
	}
	return b.Send()
}
