package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/replay"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

const MinuteWriteBatchInstruments = 100

func minuteRecord(m model.MinuteBook) record {
	r := record{"id": m.ID, "instrument_id": m.InstrumentID, "minute_time": m.MinuteTime, "valid_bitmap": m.ValidBitmap, "stored_depth": m.StoredDepth}
	for _, side := range []struct {
		name   string
		levels [50]model.Level
	}{{"bid", m.Bids}, {"ask", m.Asks}} {
		for i, l := range side.levels {
			r[fmt.Sprintf("%s_price_%02d", side.name, i+1)] = l.PriceTick
			r[fmt.Sprintf("%s_qty_%02d", side.name, i+1)] = l.QtyLot
		}
	}
	return r
}
func deltaRecord(d model.BookDelta) record {
	return record{"minute_id": d.MinuteID, "second_offset": d.SecondOffset, "bid_change_prices": d.BidChangePrice, "bid_change_qtys": d.BidChangeQty, "ask_change_prices": d.AskChangePrice, "ask_change_qtys": d.AskChangeQty}
}
func minuteModel(r record) (model.MinuteBook, error) {
	m := model.MinuteBook{ID: r["id"].(uint64), InstrumentID: r["instrument_id"].(uint32), MinuteTime: r["minute_time"].(time.Time), ValidBitmap: r["valid_bitmap"].(uint64), StoredDepth: r["stored_depth"].(uint8)}
	for n, side := range []string{"bid", "ask"} {
		for i := range 50 {
			l := model.Level{PriceTick: r[fmt.Sprintf("%s_price_%02d", side, i+1)].(int64), QtyLot: r[fmt.Sprintf("%s_qty_%02d", side, i+1)].(uint64)}
			if n == 0 {
				m.Bids[i] = l
			} else {
				m.Asks[i] = l
			}
		}
	}
	return m, m.ValidateDepth()
}
func (c *Client) loadMinute(ctx context.Context, id uint32, at time.Time) (model.MinuteBook, error) {
	t, e := c.mapping("order_book_minute")
	if e != nil {
		return model.MinuteBook{}, e
	}
	rs, e := readRecords(ctx, c.db, c.table(t.Name), t, "WHERE instrument_id=? AND minute_time=?", id, at.UTC().Truncate(time.Minute))
	if e != nil {
		return model.MinuteBook{}, e
	}
	if len(rs) == 0 {
		return model.MinuteBook{}, contracts.ErrNotFound
	}
	if len(rs) != 1 {
		return model.MinuteBook{}, contracts.ErrConflict
	}
	return minuteModel(rs[0])
}
func (c *Client) loadDeltas(ctx context.Context, id uint64, through uint8) ([]model.BookDelta, error) {
	if through > 59 {
		return nil, fmt.Errorf("delta offset exceeds minute")
	}
	t, e := c.mapping("order_book_second_delta")
	if e != nil {
		return nil, e
	}
	rs, e := readRecords(ctx, c.db, c.table(t.Name), t, "WHERE minute_id=? AND second_offset<=? ORDER BY second_offset", id, through)
	if e != nil {
		return nil, e
	}
	out := make([]model.BookDelta, len(rs))
	for i, r := range rs {
		d := &out[i]
		d.MinuteID = r["minute_id"].(uint64)
		d.SecondOffset = r["second_offset"].(uint8)
		for _, x := range r["bid_change_prices"].([]any) {
			d.BidChangePrice = append(d.BidChangePrice, x.(int64))
		}
		for _, x := range r["bid_change_qtys"].([]any) {
			d.BidChangeQty = append(d.BidChangeQty, x.(uint64))
		}
		for _, x := range r["ask_change_prices"].([]any) {
			d.AskChangePrice = append(d.AskChangePrice, x.(int64))
		}
		for _, x := range r["ask_change_qtys"].([]any) {
			d.AskChangeQty = append(d.AskChangeQty, x.(uint64))
		}
	}
	return out, nil
}
func (c *Client) loadCommittedBook(ctx context.Context, id uint32, at time.Time) (model.MinuteBatch, error) {
	m, e := c.loadMinute(ctx, id, at)
	if e != nil {
		return model.MinuteBatch{}, e
	}
	ds, e := c.loadDeltas(ctx, m.ID, 59)
	if e != nil {
		return model.MinuteBatch{}, e
	}
	batch := model.MinuteBatch{Minute: m, Deltas: ds}
	if e = contracts.ValidateMinuteBatch(batch); e != nil {
		return batch, e
	}
	var digest []byte
	var count int
	e = c.db.QueryRowContext(ctx, "SELECT m.member_hash,(SELECT COUNT(*) FROM cmi_storage.ingest_batch b WHERE b.database_name=m.database_name AND b.kind=m.kind AND b.batch_id=m.batch_id) FROM cmi_storage.ingest_member m WHERE m.database_name=? AND m.kind='cex' AND m.member_id=?", c.database, m.ID).Scan(&digest, &count)
	if e == sql.ErrNoRows {
		return batch, contracts.ErrNotFound
	}
	if e != nil {
		return batch, e
	}
	want := digestBytes(model.DiscoveryHash(model.TradingMinuteBody(batch)))
	if count != 1 || string(want) != string(digest) {
		return batch, fmt.Errorf("CEX committed membership missing or changed")
	}
	return batch, nil
}
func (c *Client) LoadMinute(ctx context.Context, id uint32, at time.Time) (model.MinuteBook, error) {
	b, e := c.loadCommittedBook(ctx, id, at)
	return b.Minute, e
}
func (c *Client) LoadDeltas(ctx context.Context, id uint64, through uint8) ([]model.BookDelta, error) {
	if through > 59 {
		return nil, fmt.Errorf("invalid delta offset")
	}
	at := time.Unix(int64(id>>32)*60, 0).UTC()
	b, e := c.loadCommittedBook(ctx, uint32(id), at)
	if e != nil {
		return nil, e
	}
	out := []model.BookDelta{}
	for _, d := range b.Deltas {
		if d.SecondOffset <= through {
			out = append(out, d)
		}
	}
	return out, nil
}
func (c *Client) ReplayBook(ctx context.Context, id uint32, at time.Time) (model.BookSnapshot, bool, error) {
	b, e := c.loadCommittedBook(ctx, id, at)
	if e != nil {
		return model.BookSnapshot{}, false, e
	}
	s, valid, e := replay.AtSecond(b.Minute, b.Deltas, uint8(at.Second()))
	if valid {
		s.SourceTime = at.UTC().Truncate(time.Second)
	}
	return s, valid, e
}

func (c *Client) WriteMinute(ctx context.Context, b model.MinuteBatch) error {
	return c.WriteCompletedMinute(ctx, model.CompletedMinute{MinuteTime: b.Minute.MinuteTime, Batches: []model.MinuteBatch{b}})
}
func (c *Client) WriteCompletedMinute(ctx context.Context, all model.CompletedMinute) error {
	deliveries, e := c.PrepareBookDeliveries(ctx, all)
	if e != nil {
		return e
	}
	for _, d := range deliveries {
		if c.queue != nil {
			e = c.queue.Accept(ctx, d)
		} else {
			e = c.Commit(ctx, d)
		}
		if e != nil {
			return e
		}
	}
	return nil
}

func (c *Client) Committed(ctx context.Context, d contracts.Delivery) (bool, error) {
	return c.committed(ctx, c.db, d)
}
func (c *Client) committed(ctx context.Context, q executor, d contracts.Delivery) (bool, error) {
	var digest []byte
	var members uint32
	var version uint16
	var service []byte
	var gen uint64
	var minute time.Time
	e := q.QueryRowContext(ctx, "SELECT content_hash,member_count,codec,service,generation,logical_time FROM cmi_storage.ingest_batch WHERE database_name=? AND kind=? AND batch_id=?", d.Database, d.Kind, d.ID).Scan(&digest, &members, &version, &service, &gen, &minute)
	if e == sql.ErrNoRows {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	if string(digest) != string(d.Digest[:]) || members != d.Members || version != d.Codec || string(service) != d.Service || gen != d.Generation || !minute.Equal(d.Minute) {
		return false, contracts.ErrConflict
	}
	return true, nil
}
func (c *Client) Commit(ctx context.Context, d contracts.Delivery) error {
	prepared, e := c.PrepareDelivery(ctx, d)
	if e != nil {
		return e
	}
	return c.CommitPrepared(ctx, prepared)
}
func (c *Client) commitCEXRows(ctx context.Context, tx *sql.Tx, d contracts.Delivery, b model.CompletedMinute, hashes map[uint64][]byte, pubs []publicationRecord) error {
	mt, e := c.mapping("order_book_minute")
	if e != nil {
		return e
	}
	dt, e := c.mapping("order_book_second_delta")
	if e != nil {
		return e
	}
	minutes, deltas := []record{}, []record{}
	for _, batch := range b.Batches {
		minutes = append(minutes, minuteRecord(batch.Minute))
		for _, d := range batch.Deltas {
			deltas = append(deltas, deltaRecord(d))
		}
	}
	if e = putImmutableRows(ctx, tx, c.table(dt.Name), dt, deltas); e != nil {
		return e
	}
	if e = minuteFault(ctx, "partial_facts", tx); e != nil {
		return e
	}
	if e = putImmutableRows(ctx, tx, c.table(mt.Name), mt, minutes); e != nil {
		return e
	}

	if e = c.recordCEXMembers(ctx, tx, d, b.Batches, hashes); e != nil {
		return e
	}
	if c.discoveryEnabled.Load() || b.KnownFromMS != 0 {
		return c.publishCEXMinutes(ctx, tx, b, pubs)
	}

	return nil
}

var _ contracts.BookReader = (*Client)(nil)
var _ contracts.BookWriter = (*Client)(nil)
var _ contracts.DeliveryStore = (*Client)(nil)
