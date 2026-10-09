package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"strconv"
	"time"
)

// Minimal durable first-knowledge metadata survives a crash before completion.
// A retry preserves it; only the separate completed publication grants access.
func (c *Client) beginDiscoveryIntent(ctx context.Context, p publicationRecord) (int64, error) {
	if p.SourceMS <= 0 || p.SourceMS > p.ObservedMS || p.ObservedMS > p.KnownMS || p.KnownMS > model.CeilMilliseconds(time.Now().UTC()) {
		return 0, fmt.Errorf("invalid first knowledge clock")
	}
	c.metadataMu.Lock()
	defer c.metadataMu.Unlock()
	known := p.KnownMS
	err := c.retryWrite(ctx, func(ctx context.Context) error {
		var old publicationRecord
		err := c.conn.QueryRow(ctx, "SELECT content_hash,codec,source_ms,observed_ms,known_from_ms,instrument_id,effective_ms,source_count,provenance_hash FROM "+c.table("source_publication_intents")+" WHERE source_kind=? AND source_id=? LIMIT 1", p.Kind, p.ID).Scan(&old.Hash, &old.Codec, &old.SourceMS, &old.ObservedMS, &old.KnownMS, &old.InstrumentID, &old.EffectiveMS, &old.SourceCount, &old.ProvenanceHash)
		if err == nil {
			if old.Hash != p.Hash || old.Codec != p.Codec || old.SourceMS != p.SourceMS || old.ObservedMS != p.ObservedMS || old.InstrumentID != p.InstrumentID || old.EffectiveMS != p.EffectiveMS || old.SourceCount != p.SourceCount || old.ProvenanceHash != p.ProvenanceHash {
				return fmt.Errorf("first knowledge identity changed")
			}
			known = old.KnownMS
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		return c.insertDerivativeRows(ctx, "source_publication_intents", "source_kind,source_id,content_hash,codec,source_ms,observed_ms,known_from_ms,instrument_id,effective_ms,source_count,provenance_hash", [][]any{{p.Kind, p.ID, p.Hash, p.Codec, p.SourceMS, p.ObservedMS, p.KnownMS, p.InstrumentID, p.EffectiveMS, p.SourceCount, p.ProvenanceHash}})
	})
	return known, err
}

func minutePublication(batch model.MinuteBatch, known int64) (publicationRecord, error) {
	m := batch.Minute
	if known < m.MinuteTime.UnixMilli()+60000 {
		return publicationRecord{}, fmt.Errorf("minute not complete at writer observation")
	}
	last := 0
	for second := 0; second < 60; second++ {
		if m.ValidBitmap&(uint64(1)<<second) != 0 {
			last = second
		}
	}
	return publicationRecord{Kind: "cex_book_minute_commit", ID: strconv.FormatUint(m.ID, 10), Hash: model.DiscoveryHash(model.TradingMinuteBody(batch)), Codec: model.DiscoveryMinuteCodec, SourceMS: m.MinuteTime.UnixMilli() + int64(last)*1000, ObservedMS: m.MinuteTime.UnixMilli() + 60000, KnownMS: known, InstrumentID: m.InstrumentID, EffectiveMS: m.MinuteTime.UnixMilli()}, nil
}
