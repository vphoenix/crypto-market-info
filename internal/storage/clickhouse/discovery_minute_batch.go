package clickhouse

import (
	"context"
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"slices"
	"strings"
	"time"
)

type minutePublicationState struct {
	intent     *publicationRecord
	commitHash string
	status     *DiscoveryProof
}

func minuteIdentityEqual(a, b publicationRecord) bool {
	return a.Kind == b.Kind && a.ID == b.ID && a.Hash == b.Hash && a.Codec == b.Codec && a.SourceMS == b.SourceMS && a.ObservedMS == b.ObservedMS && a.InstrumentID == b.InstrumentID && a.EffectiveMS == b.EffectiveMS && a.SourceCount == b.SourceCount && a.ProvenanceHash == b.ProvenanceHash
}

// A bounded SELECT per table, never a query per instrument. Duplicate ordinary
// MergeTree records must agree; do not hide conflicting records with LIMIT 1.
func (c *Client) minutePublicationStates(ctx context.Context, pubs []publicationRecord, batches []model.MinuteBatch) (map[string]minutePublicationState, error) {
	if len(pubs) == 0 || len(pubs) > MinuteWriteBatchInstruments || len(pubs) != len(batches) {
		return nil, fmt.Errorf("invalid minute publication chunk")
	}
	states := make(map[string]minutePublicationState, len(pubs))
	args := make([]any, len(pubs))
	expected := make(map[string]publicationRecord, len(pubs))
	members := make(map[string]model.MinuteBatch, len(pubs))
	for n, p := range pubs {
		args[n] = p.ID
		expected[p.ID] = p
		members[p.ID] = batches[n]
		states[p.ID] = minutePublicationState{}
	}
	where := " WHERE source_kind='cex_book_minute_commit' AND source_id IN (" + strings.TrimSuffix(strings.Repeat("?,", len(pubs)), ",") + ")"
	rows, err := c.conn.Query(ctx, "SELECT source_id,content_hash,codec,source_ms,observed_ms,known_from_ms,instrument_id,effective_ms,source_count,provenance_hash FROM "+c.table("source_publication_intents")+where, args...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		p := publicationRecord{Kind: "cex_book_minute_commit"}
		if err = rows.Scan(&p.ID, &p.Hash, &p.Codec, &p.SourceMS, &p.ObservedMS, &p.KnownMS, &p.InstrumentID, &p.EffectiveMS, &p.SourceCount, &p.ProvenanceHash); err != nil {
			rows.Close()
			return nil, err
		}
		want, ok := expected[p.ID]
		s := states[p.ID]
		if !ok || !minuteIdentityEqual(p, want) || p.KnownMS < p.ObservedMS || p.KnownMS > model.CeilMilliseconds(time.Now().UTC()) || s.intent != nil {
			rows.Close()
			return nil, fmt.Errorf("minute first knowledge identity changed: %s", p.ID)
		}
		s.intent = &p
		states[p.ID] = s
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	commitWhere := strings.Replace(where, "source_kind='cex_book_minute_commit' AND ", "", 1)
	rows, err = c.conn.Query(ctx, "SELECT source_id,instrument_id,minute_ms,stored_depth,content_hash,delta_seconds,codec FROM "+c.table("cex_book_minute_commit")+commitWhere, args...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id, hash, codec string
		var instrument uint32
		var minute int64
		var depth uint8
		var seconds []uint8
		if err = rows.Scan(&id, &instrument, &minute, &depth, &hash, &seconds, &codec); err != nil {
			rows.Close()
			return nil, err
		}
		want, ok := expected[id]
		s := states[id]
		member := members[id]
		wantSeconds := make([]uint8, len(member.Deltas))
		for n, d := range member.Deltas {
			wantSeconds[n] = d.SecondOffset
		}
		if !ok || hash != want.Hash || codec != want.Codec || instrument != want.InstrumentID || minute != want.EffectiveMS || (s.commitHash != "") || depth != member.Minute.StoredDepth || !slices.Equal(seconds, wantSeconds) {
			rows.Close()
			return nil, fmt.Errorf("minute commit identity changed: %s", id)
		}
		if s.intent == nil {
			rows.Close()
			return nil, fmt.Errorf("minute commit missing original first knowledge: %s", id)
		}
		s.commitHash = hash
		states[id] = s
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	rows, err = c.conn.Query(ctx, "SELECT source_id,content_hash,codec,source_ms,observed_ms,known_from_ms,published_ms,complete,instrument_id,effective_ms,source_count,provenance_hash FROM "+c.table("source_batch_status")+where, args...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		p := DiscoveryProof{Kind: "cex_book_minute_commit"}
		var complete uint8
		if err = rows.Scan(&p.ID, &p.Hash, &p.Codec, &p.SourceMS, &p.ObservedMS, &p.KnownMS, &p.PublishedMS, &complete, &p.InstrumentID, &p.EffectiveMS, &p.SourceCount, &p.ProvenanceHash); err != nil {
			rows.Close()
			return nil, err
		}
		want, ok := expected[p.ID]
		s := states[p.ID]
		actual := publicationRecord{Kind: p.Kind, ID: p.ID, Hash: p.Hash, Codec: p.Codec, SourceMS: p.SourceMS, ObservedMS: p.ObservedMS, KnownMS: p.KnownMS, InstrumentID: p.InstrumentID, EffectiveMS: p.EffectiveMS, SourceCount: p.SourceCount, ProvenanceHash: p.ProvenanceHash}
		if !ok || !minuteIdentityEqual(actual, want) || complete != 1 || s.intent == nil || s.commitHash == "" || p.KnownMS != s.intent.KnownMS || p.PublishedMS < p.KnownMS || p.PublishedMS > model.CeilMilliseconds(time.Now().UTC()) || s.status != nil {
			rows.Close()
			return nil, fmt.Errorf("minute publication identity changed: %s", p.ID)
		}
		s.status = &p
		states[p.ID] = s
	}
	err = rows.Err()
	rows.Close()
	return states, err
}

// All first-knowledge intents are durable before any minute/delta facts. An
// ambiguous insert re-reads every key and retains each original KnownMS.
func (c *Client) beginMinutePublications(ctx context.Context, pubs []publicationRecord, batches []model.MinuteBatch) error {
	c.metadataMu.Lock()
	defer c.metadataMu.Unlock()
	return c.retryWrite(ctx, func(ctx context.Context) error {
		states, err := c.minutePublicationStates(ctx, pubs, batches)
		if err != nil {
			return err
		}
		missing := make([][]any, 0, len(pubs))
		for n, p := range pubs {
			if s := states[p.ID]; s.intent != nil {
				pubs[n].KnownMS = s.intent.KnownMS
				continue
			}
			missing = append(missing, []any{p.Kind, p.ID, p.Hash, p.Codec, p.SourceMS, p.ObservedMS, p.KnownMS, p.InstrumentID, p.EffectiveMS, p.SourceCount, p.ProvenanceHash})
		}
		if len(missing) == 0 {
			return nil
		}
		if err = c.insertDerivativeRows(ctx, "source_publication_intents", "source_kind,source_id,content_hash,codec,source_ms,observed_ms,known_from_ms,instrument_id,effective_ms,source_count,provenance_hash", missing); err != nil {
			return err
		}
		return c.derivativeInserted("source_publication_intents")
	})
}

// Called only after the full chunk's delta and anchor facts are durable.
// Commits precede complete markers; retries preserve original PublishedMS.
func (c *Client) publishMinuteChunk(ctx context.Context, batches []model.MinuteBatch, pubs []publicationRecord) error {
	c.metadataMu.Lock()
	defer c.metadataMu.Unlock()
	return c.retryWrite(ctx, func(ctx context.Context) error {
		states, err := c.minutePublicationStates(ctx, pubs, batches)
		if err != nil {
			return err
		}
		commits := make([][]any, 0, len(pubs))
		statuses := make([][]any, 0, len(pubs))

		for n, p := range pubs {
			s := states[p.ID]
			if s.intent == nil || p.KnownMS != s.intent.KnownMS {
				return fmt.Errorf("minute first knowledge missing/changed")
			}
			if s.status != nil {
				continue
			}
			if s.commitHash == "" {
				b := batches[n]
				seconds := make([]uint8, 0, len(b.Deltas))
				for _, d := range b.Deltas {
					seconds = append(seconds, d.SecondOffset)
				}
				commits = append(commits, []any{p.ID, p.InstrumentID, p.EffectiveMS, b.Minute.StoredDepth, p.Hash, seconds, p.Codec})
			}
		}
		if len(commits) > 0 {
			if err = c.insertDerivativeRows(ctx, "cex_book_minute_commit", "source_id,instrument_id,minute_ms,stored_depth,content_hash,delta_seconds,codec", commits); err != nil {
				return err
			}
			if err = c.derivativeInserted("cex_book_minute_commit"); err != nil {
				return err
			}
		}
		// Timestamp publication only after EVERY dependency commit is durable.
		published := model.CeilMilliseconds(time.Now().UTC())
		for _, p := range pubs {
			if states[p.ID].status != nil {
				continue
			}
			if published < p.KnownMS {
				return fmt.Errorf("publication clock precedes first knowledge")
			}
			statuses = append(statuses, []any{p.Kind, p.ID, p.Hash, p.SourceMS, p.ObservedMS, p.KnownMS, published, uint8(1), p.Codec, p.InstrumentID, p.EffectiveMS, p.SourceCount, p.ProvenanceHash})
		}
		if len(statuses) > 0 {
			if err = c.insertDerivativeRows(ctx, "source_batch_status", "source_kind,source_id,content_hash,source_ms,observed_ms,known_from_ms,published_ms,complete,codec,instrument_id,effective_ms,source_count,provenance_hash", statuses); err != nil {
				return err
			}
			return c.derivativeInserted("source_batch_status")
		}
		return nil
	})
}
