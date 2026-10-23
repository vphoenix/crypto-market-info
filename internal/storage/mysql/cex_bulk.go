package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

func (c *Client) recordCEXMembers(ctx context.Context, tx *sql.Tx, d contracts.Delivery, batches []model.MinuteBatch, hashes map[uint64][]byte) error {
	trace := bulkTrace(ctx, "ingest_member")
	started := time.Now()
	args := []any{c.database}
	byID := map[uint64][]byte{}
	for _, b := range batches {
		if _, ok := byID[b.Minute.ID]; ok {
			return contracts.ErrConflict
		}
		hash, ok := hashes[b.Minute.ID]
		if !ok || len(hash) != 32 {
			return fmt.Errorf("prepared CEX member hash missing")
		}
		byID[b.Minute.ID] = hash
		args = append(args, b.Minute.ID)
	}
	if trace != nil {
		trace.Rows += len(batches)
		trace.PrepareMS += float64(time.Since(started)) / float64(time.Millisecond)
	}
	started = time.Now()
	rows, e := tx.QueryContext(ctx, "SELECT member_id,member_hash FROM cmi_storage.ingest_member WHERE database_name=? AND kind='cex' AND member_id IN ("+placeholders(len(batches))+") ORDER BY member_id FOR UPDATE", args...)
	if e != nil {
		return e
	}
	seen := map[uint64]bool{}
	for rows.Next() {
		var id uint64
		var hash []byte
		if e = rows.Scan(&id, &hash); e != nil {
			rows.Close()
			return e
		}
		want, ok := byID[id]
		if !ok || seen[id] || string(want) != string(hash) {
			rows.Close()
			return contracts.ErrConflict
		}
		seen[id] = true
	}
	e = rows.Err()
	rows.Close()
	if trace != nil {
		trace.ReadMS += float64(time.Since(started)) / float64(time.Millisecond)
	}
	if e != nil {
		return e
	}
	values := []string{}
	parameters := []any{}
	for _, b := range batches {
		if seen[b.Minute.ID] {
			continue
		}
		values = append(values, "(?,'cex',?,?,?)")
		parameters = append(parameters, c.database, b.Minute.ID, d.ID, byID[b.Minute.ID])
	}
	if len(values) > 0 {
		started = time.Now()
		_, e = tx.ExecContext(ctx, "INSERT INTO cmi_storage.ingest_member(database_name,kind,member_id,batch_id,member_hash) VALUES "+strings.Join(values, ","), parameters...)
		if trace != nil {
			trace.InsertMS += float64(time.Since(started)) / float64(time.Millisecond)
		}
	}
	return e
}

func (c *Client) publicationRows(ctx context.Context, tx *sql.Tx, name string, pubs []publicationRecord) (map[string]record, error) {
	t, e := c.mapping(name)
	if e != nil {
		return nil, e
	}
	if !t.HashedKey {
		return nil, fmt.Errorf("publication requires pinned hashed identity")
	}
	if len(pubs) == 0 {
		return map[string]record{}, nil
	}
	// CEX publication IDs are ordered minute/member identities. The existing
	// business_lookup index probes the adjacent new identities without fetching
	// a random primary-index page for each absent SHA-256 key. Full identities
	// and every physical occurrence are still read and validated below.
	args := []any{[]byte(pubs[0].Kind)}
	want := make(map[string]bool, len(pubs))
	sameKind := true
	for _, p := range pubs {
		if want[p.Kind+"\x00"+p.ID] {
			return nil, contracts.ErrConflict
		}
		sameKind = sameKind && p.Kind == pubs[0].Kind
		want[p.Kind+"\x00"+p.ID] = true
		args = append(args, []byte(p.ID))
	}
	where := "WHERE source_kind=? AND source_id IN (" + placeholders(len(pubs)) + ") ORDER BY source_id,`_occurrence` FOR UPDATE"
	if !sameKind {
		predicates := make([]string, 0, len(pubs))
		args = nil
		for _, p := range pubs {
			predicates = append(predicates, "(source_kind=? AND source_id=?)")
			args = append(args, []byte(p.Kind), []byte(p.ID))
		}
		where = "WHERE " + strings.Join(predicates, " OR ") + " ORDER BY source_kind,source_id,`_occurrence` FOR UPDATE"
	}
	started := time.Now()
	rows, e := c.sourceLockedRows(ctx, tx, name, where, args...)
	if trace := bulkTrace(ctx, name); trace != nil {
		trace.ReadMS += float64(time.Since(started)) / float64(time.Millisecond)
	}
	if e != nil {
		return nil, e
	}
	out := map[string]record{}
	for _, r := range rows {
		id := r["source_kind"].(string) + "\x00" + r["source_id"].(string)
		if !want[id] {
			return nil, contracts.ErrConflict
		}
		if _, ok := out[id]; ok {
			return nil, contracts.ErrConflict
		}
		out[id] = r
	}
	return out, nil
}
func scanPublication(r record) (publicationRecord, error) {
	var p publicationRecord
	e := scanRecord(r, intentColumns, &p.Kind, &p.ID, &p.Hash, &p.Codec, &p.SourceMS, &p.ObservedMS, &p.KnownMS, &p.InstrumentID, &p.EffectiveMS, &p.SourceCount, &p.ProvenanceHash)
	return p, e
}

// Preserve first knowledge and every physical occurrence while checking and
// inserting a whole CEX shard. No per-instrument database round trips remain.
func (c *Client) publishCEXMinutes(ctx context.Context, tx *sql.Tx, b model.CompletedMinute, prepared []publicationRecord) error {
	if len(prepared) != len(b.Batches) {
		return fmt.Errorf("prepared CEX publication missing")
	}
	pubs := append([]publicationRecord(nil), prepared...)
	now := model.CeilMilliseconds(time.Now().UTC())
	for _, p := range pubs {
		if p.SourceMS <= 0 || p.SourceMS > p.ObservedMS || p.ObservedMS > p.KnownMS || p.KnownMS > now {
			return fmt.Errorf("invalid first knowledge time")
		}
	}
	intents, e := c.publicationRows(ctx, tx, "source_publication_intents", pubs)
	if e != nil {
		return e
	}
	missing := []record{}
	for n, p := range pubs {
		if old, ok := intents[p.Kind+"\x00"+p.ID]; ok {
			v, e := scanPublication(old)
			if e != nil {
				return e
			}
			known := v.KnownMS
			v.KnownMS = p.KnownMS
			if v != p {
				return contracts.ErrConflict
			}
			pubs[n].KnownMS = known
		} else {
			if b.KnownFromMS == 0 {
				return fmt.Errorf("minute first-knowledge intent missing")
			}
			r, e := namedRecord(intentColumns, []any{p.Kind, p.ID, p.Hash, p.Codec, p.SourceMS, p.ObservedMS, p.KnownMS, p.InstrumentID, p.EffectiveMS, p.SourceCount, p.ProvenanceHash})
			if e != nil {
				return e
			}
			missing = append(missing, r)
		}
	}
	if e = c.insertSourceRows(ctx, tx, "source_publication_intents", missing); e != nil {
		return e
	}
	minuteRows := []record{}
	for n, batch := range b.Batches {
		p := pubs[n]
		seconds := []uint8{}
		for _, d := range batch.Deltas {
			seconds = append(seconds, d.SecondOffset)
		}
		r, e := namedRecord("source_id,instrument_id,minute_ms,stored_depth,content_hash,delta_seconds,codec", []any{p.ID, p.InstrumentID, p.EffectiveMS, batch.Minute.StoredDepth, p.Hash, seconds, p.Codec})
		if e != nil {
			return e
		}
		minuteRows = append(minuteRows, r)
	}
	minuteTable, _ := c.mapping("cex_book_minute_commit")
	if e = putImmutableRows(ctx, tx, c.table(minuteTable.Name), minuteTable, minuteRows); e != nil {
		return e
	}
	status, e := c.publicationRows(ctx, tx, "source_batch_status", pubs)
	if e != nil {
		return e
	}
	statusRows := []record{}
	for _, p := range pubs {
		if old, ok := status[p.Kind+"\x00"+p.ID]; ok {
			v, e := scanPublication(old)
			if e != nil {
				return e
			}
			if v != p || old["complete"] != uint8(1) || old["published_ms"].(int64) < p.KnownMS {
				return contracts.ErrConflict
			}
			continue
		}
		published := model.CeilMilliseconds(time.Now().UTC())
		if published < p.KnownMS {
			return fmt.Errorf("publication clock precedes knowledge")
		}
		r, e := namedRecord(statusColumns, []any{p.Kind, p.ID, p.Hash, p.SourceMS, p.ObservedMS, p.KnownMS, published, uint8(1), p.Codec, p.InstrumentID, p.EffectiveMS, p.SourceCount, p.ProvenanceHash})
		if e != nil {
			return e
		}
		statusRows = append(statusRows, r)
	}
	return c.insertSourceRows(ctx, tx, "source_batch_status", statusRows)
}
