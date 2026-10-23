package mysql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	driver "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/options"
	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

func profileDigest(t Table, r record) ([32]byte, error) {
	h := sha256.New()
	writeFramed(h, []byte("options-quality-profile-v1"))
	for _, col := range t.Columns {
		if col.Name == "profile_hash" {
			continue
		}
		v, ok := r[col.Name]
		if !ok {
			return [32]byte{}, fmt.Errorf("missing profile field %s", col.Name)
		}
		raw, err := codec.Encode(col.Type, v)
		if err != nil {
			return [32]byte{}, err
		}
		writeFramed(h, raw)
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out, nil
}

func splitQualityEvidence(q options.QualityEvidence) (record, record, error) {
	r, err := modelRecord(q)
	if err != nil {
		return nil, nil, err
	}
	if err = compactRecord("options_catalog_quality_evidence_minute", r); err != nil {
		return nil, nil, err
	}
	p, m := record{}, record{}
	for k, v := range r {
		if k == "minute_time" {
			p[k] = v
		}
		if strings.HasSuffix(k, "_offsets") || strings.HasSuffix(k, "_values") {
			p[k] = v
		} else {
			m[k] = v
		}
	}
	t, err := Mapping("crypto_market_info", qualityProfile)
	if err != nil {
		return nil, nil, err
	}
	hash, err := profileDigest(t, p)
	if err != nil {
		return nil, nil, err
	}
	p["profile_hash"], m["profile_hash"] = string(hash[:]), string(hash[:])
	return p, m, nil
}

// On retries or after disabling new writes, keep a batch in its existing format.
// The alternate lookup is bounded by the existing minute/batch index.
func (c *Client) qualityWriteTable(ctx context.Context, tx *sql.Tx, legacy, compact string, minute time.Time, id string) (string, error) {
	if !c.qualityStorageAvailable {
		return legacy, nil
	}
	target, alternate := legacy, compact
	if c.qualityStorageV2 {
		target, alternate = compact, legacy
	}
	var found int
	err := tx.QueryRowContext(ctx, "SELECT 1 FROM "+c.table(alternate)+" WHERE minute_time=? AND compact_batch_id=? LIMIT 1", minute.UTC(), digestBytes(id)).Scan(&found)
	if err == nil {
		return alternate, nil
	}
	if err != sql.ErrNoRows {
		return "", err
	}
	return target, nil
}

func (c *Client) putQualityEvidence(ctx context.Context, tx *sql.Tx, rows []options.QualityEvidence) error {
	if len(rows) == 0 {
		return nil
	}
	target, err := c.qualityWriteTable(ctx, tx, "options_catalog_quality_evidence_minute", qualityMember, rows[0].MinuteTime, rows[0].BatchID)
	if err != nil {
		return err
	}
	if target != qualityMember {
		return putModelsBulk(ctx, c, tx, target, rows)
	}
	profiles := make([]record, 0, len(rows))
	members := make([]record, 0, len(rows))
	for _, q := range rows {
		p, m, err := splitQualityEvidence(q)
		if err != nil {
			return err
		}
		profiles = append(profiles, p)
		members = append(members, m)
	}
	// Consistent lock order across batches sharing a profile.
	slices.SortFunc(profiles, func(a, b record) int { return strings.Compare(a["profile_hash"].(string), b["profile_hash"].(string)) })
	if err := c.putQualityProfiles(ctx, tx, profiles); err != nil {
		return err
	}
	t, err := c.mapping(qualityMember)
	if err != nil {
		return err
	}
	return putImmutableRows(ctx, tx, c.table(qualityMember), t, members)
}

// Existing profiles are immutable and may be read concurrently. Do not hold
// exclusive locks on a shared profile throughout every member transaction.
// A competing first INSERT requires rollback and complete content revalidation.
func (c *Client) putQualityProfiles(ctx context.Context, tx *sql.Tx, input []record) error {
	t, err := c.mapping(qualityProfile)
	if err != nil {
		return err
	}
	unique := map[string]record{}
	keys := []string{}
	for _, r := range input {
		key := r["profile_hash"].(string)
		if old := unique[key]; old != nil {
			a, err := rowHash(t, old)
			if err != nil {
				return err
			}
			b, err := rowHash(t, r)
			if err != nil {
				return err
			}
			if a != b {
				return contracts.ErrConflict
			}
		} else {
			unique[key] = r
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		return nil
	}
	slices.Sort(keys)
	args := []any{input[0]["minute_time"]}
	for _, key := range keys {
		args = append(args, []byte(key))
	}
	stored, err := readRecords(ctx, tx, c.table(qualityProfile), t, "WHERE minute_time=? AND profile_hash IN ("+placeholders(len(keys))+")", args...)
	if err != nil {
		return err
	}
	for _, r := range stored {
		key := r["profile_hash"].(string)
		want := unique[key]
		if want == nil {
			return contracts.ErrConflict
		}
		a, err := rowHash(t, want)
		if err != nil {
			return err
		}
		b, err := rowHash(t, r)
		if err != nil {
			return err
		}
		if a != b {
			return contracts.ErrConflict
		}
		delete(unique, key)
	}
	missing := []record{}
	for _, key := range keys {
		if r := unique[key]; r != nil {
			missing = append(missing, r)
		}
	}
	if len(missing) > 0 {
		if err := minuteFault(ctx, "before_quality_profile_insert", tx); err != nil {
			return err
		}
	}
	if err = insertAbsentImmutableRows(ctx, tx, c.table(qualityProfile), t, missing); err != nil {
		var duplicate *driver.MySQLError
		if errors.As(err, &duplicate) && duplicate.Number == 1062 {
			return temporaryStorageError{fmt.Errorf("%w: shared profile insert: %w", errMinuteInsertCollision, err)}
		}
		return err
	}
	return nil
}

func evidenceFromRecord(r record) (options.QualityEvidence, error) {
	q, err := assignEvidenceRecord(r)
	if err != nil {
		return q, err
	}
	if err = q.Validate(); err != nil {
		return q, err
	}
	if q.RowHash != q.Hash() {
		return q, fmt.Errorf("quality evidence digest mismatch")
	}
	return q, nil
}

func assignEvidenceRecord(r record) (options.QualityEvidence, error) {
	var q options.QualityEvidence
	v := reflect.ValueOf(&q).Elem()
	for n := 0; n < v.NumField(); n++ {
		name := v.Type().Field(n).Tag.Get("ch")
		x, ok := r[name]
		if !ok {
			return q, fmt.Errorf("missing quality field %s", name)
		}
		if err := codec.Assign(v.Field(n), x); err != nil {
			return q, err
		}
	}
	return q, nil
}

func (c *Client) loadQualityEvidence(ctx context.Context, run uuid.UUID, minute time.Time, id string) ([]options.QualityEvidence, error) {
	if !c.qualityStorageAvailable {
		return readModels[options.QualityEvidence](ctx, c, c.db, "options_catalog_quality_evidence_minute", "WHERE run_id=? AND minute_time=? AND compact_batch_id=? ORDER BY instrument_id", run[:], minute, digestBytes(id))
	}
	old, err := c.mapping("options_catalog_quality_evidence_minute")
	if err != nil {
		return nil, err
	}
	profile, err := c.mapping(qualityProfile)
	if err != nil {
		return nil, err
	}
	// One statement gives both formats a consistent snapshot and avoids a SQL
	// round trip per storage layer. LEFT JOIN makes dangling references fail the
	// non-null typed decoding instead of disappearing from the result.
	joined := []string{}
	for _, col := range old.Columns {
		prefix := "m"
		if strings.HasSuffix(col.Name, "_offsets") || strings.HasSuffix(col.Name, "_values") {
			prefix = "p"
		}
		joined = append(joined, prefix+".`"+col.Name+"`")
	}
	marker := Column{Name: "runtime_profile_hash", SourceType: "Nullable(FixedString(32))", MySQLType: "BINARY(32)", Nullable: true}
	marker.Type, err = codec.Parse(marker.SourceType)
	if err != nil {
		return nil, err
	}
	readTable := old
	readTable.Columns = append(slices.Clone(old.Columns), marker)
	qualified := "(SELECT " + columns(old) + ",CAST(NULL AS BINARY(32)) AS runtime_profile_hash FROM " + c.table(old.Name) + " WHERE run_id=? AND minute_time=? AND compact_batch_id=? UNION ALL SELECT " + strings.Join(joined, ",") + ",m.profile_hash AS runtime_profile_hash FROM " + c.table(qualityMember) + " m LEFT JOIN " + c.table(qualityProfile) + " p ON p.minute_time=m.minute_time AND p.profile_hash=m.profile_hash WHERE m.minute_time=? AND m.run_id=? AND m.compact_batch_id=?) AS combined_evidence"
	rows, err := readRecords(ctx, c.db, qualified, readTable, "ORDER BY instrument_id", run[:], minute, digestBytes(id), minute, run[:], digestBytes(id))
	if err != nil {
		return nil, err
	}
	if len(rows) > 448 {
		return nil, fmt.Errorf("quality member quota exceeded")
	}
	legacy := make([]options.QualityEvidence, 0, len(rows))
	checkedProfiles := map[string]bool{}
	for _, r := range rows {
		if key := r["runtime_profile_hash"]; key != nil {
			profileKey := key.(string)
			// Every occurrence is joined to the same unique immutable profile in
			// this statement snapshot. Hash its shared fields once per key.
			if !checkedProfiles[profileKey] {
				hash, err := profileDigest(profile, r)
				if err != nil {
					return nil, err
				}
				if profileKey != string(hash[:]) {
					return nil, contracts.ErrConflict
				}
				checkedProfiles[profileKey] = true
			}
		}
		// The public catalog reader validates the complete original member hash
		// against its committed manifest, exactly as for legacy physical rows.
		q, err := assignEvidenceRecord(r)
		if err != nil {
			return nil, err
		}
		legacy = append(legacy, q)
	}
	for n := 1; n < len(legacy); n++ {
		if legacy[n-1].InstrumentID == legacy[n].InstrumentID {
			return nil, contracts.ErrConflict
		}
	}
	return legacy, nil
}

func (c *Client) loadDerivativeQuality(ctx context.Context, minute time.Time, id string) ([]record, error) {
	where := "WHERE minute_time=? AND compact_batch_id=?"
	if !c.qualityStorageAvailable {
		return c.records(ctx, c.db, "derivative_book_quality_minute", where+" ORDER BY instrument_id", minute, digestBytes(id))
	}
	t, err := c.mapping("derivative_book_quality_minute")
	if err != nil {
		return nil, err
	}
	qualified := "(SELECT " + columns(t) + " FROM " + c.table(t.Name) + " " + where + " UNION ALL SELECT " + columns(t) + " FROM " + c.table(qualityMinuteV2) + " " + where + ") AS combined_quality"
	rows, err := readRecords(ctx, c.db, qualified, t, "ORDER BY instrument_id", minute, digestBytes(id), minute, digestBytes(id))
	if err != nil {
		return nil, err
	}
	for n := 1; n < len(rows); n++ {
		if rows[n-1]["instrument_id"] == rows[n]["instrument_id"] {
			return nil, contracts.ErrConflict
		}
	}
	return rows, nil
}
