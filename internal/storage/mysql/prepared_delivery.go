package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"reflect"
	"time"

	driver "github.com/go-sql-driver/mysql"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/options"
	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
	"github.com/vphoenix/crypto-market-info/internal/storage/spool"
)

type PreparedDelivery struct {
	d               contracts.Delivery
	cex             *model.CompletedMinute
	live            *options.LiveEnvelope
	catalog         *options.CatalogEnvelope
	run             options.LiveRun
	plan            options.CollectionPlan
	cexHashes       map[uint64][]byte
	cexPublications []publicationRecord
	cexSpill        *cexPreparation
}

func (p *PreparedDelivery) Streams() []string {
	out := []string{}
	if p.cexSpill != nil {
		for _, id := range p.cexSpill.streams {
			out = append(out, fmt.Sprint(p.d.Service, "/", id))
		}
	}
	if p.cex != nil {
		for _, b := range p.cex.Batches {
			out = append(out, fmt.Sprint(p.d.Service, "/", b.Minute.InstrumentID))
		}
	}
	if p.live != nil {
		for _, b := range p.live.Books {
			out = append(out, fmt.Sprint(p.d.Service, "/", b.InstrumentID))
		}
	}
	if p.catalog != nil {
		for _, b := range p.catalog.Live.Books {
			out = append(out, fmt.Sprint(p.d.Service, "/", b.InstrumentID))
		}
	}
	return out
}

// MemoryBytes includes decoded slices, strings, maps and pointer targets. It
// deliberately overcounts aliases, so admission never relies on compressed
// payload size alone.
func (p *PreparedDelivery) MemoryBytes() int64 {
	var heap func(reflect.Value) int64
	heap = func(v reflect.Value) int64 {
		if !v.IsValid() {
			return 0
		}
		if v.Type() == reflect.TypeOf((*os.File)(nil)) {
			return 0
		}
		var n int64
		switch v.Kind() {
		case reflect.String:
			return int64(v.Len())
		case reflect.Pointer, reflect.Interface:
			if !v.IsNil() {
				return int64(v.Elem().Type().Size()) + heap(v.Elem())
			}
		case reflect.Slice:
			n = int64(v.Cap()) * int64(v.Type().Elem().Size())
			for j := 0; j < v.Len(); j++ {
				n += heap(v.Index(j))
			}
		case reflect.Array:
			for j := 0; j < v.Len(); j++ {
				n += heap(v.Index(j))
			}
		case reflect.Struct:
			for j := 0; j < v.NumField(); j++ {
				n += heap(v.Field(j))
			}
		case reflect.Map:
			if !v.IsNil() {
				n = int64(v.Len()) * int64(v.Type().Key().Size()+v.Type().Elem().Size()+32)
				it := v.MapRange()
				for it.Next() {
					n += heap(it.Key()) + heap(it.Value())
				}
			}
		}
		return n
	}
	n := int64(reflect.TypeOf(*p).Size()) + heap(reflect.ValueOf(p).Elem())
	if p.cexSpill != nil {
		n += p.cexSpill.workingBytes
	}
	return n
}

func (c *Client) PrepareDelivery(ctx context.Context, d contracts.Delivery) (*PreparedDelivery, error) {
	if e := spool.Validate(d); e != nil {
		return nil, e
	}
	if d.Database != c.database || d.Service != c.producer.Service || d.Generation != c.producer.Generation {
		return nil, fmt.Errorf("delivery producer mismatch")
	}
	p := &PreparedDelivery{d: d}
	p.d.Payload = nil
	switch d.Kind {
	case "cex":
		if d.Codec != 1 && d.Codec != 2 {
			return nil, fmt.Errorf("unsupported CEX codec")
		}
		v, e := codec.DecodeBookDelivery(d.Minute, d.Payload, d.Codec)
		if e != nil {
			return nil, e
		}
		if len(v.Batches) == 0 || len(v.Batches) > 100 || uint32(len(v.Batches)) != d.Members {
			return nil, fmt.Errorf("delivery member count mismatch")
		}
		p.cex = &v
		p.cexHashes = map[uint64][]byte{}
		known := v.KnownFromMS
		if known == 0 {
			known = model.CeilMilliseconds(time.Now().UTC())
		}
		for _, batch := range v.Batches {
			hash := model.DiscoveryHash(model.TradingMinuteBody(batch))
			p.cexHashes[batch.Minute.ID] = digestBytes(hash)
			if c.discoveryEnabled.Load() || v.KnownFromMS != 0 {
				publication, e := minutePublicationHash(batch, known, hash)
				if e != nil {
					return nil, e
				}
				p.cexPublications = append(p.cexPublications, publication)
			}
		}
		if e := c.prefetchCEXPublications(ctx, p.cexPublications); e != nil {
			return nil, e
		}
	case "options_live", "options_catalog":
		if d.Codec != 1 {
			return nil, fmt.Errorf("unsupported options codec")
		}
		var live options.LiveEnvelope
		if d.Kind == "options_catalog" {
			v, e := codec.DecodeOptionsCatalog(d.Payload)
			if e != nil {
				return nil, e
			}
			p.catalog = &v
			live = v.Live
		} else {
			v, e := codec.DecodeOptionsLive(d.Payload)
			if e != nil {
				return nil, e
			}
			p.live = &v
			live = v
		}
		if !live.MinuteTime.Equal(d.Minute) || uint32(len(live.Books)) != d.Members {
			return nil, fmt.Errorf("options delivery membership mismatch")
		}
		if e := live.Validate(); e != nil {
			return nil, e
		}
		r, e := c.LoadOptionsRun(ctx, live.RunID)
		if e != nil {
			return nil, e
		}
		p.run = r
		if e = live.ValidateRun(r); e != nil {
			return nil, e
		}
		// Specs, rules, and evidence are immutable and cached by identity. The
		// first load verifies stored hashes; envelope validation checks expected
		// hashes and first-knowledge times on every batch, including cache hits.
		if e = c.validateDerivativeReferences(ctx, live.Books); e != nil {
			return nil, e
		}
		if p.catalog != nil {
			v := *p.catalog
			if e = v.Validate(); e != nil {
				return nil, e
			}
			plan, e := c.OptionsPlanAt(ctx, d.Minute)
			if e != nil {
				return nil, e
			}
			p.plan = plan
			if plan.ID != v.PlanID || plan.Hash() != v.PlanHash {
				return nil, fmt.Errorf("minute plan mismatch")
			}
			if e = plan.ValidateRun(r, d.Minute); e != nil {
				return nil, e
			}
			if e = c.validateCatalogEvidence(ctx, r, v); e != nil {
				return nil, e
			}
		} else {
			if r.Selection == options.CatalogSelection {
				return nil, fmt.Errorf("catalog requires plan commit")
			}
			if e = c.validateLiveMetadata(ctx, r, live); e != nil {
				return nil, e
			}
		}
		if e := c.prefetchOptionsFacts(ctx, live, p.catalog); e != nil {
			return nil, e
		}
	default:
		return nil, fmt.Errorf("unsupported immutable minute delivery")
	}
	return p, nil
}

var errExistingBatch = errors.New("existing batch requires committed proof")

// A package-private fault boundary lets isolated tests stop an actual InnoDB
// transaction at the same points a process or connection may fail.
type minuteFaultKey struct{}

func minuteFault(ctx context.Context, stage string, tx *sql.Tx) error {
	if fn, ok := ctx.Value(minuteFaultKey{}).(func(string, *sql.Tx) error); ok {
		return fn(stage, tx)
	}
	return nil
}

// CommitPrepared is the sole shared-generation path. Mutable business writers
// still use transaction's exclusive fence and local write slot.
func (c *Client) CommitPrepared(ctx context.Context, p *PreparedDelivery) (err error) {
	ctx, cancel := context.WithTimeout(ctx, 40*time.Second)
	defer cancel()
	defer func() { err = transactionError(ctx, err) }()
	err = c.commitPreparedAttempt(context.WithValue(ctx, minuteInsertKey{}, true), p)
	if !errors.Is(err, errMinuteInsertCollision) {
		return err
	}
	// The first attempt confirmed a complete rollback. A duplicate is never
	// a batch success: repeat the original full checks with unchanged identity,
	// plan, generation, payload and first-knowledge timestamps.
	if err = minuteFault(ctx, "after_fact_duplicate_rollback", nil); err != nil {
		return err
	}
	return c.commitPreparedAttempt(ctx, p)
}

func (c *Client) commitPreparedAttempt(ctx context.Context, p *PreparedDelivery) (err error) {
	if p == nil || c.readOnly {
		return fmt.Errorf("invalid prepared submission")
	}
	d := p.d
	if d.Database != c.database || d.Service != c.producer.Service || d.Generation != c.producer.Generation {
		return contracts.ErrFenced
	}
	if ok, e := c.Committed(ctx, d); e != nil || ok {
		return e
	}
	conn, e := c.db.Conn(ctx)
	if e != nil {
		return e
	}
	defer conn.Close()
	trace, _ := ctx.Value(commitTraceKey{}).(*CommitTrace)
	if trace != nil {
		trace.RollbackConfirmed = false
		trace.Committed = false
		if e = conn.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&trace.ConnectionID); e != nil {
			return e
		}
	}
	tx, e := conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if e != nil {
		return e
	}
	var sqlStarted time.Time
	sqlFinished := false
	defer func() {
		if trace != nil && !sqlFinished && !sqlStarted.IsZero() {
			trace.SQL += time.Since(sqlStarted)
		}
		rollback := tx.Rollback()
		if trace != nil && rollback == nil {
			trace.RollbackConfirmed = true
		}
		if errors.Is(err, errMinuteInsertCollision) && rollback != nil {
			// A failed rollback cannot authorize another attempt on this call.
			err = fmt.Errorf("minute rollback unconfirmed after fact collision: %w", rollback)
		}
	}()
	var gen uint64
	var holder []byte
	lockStarted := time.Now()
	e = tx.QueryRowContext(ctx, "SELECT generation,holder FROM cmi_storage.producer_generation WHERE service=? FOR SHARE", c.producer.Service).Scan(&gen, &holder)
	if trace != nil {
		trace.Lock += time.Since(lockStarted)
	}
	if e != nil {
		return e
	}
	if gen != c.producer.Generation || string(holder) != string(c.producer.Holder[:]) {
		return contracts.ErrFenced
	}
	sqlStarted = time.Now()
	// Only this INSERT's duplicate key is interpreted as a competing delivery.
	// Other fact-table duplicates remain errors and roll back the whole batch.
	if e = c.recordDelivery(ctx, tx, d); e != nil {
		var duplicate *driver.MySQLError
		if errors.As(e, &duplicate) && duplicate.Number == 1062 {
			e = errExistingBatch
		}
		if !errors.Is(e, errExistingBatch) {
			return e
		}
		if rollback := tx.Rollback(); rollback != nil && !errors.Is(rollback, sql.ErrTxDone) {
			return rollback
		}
		ok, e := c.Committed(ctx, d)
		if e != nil {
			return e
		}
		if !ok {
			return fmt.Errorf("duplicate batch has no durable proof")
		}
		return nil
	}
	if e = minuteFault(ctx, "claimed", tx); e != nil {
		return e
	}
	if p.cexSpill != nil {
		e = c.commitCEXPreparation(ctx, tx, p)
	} else if p.cex != nil {
		e = c.commitCEXRows(ctx, tx, d, *p.cex, p.cexHashes, p.cexPublications)
	} else if p.catalog != nil {
		// Shared generation protection excludes concurrent exclusive plan
		// publication. Check the effective identity and uniqueness in this tx.
		id, err := c.latestOptionsPlanID(ctx, tx, d.Minute, false)
		if err != nil {
			return err
		}
		if id != p.plan.ID {
			return contracts.ErrConflict
		}
		var count int
		if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+c.table("options_collection_plan")+" WHERE effective_minute=?", p.plan.EffectiveMinute).Scan(&count); err != nil {
			return err
		}
		if count != 1 {
			return contracts.ErrConflict
		}
		e = c.persistOptionsCatalogRows(ctx, tx, p.run, p.plan, *p.catalog)
	} else if p.live != nil {
		e = c.persistOptionsLive(ctx, tx, *p.live)
	} else {
		return fmt.Errorf("missing prepared minute")
	}
	if e != nil {
		return e
	}
	if e = minuteFault(ctx, "facts", tx); e != nil {
		return e
	}
	r, e := tx.ExecContext(ctx, "UPDATE cmi_storage.ingest_batch SET committed_at=UTC_TIMESTAMP(6) WHERE database_name=? AND kind=? AND batch_id=?", d.Database, d.Kind, d.ID)
	if e != nil {
		return e
	}
	if n, e := r.RowsAffected(); e != nil || n != 1 {
		if e != nil {
			return e
		}
		return fmt.Errorf("owned batch row disappeared")
	}
	if trace != nil {
		trace.SQL += time.Since(sqlStarted)
	}
	sqlFinished = true
	commitStarted := time.Now()
	if e = minuteFault(ctx, "before_commit", tx); e != nil {
		return e
	}
	e = tx.Commit()
	if trace != nil {
		trace.Commit = time.Since(commitStarted)
		trace.Committed = e == nil
	}
	if e == nil {
		e = minuteFault(ctx, "after_commit", tx)
	}
	return e
}
