package optionslive

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/options"
)

// Finished typed minutes are held in memory only, encoded by background writers.
// A separate count/byte bound prevents DB backpressure from exhausting network
// ingress. A minute that cannot be queued has no commit, so readers see the gap.
const maxCatalogPendingBytes = 32 << 20

type catalogPendingMinute struct {
	raw    []byte
	value  *options.CatalogEnvelope
	cache  *catalogMinuteCache
	budget *exchange.BufferBudget
	cost   int64
	run    uuid.UUID
	minute time.Time
}

type catalogMinuteCache struct {
	budget   *exchange.BufferBudget
	encoders chan struct{}
}

func newCatalogMinuteCache(limit int64) *catalogMinuteCache {
	return &catalogMinuteCache{budget: &exchange.BufferBudget{Limit: limit}, encoders: make(chan struct{}, 4)}
}

// Complete() and catalog proof rotation transfer an immutable finished minute.
// Enqueue that ownership without encoding on the sampling goroutine. The
// conservative reservation includes quality/evidence and variable book arrays.
func captureCatalogMinute(v options.CatalogEnvelope, cache *catalogMinuteCache) (*catalogPendingMinute, error) {
	cost := int64(1024 + len(v.Live.Books)*60*512 + len(v.Live.Indexes)*60*128)
	for _, b := range v.Live.Books {
		if b.Minute != nil {
			cost += int64(128 + 16*(len(b.Minute.Bids)+len(b.Minute.Asks)))
		}
		for _, d := range b.Deltas {
			cost += int64(128 + 8*(len(d.BidChangePrice)+len(d.BidChangeQty)+len(d.AskChangePrice)+len(d.AskChangeQty)))
		}
	}
	if cost > maxCatalogPendingBytes || !cache.budget.Reserve(cost) {
		return nil, fmt.Errorf("options pending minute memory budget exceeded")
	}
	return &catalogPendingMinute{value: &v, cache: cache, budget: cache.budget, cost: cost, run: v.Live.RunID, minute: v.Live.MinuteTime}, nil
}

func (p *catalogPendingMinute) compress(ctx context.Context) error {
	if p.value == nil {
		return nil
	}
	select {
	case p.cache.encoders <- struct{}{}:
		defer func() { <-p.cache.encoders }()
	case <-ctx.Done():
		return ctx.Err()
	}
	raw, err := encodeCatalogMinute(*p.value)
	if err != nil {
		return err
	}
	cost := int64(len(raw) + 256)
	if cost > p.cost {
		if !p.budget.Reserve(cost - p.cost) {
			return fmt.Errorf("options compressed minute memory budget exceeded")
		}
	} else {
		p.budget.Release(p.cost - cost)
	}
	p.raw, p.value, p.cost = raw, nil, cost
	return nil
}

type lazyCatalogSink interface {
	WriteOptionsCatalogMinuteLazy(context.Context, uuid.UUID, time.Time, func() (options.CatalogEnvelope, error)) error
}

// gob cannot encode nil BinaryMarshaler pointers inside slices. Preserve SQL
// NULL evidence explicitly rather than replacing it with a zero UUID/time.
type pendingUUID struct {
	Present bool
	Value   uuid.UUID
}
type pendingTime struct {
	Present bool
	Value   time.Time
}
type pendingEvidence struct {
	Value     options.QualityEvidence
	IDs       [5][]pendingUUID
	Confirmed []pendingTime
}
type pendingEnvelope struct {
	Value    options.CatalogEnvelope
	Evidence []pendingEvidence
}

func packPending(v options.CatalogEnvelope) pendingEnvelope {
	w := pendingEnvelope{Value: v, Evidence: make([]pendingEvidence, len(v.Evidence))}
	w.Value.Evidence = nil
	for i, e := range v.Evidence {
		p := &w.Evidence[i]
		p.Value = e
		for n, ids := range [][]*uuid.UUID{e.StateIDs, e.RuleObservationIDs, e.PlatformIDs, e.MaintenanceIDs, e.LockIDs} {
			p.IDs[n] = make([]pendingUUID, len(ids))
			for s, id := range ids {
				if id != nil {
					p.IDs[n][s] = pendingUUID{Present: true, Value: *id}
				}
			}
		}
		p.Confirmed = make([]pendingTime, len(e.LifecycleConfirmed))
		for s, at := range e.LifecycleConfirmed {
			if at != nil {
				p.Confirmed[s] = pendingTime{Present: true, Value: *at}
			}
		}
		p.Value.StateIDs, p.Value.RuleObservationIDs, p.Value.PlatformIDs = nil, nil, nil
		p.Value.MaintenanceIDs, p.Value.LockIDs, p.Value.LifecycleConfirmed = nil, nil, nil
	}
	return w
}

func (w pendingEnvelope) unpack() options.CatalogEnvelope {
	v := w.Value
	v.Evidence = make([]options.QualityEvidence, len(w.Evidence))
	for i, p := range w.Evidence {
		e := p.Value
		for n, dest := range []*[]*uuid.UUID{&e.StateIDs, &e.RuleObservationIDs, &e.PlatformIDs, &e.MaintenanceIDs, &e.LockIDs} {
			*dest = make([]*uuid.UUID, len(p.IDs[n]))
			for s, id := range p.IDs[n] {
				if id.Present {
					x := id.Value
					(*dest)[s] = &x
				}
			}
		}
		e.LifecycleConfirmed = make([]*time.Time, len(p.Confirmed))
		for s, at := range p.Confirmed {
			if at.Present {
				x := at.Value
				e.LifecycleConfirmed[s] = &x
			}
		}
		v.Evidence[i] = e
	}
	return v
}

type limitedCatalogWriter struct {
	io.Writer
	n int
}

func (w *limitedCatalogWriter) Write(b []byte) (int, error) {
	if len(b) > maxCatalogPendingBytes-w.n {
		return 0, fmt.Errorf("catalog pending minute size limit")
	}
	n, err := w.Writer.Write(b)
	w.n += n
	return n, err
}

func encodeCatalogMinute(v options.CatalogEnvelope) ([]byte, error) {
	var b bytes.Buffer
	z, err := gzip.NewWriterLevel(&b, gzip.BestSpeed)
	if err != nil {
		return nil, err
	}
	w := &limitedCatalogWriter{Writer: z}
	if err = gob.NewEncoder(w).Encode(packPending(v)); err != nil {
		_ = z.Close()
		return nil, err
	}
	if err = z.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func freezeCatalogMinute(v options.CatalogEnvelope, budget *exchange.BufferBudget) (*catalogPendingMinute, error) {
	raw, err := encodeCatalogMinute(v)
	if err != nil {
		return nil, err
	}
	cost := int64(len(raw) + 256)
	if !budget.Reserve(cost) {
		return nil, fmt.Errorf("global options pending minute budget exceeded")
	}
	return &catalogPendingMinute{raw: raw, budget: budget, cost: cost, run: v.Live.RunID, minute: v.Live.MinuteTime}, nil
}

func (p *catalogPendingMinute) thaw() (options.CatalogEnvelope, error) {
	var v options.CatalogEnvelope
	z, err := gzip.NewReader(bytes.NewReader(p.raw))
	if err != nil {
		return v, err
	}
	defer z.Close()
	raw, err := io.ReadAll(io.LimitReader(z, maxCatalogPendingBytes+1))
	if err != nil {
		return v, err
	}
	if len(raw) > maxCatalogPendingBytes {
		return v, fmt.Errorf("catalog pending minute size limit")
	}
	var w pendingEnvelope
	err = gob.NewDecoder(bytes.NewReader(raw)).Decode(&w)
	if err != nil {
		return v, err
	}
	return w.unpack(), nil
}

func (p *catalogPendingMinute) release() { p.budget.Release(p.cost); p.raw, p.value = nil, nil }

func writeCatalogMinutes(ctx context.Context, batches <-chan *catalogPendingMinute, sink CatalogSink, logger *slog.Logger) error {
	// A worker closes its queue before canceling a draining writer.
	defer func() {
		for {
			select {
			case p, ok := <-batches:
				if !ok {
					return
				}
				p.release()
			default:
				return
			}
		}
	}()
	for p := range batches {
		if err := p.compress(ctx); err != nil {
			p.release()
			return err
		}
		var v *options.CatalogEnvelope
		for attempt := 0; ; attempt++ {
			if ctx.Err() != nil {
				p.release()
				return ctx.Err()
			}
			var loadErr error
			load := func() (options.CatalogEnvelope, error) {
				x, err := p.thaw()
				loadErr = err
				return x, err
			}
			var err error
			if lazy, ok := sink.(lazyCatalogSink); ok {
				// Production expands at most three batches, after DB admission.
				err = lazy.WriteOptionsCatalogMinuteLazy(ctx, p.run, p.minute, load)
			} else {
				if v == nil {
					x, e := load()
					if e == nil {
						v = &x
					}
				}
				if loadErr == nil {
					err = sink.WriteOptionsCatalogMinute(ctx, *v)
				}
			}
			if loadErr != nil {
				p.release()
				return loadErr
			}
			if err == nil {
				break
			}
			if ctx.Err() != nil {
				p.release()
				return ctx.Err()
			}
			logger.Warn("options minute write retrying", "run", p.run, "minute", p.minute, "attempt", attempt+1, "error", err)
			delay := min(time.Second<<min(attempt, 3), 8*time.Second)
			if !exchange.Wait(ctx, delay) {
				p.release()
				return ctx.Err()
			}
		}
		p.release()
	}
	return nil
}
