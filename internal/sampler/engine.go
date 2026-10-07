package sampler

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
)

type BookSource interface {
	Snapshot(int) (model.BookSnapshot, bool)
}
type MinuteSink interface {
	WriteCompletedMinute(context.Context, model.CompletedMinute) error
}

type Source struct {
	InstrumentID uint32
	Book         BookSource
	StoredDepth  int // Zero preserves the legacy constructor's ten-level default.
}

type Engine struct {
	sources         []Source
	sink            MinuteSink
	logger          *slog.Logger
	queue           chan queuedMinute
	mu              sync.Mutex
	minute          time.Time
	buffers         map[uint32]*MinuteBuffer
	started         bool
	capacity        int
	pendingWeight   int
	peakWeight      int
	pending         map[time.Time]time.Time
	failure         error
	now             func() time.Time
	sampleDurations []time.Duration
	sampleCount     uint64
	writeDurations  []time.Duration
	writeCount      uint64
	sampleOverruns  uint64
	missedSamples   uint64
	lastOverrunAt   time.Time
	lastOverrunID   uint32
	pendingSources  []Source
	sourcesAt       time.Time
}

type queuedMinute struct {
	model.CompletedMinute
	enqueuedAt time.Time
	weight     int
}

const MaxMinuteBacklog = 45 * time.Second

type Health struct {
	SampleSources      int
	PendingBatches     int
	PeakPendingBatches int
	OldestPendingAt    time.Time
	SampleCount        uint64
	SampleDurationP99  time.Duration
	WriteCount         uint64
	WriteDurationP99   time.Duration
	SampleOverruns     uint64
	MissedSamples      uint64
	LastOverrunAt      time.Time
	LastOverrunID      uint32
}

func NewEngine(sources []Source, sink MinuteSink, queueCapacity int, logger *slog.Logger) (*Engine, error) {
	if sink == nil || len(sources) == 0 {
		return nil, fmt.Errorf("sampler requires sources and a minute sink")
	}
	if queueCapacity <= 0 {
		queueCapacity = max(512, len(sources)*2)
	}
	if queueCapacity < len(sources)*2 {
		return nil, fmt.Errorf("minute queue capacity %d cannot hold two complete minutes of %d sources", queueCapacity, len(sources))
	}
	if logger == nil {
		logger = slog.Default()
	}
	seen := make(map[uint32]struct{}, len(sources))
	sources = append([]Source(nil), sources...)
	for n := range sources {
		if sources[n].StoredDepth == 0 {
			sources[n].StoredDepth = model.BookDepth
		}
		if sources[n].StoredDepth != 5 && sources[n].StoredDepth != 10 {
			return nil, fmt.Errorf("unsupported sample depth")
		}
		source := sources[n]
		if source.InstrumentID == 0 || source.Book == nil {
			return nil, fmt.Errorf("sampler source is invalid")
		}
		if _, ok := seen[source.InstrumentID]; ok {
			return nil, fmt.Errorf("duplicate sampler instrument %d", source.InstrumentID)
		}
		seen[source.InstrumentID] = struct{}{}
	}
	sources = append([]Source(nil), sources...)
	sort.Slice(sources, func(i, j int) bool { return sources[i].InstrumentID < sources[j].InstrumentID })
	return &Engine{sources: sources, sink: sink, logger: logger, queue: make(chan queuedMinute, 2), capacity: queueCapacity, buffers: make(map[uint32]*MinuteBuffer), pending: make(map[time.Time]time.Time), now: time.Now}, nil
}

func (e *Engine) Run(ctx context.Context) (result error) {
	return e.run(ctx, time.Time{})
}

func (e *Engine) RunAt(ctx context.Context, at time.Time) error {
	if at.IsZero() || !at.Equal(at.UTC().Truncate(time.Minute)) {
		return fmt.Errorf("sampler start requires exact UTC minute")
	}
	return e.run(ctx, at.UTC())
}

func (e *Engine) run(ctx context.Context, first time.Time) (result error) {
	e.mu.Lock()
	if e.started {
		e.mu.Unlock()
		return fmt.Errorf("sampler already started")
	}
	e.started = true
	e.mu.Unlock()
	writerCtx, cancelWriter := context.WithCancel(context.Background())
	defer cancelWriter()
	writerDone := make(chan error, 1)
	go func() {
		writerDone <- e.writeCompleted(writerCtx)
	}()
	writerFinished := false
	defer func() {
		close(e.queue)
		if result != nil {
			cancelWriter()
		}
		if !writerFinished {
			if err := <-writerDone; result == nil {
				result = err
			}
		}
	}()
	if !first.IsZero() {
		timer := time.NewTimer(time.Until(first))
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
		if err := e.sampleBefore(first, first.Add(time.Second)); err != nil {
			return err
		}
	}
	for {
		now := time.Now().UTC()
		next := now.Truncate(time.Second).Add(time.Second)
		timer := time.NewTimer(time.Until(next))
		select {
		case err := <-writerDone:
			timer.Stop()
			writerFinished = true
			return err
		case <-ctx.Done():
			timer.Stop()
			return e.flushIfCompleted(time.Now().UTC())
		case tick := <-timer.C:
			at := tick.UTC().Truncate(time.Second)
			if err := e.sampleBefore(at, at.Add(time.Second)); err != nil {
				return err
			}
		}
	}
}

func (e *Engine) flushIfCompleted(now time.Time) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.minute.IsZero() && now.UTC().Truncate(time.Minute).After(e.minute) {
		if e.failure != nil {
			return e.failure
		}
		if err := e.enqueueCompletedLocked(); err != nil {
			return err
		}
		e.minute = time.Time{}
		e.buffers = make(map[uint32]*MinuteBuffer)
	}
	return nil
}

// SampleAt is public to permit deterministic clock-driven tests.
func (e *Engine) SampleAt(at time.Time) error {
	return e.sampleBefore(at, e.now().Add(time.Second))
}

func (e *Engine) sampleBefore(at, deadline time.Time) error {
	startedAt := e.now()
	at = at.UTC().Truncate(time.Second)
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.failure != nil {
		return e.failure
	}
	for minute, enqueuedAt := range e.pending {
		if e.now().Sub(enqueuedAt) >= MaxMinuteBacklog {
			return e.failLocked(fmt.Errorf("minute writer backlog reached %s for %s", MaxMinuteBacklog, minute))
		}
	}
	minute := at.Truncate(time.Minute)
	if !e.minute.IsZero() && minute.Before(e.minute) {
		return e.failLocked(fmt.Errorf("sample clock moved backwards"))
	}
	if e.minute.IsZero() || minute.After(e.minute) {
		if !e.minute.IsZero() {
			if err := e.enqueueCompletedLocked(); err != nil {
				return e.failLocked(err)
			}
		}
		if !e.sourcesAt.IsZero() && !minute.Before(e.sourcesAt) {
			e.sources = e.pendingSources
			e.pendingSources = nil
			e.sourcesAt = time.Time{}
		}
		e.minute = minute
		e.buffers = make(map[uint32]*MinuteBuffer, len(e.sources))
		for _, source := range e.sources {
			buffer, err := NewMinuteBufferWithDepth(source.InstrumentID, minute, source.StoredDepth)
			if err != nil {
				return err
			}
			e.buffers[source.InstrumentID] = buffer
		}
	}
	if !e.now().Before(deadline) {
		return e.markOverrunLocked(at, 0, startedAt)
	}
	for index, source := range e.sources {
		snapshot, valid := source.Book.Snapshot(source.StoredDepth)
		if !e.now().Before(deadline) {
			return e.markOverrunLocked(at, index, startedAt)
		}
		if err := e.buffers[source.InstrumentID].Sample(at, snapshot, valid); err != nil {
			return e.failLocked(fmt.Errorf("instrument %d: %w", source.InstrumentID, err))
		}
	}
	e.sampleDurations = observeDuration(e.sampleDurations, e.sampleCount, 3600, e.now().Sub(startedAt))
	e.sampleCount++
	return nil
}

// markOverrunLocked records the sources whose snapshots could not be captured
// before this second's deadline as explicitly invalid. A transient scheduler or
// GC pause must lose at most data for that second, not terminate every market
// connection and force the whole universe through cold start again.
func (e *Engine) markOverrunLocked(at time.Time, firstMissed int, startedAt time.Time) error {
	for _, source := range e.sources[firstMissed:] {
		if err := e.buffers[source.InstrumentID].Sample(at, model.BookSnapshot{}, false); err != nil {
			return e.failLocked(fmt.Errorf("mark missed instrument %d: %w", source.InstrumentID, err))
		}
	}
	e.sampleOverruns++
	e.missedSamples += uint64(len(e.sources) - firstMissed)
	e.lastOverrunAt = at
	e.lastOverrunID = 0
	if firstMissed < len(e.sources) {
		e.lastOverrunID = e.sources[firstMissed].InstrumentID
	}
	elapsed := e.now().Sub(startedAt)
	e.sampleDurations = observeDuration(e.sampleDurations, e.sampleCount, 3600, elapsed)
	e.sampleCount++
	e.logger.Warn("sampling_deadline_missed", "second", at, "first_missed_instrument", e.lastOverrunID,
		"missed_instruments", len(e.sources)-firstMissed, "elapsed", elapsed)
	return nil
}

func (e *Engine) enqueueCompletedLocked() error {
	completed := model.CompletedMinute{MinuteTime: e.minute, Batches: make([]model.MinuteBatch, 0, len(e.sources))}
	for _, source := range e.sources {
		batch, ok := e.buffers[source.InstrumentID].Batch()
		if !ok {
			continue
		}
		completed.Batches = append(completed.Batches, batch)
	}
	weight := max(1, len(completed.Batches))
	if e.pendingWeight+weight > e.capacity {
		return fmt.Errorf("minute writer weighted queue full: pending=%d incoming=%d capacity=%d", e.pendingWeight, weight, e.capacity)
	}
	queued := queuedMinute{CompletedMinute: completed, enqueuedAt: e.now(), weight: weight}
	select {
	case e.queue <- queued:
		e.pendingWeight += weight
		e.peakWeight = max(e.peakWeight, e.pendingWeight)
		e.pending[e.minute] = queued.enqueuedAt
		return nil
	default:
		return fmt.Errorf("minute writer queue full: two completed minutes already queued")
	}
}

func (e *Engine) failLocked(err error) error {
	e.failure = err
	e.buffers = nil // An unfinished minute must never be flushed after failure.
	return err
}

func (e *Engine) writeCompleted(ctx context.Context) error {
	for queued := range e.queue {
		deadline := queued.enqueuedAt.Add(MaxMinuteBacklog)
		if !e.now().Before(deadline) {
			return fmt.Errorf("minute writer backlog reached %s for %s", MaxMinuteBacklog, queued.MinuteTime)
		}
		writeCtx, cancel := context.WithDeadline(ctx, deadline)
		start := e.now()
		err := e.sink.WriteCompletedMinute(writeCtx, queued.CompletedMinute)
		cancel()
		if err != nil {
			return fmt.Errorf("write completed minute %s: %w", queued.MinuteTime, err)
		}
		if !e.now().Before(deadline) {
			return fmt.Errorf("minute writer exceeded %s deadline for %s", MaxMinuteBacklog, queued.MinuteTime)
		}
		e.mu.Lock()
		e.pendingWeight -= queued.weight
		delete(e.pending, queued.MinuteTime)
		elapsed := e.now().Sub(start)
		e.writeDurations = observeDuration(e.writeDurations, e.writeCount, 1440, elapsed)
		e.writeCount++
		e.mu.Unlock()
		e.logger.Info("completed_minute_written", "minute", queued.MinuteTime, "instruments", len(queued.Batches), "elapsed", elapsed)
	}
	return nil
}

// ScheduleSources changes ownership only after freezing the preceding minute.
// Each new book must be independently warmed by its adapter before scheduling.
func (e *Engine) ScheduleSources(sources []Source, at time.Time) error {
	if at.IsZero() || !at.Equal(at.UTC().Truncate(time.Minute)) {
		return fmt.Errorf("source switch requires exact UTC minute")
	}
	copySources := append([]Source(nil), sources...)
	seen := map[uint32]bool{}
	for n := range copySources {
		s := &copySources[n]
		if s.StoredDepth == 0 {
			s.StoredDepth = model.BookDepth
		}
		if s.InstrumentID == 0 || s.Book == nil || seen[s.InstrumentID] || (s.StoredDepth != 5 && s.StoredDepth != 10) {
			return fmt.Errorf("invalid scheduled source")
		}
		seen[s.InstrumentID] = true
	}
	sort.Slice(copySources, func(i, j int) bool { return copySources[i].InstrumentID < copySources[j].InstrumentID })
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.failure != nil {
		return e.failure
	}
	if at.Compare(e.minute) <= 0 || at.Compare(e.now().UTC().Truncate(time.Minute)) <= 0 || !e.sourcesAt.IsZero() || 2*len(copySources) > e.capacity {
		return fmt.Errorf("source switch violates time/pending/capacity boundary")
	}
	e.pendingSources, e.sourcesAt = copySources, at.UTC()
	return nil
}

func (e *Engine) HealthSnapshot() Health {
	e.mu.Lock()
	defer e.mu.Unlock()
	h := Health{SampleSources: len(e.sources), PendingBatches: e.pendingWeight, PeakPendingBatches: e.peakWeight,
		SampleCount: e.sampleCount, SampleDurationP99: durationP99(e.sampleDurations), WriteCount: e.writeCount, WriteDurationP99: durationP99(e.writeDurations),
		SampleOverruns: e.sampleOverruns, MissedSamples: e.missedSamples, LastOverrunAt: e.lastOverrunAt, LastOverrunID: e.lastOverrunID}
	for _, enqueuedAt := range e.pending {
		if h.OldestPendingAt.IsZero() || enqueuedAt.Before(h.OldestPendingAt) {
			h.OldestPendingAt = enqueuedAt
		}
	}
	return h
}

func observeDuration(values []time.Duration, count uint64, limit int, value time.Duration) []time.Duration {
	if len(values) < limit {
		return append(values, value)
	}
	values[count%uint64(limit)] = value
	return values
}

func durationP99(values []time.Duration) time.Duration {
	if len(values) == 0 {
		return 0
	}
	ordered := append([]time.Duration(nil), values...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	return ordered[(len(ordered)*99+99)/100-1]
}
